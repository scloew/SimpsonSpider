# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from random import randint
import pyodbc


class SimpsonCharactersPipeline(object):
    """Insert scraped data into database"""

    statuses = {0 : 'basic', 1 : 'intermediate', 2 : 'advance'}
    conn = pyodbc.connect(
        "Driver={SQL Server};"
        "Server=LAPTOP-B3HSU0AK\LOEWSQL;"
        "Database=BlockbusterDB;"
        "Trusted_Connection=yes"
    )

    def _item_exists(self, item):
        """avoid insert of duplicates from rerunning spider;
           handled here and not in sql because want capacity
           for duplicated name since primary key is indexing column"""
        cursor = self.conn.cursor()
        cursor.execute(
            'SELECT * FROM Members where first_name = ?;',
            (item['first_name'])
        )
        return True if len(cursor.fetchall()) else False

    def process_item(self, item):
        if not self._item_exists(item):
            status = self.statuses[randint(0, 2)]
            cursor = self.conn.cursor()
            cursor.execute(f"AddNewMember '{item['first_name']}', '{item['last_name']}', '{status}'")
            cursor.commit()
        return item
