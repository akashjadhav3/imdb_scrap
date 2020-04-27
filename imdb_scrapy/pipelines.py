# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import pymongo
import sqlite3

# class MongodbPipeline(object):
#     collection_name = "best_movies"

#     def open_spider(self, spider):
#         self.client = pymongo.MongoClient("mongodb+srv://akash:asme@cluster0-aoimn.mongodb.net/test?retryWrites=true&w=majority")
#         self.db = self.client["IMDB"]

#     def close_spider(self, spider):
#         self.client.close()

#     def process_item(self, item, spider):
#         self.db[self.collection_name].insert(item)
#         return item


class SqLitePipeline(object):

    def open_spider(self, spider):
        self.connnection = sqlite3.connect("imdb.db")
        self.c = self.connnection.cursor()
        try:
            self.c.execute('''
                CREATE TABLE best_movies(
                    title TEXT,
                    year TEXT,
                    duration TEXT,
                    genere TEXT,
                    rating TEXT,
                    movie_url TEXT
                )
            
            ''')
            self.connnection.commit()
        except sqlite3.OperationalError:
            pass

    def close_spider(self, spider):
        self.connnection.close()

    def process_item(self, item, spider):
        self.c.execute('''
            INSERT INTO best_movies (title,year,duration,genere,rating,movie_url) VALUES (?,?,?,?,?,?)
        ''', (
            item.get('title'),
            item.get('year'),
            item.get('duration'),
            item.get('genere'),
            item.get('rating'),
            item.get('movie_url')
        ))
        self.connnection.commit()
        return item
