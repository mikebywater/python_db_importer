#!/usr/local/bin/python3

__author__ = 'mike'

import pymysql
import os
import csv


class ImportMySQL:



    def __init__(self, host="localhost", user="user", passwd="password", dbname="data_import", path="source_files"):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.dbname = dbname
        self.db = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.dbname)
        self.db.set_charset('utf8')
        self.cur = self.db.cursor()
        self.cur.execute('SET NAMES utf8;')
        self.cur.execute('SET CHARACTER SET utf8;')
        self.cur.execute('SET character_set_connection=utf8;')
        self.path = path

    def drop_table(self, table):
        # Drop the table if it exists
        query = "DROP TABLE IF EXISTS " + table
        self.cur.execute(query)
        self.db.commit()
        return "success"

    def import_data(self, file):
        table = file.replace('.csv','')
        ImportMySQL.drop_table(self, table)
        # Open file
        n = 0
        with open(self.path + file) as csvfile:
            fp = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in fp:

                n += 1
                if n == 1:
                    cols = row
                    # Recreate the table from the first line of the text file (create an id column too)
                    query = "CREATE TABLE " + table + " (id INT NOT NULL AUTO_INCREMENT"
                    for col in cols:
                        query += ", `" + col + "` VARCHAR(30)"
                    query += " , PRIMARY KEY(id))"
                    self.cur.execute(query)
                    self.db.commit()
                else:
                    # Fill table with data
                    query = "INSERT INTO " + table + " VALUES(NULL"
                    for rec in row:
                        rec = pymysql.escape_string(rec)
                        query += ", '" + rec + "'"
                    query += ")"
                    self.cur.execute(query)
                    self.db.commit()

    def import_dir(self):
        for file in os.listdir(self.path):
            if file.endswith(".csv"):
                ImportMySQL.import_data(self, file)