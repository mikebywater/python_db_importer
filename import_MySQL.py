#!/usr/bin/python3

__author__ = 'mike'

import pymysql
import os


# Connect to db

db = pymysql.connect(host="localhost", user="user", passwd="password", db="data_import", )

# create a Cursor object. It will let you execute all the queries you need

cur = db.cursor()

#cur.execute("INSERT INTO users (firstname,lastname,email) VALUES ('Zoe','Smout', 'xxzoesxx@hotmail.com')")
path = 'source_files/'
#file = 'bank_details.csv'


def drop_table(table):
    # Drop the table if it exists
    query = "DROP TABLE IF EXISTS " + table
    cur.execute(query)
    db.commit()


def import_data(file):
    table = file.strip('csv')
    table = table.strip('.')
    drop_table(table)
    # Open file
    n = 0
    with open(path + file) as fp:
        for line in fp:
            line = line.rstrip()
            n += 1
            if n == 1:
                cols = line.split(',')
                # Recreate the table from the first line of the text file (create an id column too)
                query = "CREATE TABLE " + table + " (id INT NOT NULL AUTO_INCREMENT"
                for col in cols:
                    query += ", " + col + " VARCHAR(30)"
                query += " , PRIMARY KEY(id))"
                cur.execute(query)
                db.commit()
            else:
                row = line.split(',')
                # Fill table with data
                query = "INSERT INTO " + table + " VALUES(NULL"
                for rec in row:
                    query += ", '" + rec + "'"
                query += ")"
                cur.execute(query)
                db.commit()

def import_dir(path):
    for file in os.listdir(path):
        if file.endswith(".csv"):
            import_data(file)

import_dir(path)