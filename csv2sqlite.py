#!/usr/local/bin/python3

import csv
import os
import logging
import sqlite3
from sys import argv

# TODO: Fixup
# TODO: Add options and jazz.
# TODO: Basic Test code

class Csv2Sqlite:
    """The summary line for a class docstring should fit on one line.

    If the class has public attributes, they may be documented here
    in an ``Attributes`` section and follow the same formatting as a
    function's ``Args`` section. Alternatively, attributes may be documented
    inline with the attribute's declaration (see __init__ method below).

    Properties created with the ``@property`` decorator should be documented
    in the property's getter method.

    Attributes:
        attr1 (str): Description of `attr1`.
        attr2 (:obj:`int`, optional): Description of `attr2`.
    """

    def __init__(self, file_path, db="example.db", table_format=None):
        self.file_path = file_path
        self.table_format = table_format

        file_name = os.path.basename(self.file_path)
        self.table_name = file_name.split(".")[0]
        logging.info("Table name will be '%s'", self.table_name)

        self.connection = sqlite3.connect(db)
        self.cursor = self.connection.cursor()

        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (self.table_name,))

        if len(self.cursor.fetchall()):
            logging.info("Table '%s' already exists.", self.table_name)
            logging.info("Dropping table '%s' to prevent format errors.", self.table_name)

            # I know this is bad form but no option for this in sqlite lib
            self.cursor.execute("DROP TABLE %s;" % self.table_name)

        if not self.table_format:
            logging.debug("Format for table is not given, will be decided in runtime.")

    def create_table(self):
        # I know this is bad form but no option for this in sqlite lib
        query = 'CREATE TABLE %s (' % self.table_name
        query += ', '.join(['"%s"' % col for col in self.table_format])
        query += ");"

        logging.debug(query)
        self.cursor.execute(query)

    def get_insert_query(self):
        # I know this is bad form but no option for this in sqlite lib
        query = 'INSERT INTO %s VALUES (' % self.table_name
        query += ','.join(['?' for _ in self.table_format])
        query += ');'
        return query

    def run(self):
        with open(self.file_path, newline='') as csvfile:
            reader = csv.reader(csvfile, skipinitialspace=True, delimiter=',', quotechar='"')

            if not self.table_format:
                self.table_format = reader.__next__()

            self.create_table()

            self.cursor.executemany(self.get_insert_query(), reader)
            # I know this is bad form but no option for this in sqlite lib
            self.cursor.execute("SELECT Count(*) FROM %s" % self.table_name)

            count = self.cursor.fetchone()[0]
            logging.info("%d records inserted", count)

            self.connection.commit()
            self.connection.close()


if __name__ == "__main__":
    logging.basicConfig(format='[%(levelname)s] [%(filename)s:%(lineno)d] [%(asctime)s] - %(message)s',
                        level=logging.INFO)
    if len(argv) != 2:
        logging.error("Please follow the format of ./csv2sqlite.py <csv_file> OR python3 ./csv2sqlite.py <csv_file> (if shebang fails)")

    Csv2Sqlite(argv[1]).run()
