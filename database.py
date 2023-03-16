#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import sqlite3
import os

working_directory = os.path.dirname(os.path.realpath(__file__))
database_path = os.path.join(working_directory, "domains.sqlite")


def chunks(data, rows=10000):
    for i in range(0, len(data), rows):
        yield data[i:i+rows]


class Urls:
    """This is our database handler/class."""

    def __init__(self) -> None:
        """On creation of the class we want to open a connection to our sqlite3 database."""
        self.db = sqlite3.connect(database_path)
        self.cursor = self.db.cursor()

    def create_table(self) -> None:
        logging.info("creating new database table")
        self.db.execute("CREATE TABLE domains (url TEXT)")

    def mass_insert_urls(self, urls: list) -> None:
        """This function will insert all domains to the database."""

        chunk_data = chunks(urls)
        for chunk in chunk_data:
            self.cursor.execute('BEGIN TRANSACTION')
            for ID, Domain in chunk:
                self.cursor.execute('INSERT INTO domains (url) VALUES (?)', (Domain,))
            self.cursor.execute('COMMIT')

    def get_random_domains(self, count: int = 1) -> list[str]:
        logging.info(f"fetching {count} domains from db")
        # https://web.archive.org/web/20200628215538/http://www.bernzilla.com/2008/05/13/selecting-a-random-row-from-an-sqlite-table/
        self.cursor.execute("SELECT url FROM Domains ORDER BY RANDOM();")
        return_urls = []
        for url in self.cursor.fetchmany(count):
            return_urls.append(url[0])
        return return_urls

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.db.close()
