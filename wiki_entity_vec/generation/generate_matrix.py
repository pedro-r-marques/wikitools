# -*- coding: utf-8 -*-

"""
"""

from __future__ import generators, print_function

import argparse
import mysql.connector
import os
import struct

from dictionary import Dictionary

FORMAT = 'IIf'

class PageKeyMapper(object):
    """ Map sql table keys (page_id) to page_title.
    """
    def __init__(self, connection):
        self._connection = connection
        self._keys = {}

    def lookup(self, key):
        """ Returns the page_title given a page_id.
        Caches the result.
        """
        if key in self._keys:
            return self._keys[key]
        page = self._connection.cursor()
        page.execute(
            "select page_title from page where page_id = {0}".format(key))
        name = page.fetchall()[0][0]
        self._keys[key] = name
        return name


def ResultIter(cursor):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(size=1000)
        if not results:
            break
        for result in results:
            yield result


def generate_matrix(cnx, pages, page_dict, category_dict, file):
    mapper = PageKeyMapper(cnx)

    for result in ResultIter(pages):
        page_id = result[0]
        page_title = result[1]
        if not page_dict.contains(page_title):
            continue

        outlinks_offset = page_dict.size()
        categorylinks_offset = 2 * page_dict.size()

        inlinks = cnx.cursor()
        inlinks.execute(
            u"select pl_from from pagelinks where pl_from_namespace = 0 and pl_namespace = 0 and pl_title = '{0}'".format(page_title))
        inlink_keys = []
        for link in inlinks.fetchall():
            from_id = int(link[0])
            inlink_keys.append(from_id)

        matrix_row = page_dict.id(page_title)
        for key in inlink_keys:
            from_title = mapper.lookup(key)
            col = page_dict.id(from_title)
            if col == -1:
                continue
            file.write(struct.pack(FORMAT, matrix_row, col, 1.0))

        outlinks = cnx.cursor()
        outlinks.execute(
            "select pl_title from pagelinks where pl_from = {0}".format(page_id))
        for link in outlinks.fetchall():
            col = page_dict.id(link[0])
            if col == -1:
                continue
            col += outlinks_offset
            file.write(struct.pack(FORMAT, matrix_row, col, 1.0))

        categories = cnx.cursor()
        categories.execute(
            "select cl_to from categorylinks where cl_from = {0}".format(page_id))
        for link in categories.fetchall():
            col = category_dict.id(link[0])
            if col == -1:
                continue
            col += categorylinks_offset
            file.write(struct.pack(FORMAT, matrix_row, col, 1.0))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--password')
    parser.add_argument('--output_dir', required=True)
    parser.add_argument('--page_dictionary', required=True)
    parser.add_argument('--category_file', required=True)
    parser.add_argument('--num_shards', type=int, default=32)
    parser.add_argument('--shard_id', type=int, required=True)
    args = parser.parse_args()

    page_dict = Dictionary()
    page_dict.load(args.page_dictionary)
    category_dict = Dictionary()
    category_dict.load(args.category_file)

    cnx = mysql.connector.connect(
        user=os.environ['USER'], passwd=args.password, database='enwiki')
    pages = cnx.cursor(buffered=True)
    pages.execute("select page_id, page_title from page where MOD(CRC32(page_id), {1}) = {0}".format(
        args.shard_id, args.num_shards))

    output_file = os.path.join(
        args.output_dir, 'data.bin.{0:05d}-of-{1:05d}'.format(args.shard_id, args.num_shards))
    with open(output_file, 'w') as file:
        generate_matrix(cnx, pages, page_dict, category_dict, file)


if __name__ == '__main__':
    main()
