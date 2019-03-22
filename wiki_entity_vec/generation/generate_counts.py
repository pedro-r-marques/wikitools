# -*- coding: utf-8 -*-
# 
# outlinks: select count(*) from pagelinks where pl_from = 534366;
# inlinks: select count(*) from pagelinks where pl_from_namespace = 0 and pl_namespace = 0 and pl_title = 'Barack_Obama';
from __future__ import generators, print_function

import argparse
import mysql.connector
import os

def ResultIter(cursor):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(size=1000)
        if not results:
            break
        for result in results:
            yield result

def process_page(cnx, result, min_links, page_out, category_set):
    is_redirect = result[4]
    if is_redirect:
        return
 
    page_id = result[0]
    page_title = result[2]
 
    outlinks = cnx.cursor()
    outlinks.execute("select count(*) from pagelinks where pl_from = {0}".format(page_id))
    outcount = outlinks.fetchall()[0][0]
    inlinks = cnx.cursor()
    inlinks.execute(u"select count(*) from pagelinks where pl_from_namespace = 0 and pl_namespace = 0 and pl_title = '{0}'".format(page_title))
    incount = inlinks.fetchall()[0][0]
    if incount + outcount < min_links:
        return
    page_out.write(page_title.encode('utf-8') + '\n')
    categories = cnx.cursor()
    categories.execute("select cl_to from categorylinks where cl_from = {0}".format(page_id))
    for category_info in categories.fetchall():
        category_set.add(category_info[0])

def main():
    parser = argparse.ArgumentParser(description='wikipedia page link counts')
    parser.add_argument('--password')
    parser.add_argument('--min_links', default=50)
    parser.add_argument('--page_dictionary', required=True)
    parser.add_argument('--category_file', required=True)
    args = parser.parse_args()

    cnx = mysql.connector.connect(user=os.environ['USER'], passwd=args.password, database='enwiki')
    pages = cnx.cursor(buffered=True)
    pages.execute("select * from page")
    category_set = set()

    with open(args.page_dictionary, 'w') as page_out:
        for result in ResultIter(pages):
            process_page(cnx, result, args.min_links, page_out, category_set)

    with open(args.category_file, 'w') as category_file:
        for cat in category_set:
            category_file.write(cat.encode('utf-8') + "\n")

if __name__ == '__main__':
    main()