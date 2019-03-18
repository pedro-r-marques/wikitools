from __future__ import print_function
import argparse
import os
import subprocess
import sys
import re


def build(basedir):
    env = {
        'PATH': os.environ['PATH'],
        'HOME': os.environ['HOME'],
    }
    args = [
        'go', 'build', './cmd/...'
    ]
    p = subprocess.Popen(args, cwd=basedir, env=env)
    p.wait()

def main():
    parser = argparse.ArgumentParser(
        description='Wikipedia sqldump data filter.')
    parser.add_argument('--page-filter')
    parser.add_argument('input_dir')
    parser.add_argument('output_dir')
    args = parser.parse_args()

    # look for <basename>-page.sql and <basename>-pagelinks.sql
    pattern = re.compile(r'(.*?)-page(?:links)?.sql$')
    basenames = set()
    files = []
    for file in os.listdir(args.input_dir):
        m = pattern.match(file)
        if m:
            print(file)
            files.append(m.group(0))
            basenames.add(m.group(1))

    if len(basenames) != 1 or len(files) != 2:
        print('files: ', files)
        sys.exit(1)

    curdir = os.path.dirname(os.path.abspath(__file__))
    rootdir = os.path.dirname(curdir)
    build(rootdir)
    env = {
        'PATH': rootdir
    }

    basename = list(basenames)[0]
    cmd = ['sqlfilter',
           '--input={0}/{1}-page.sql'.format(args.input_dir, basename),
           '--page-filter={0}'.format(args.page_filter),
           '--output={0}/{1}-filtered-page.sql'.format(
               args.output_dir, basename),
           '--index-output={0}/page-indices.txt'.format(args.output_dir)]
    p = subprocess.Popen(cmd, env=env)
    retcode = p.wait()
    if retcode:
        sys.exit(retcode)

    cmd = ['sqlfilter',
           '--input={0}/{1}-pagelinks.sql'.format(args.input_dir, basename),
           '--page-filter={0}'.format(args.page_filter),
           '--index-filter={0}/page-indices.txt'.format(args.output_dir),
           '--output={0}/{1}-filtered-pagelinks.sql'.format(args.output_dir, basename),
           '--chunk-size=1000']
    p = subprocess.Popen(cmd, env=env)
    p.wait()

    cmd = ['sqlfilter',
           '--input={0}/{1}-categorylinks.sql'.format(args.input_dir, basename),
           '--index-filter={0}/page-indices.txt'.format(args.output_dir),
           '--output={0}/{1}-filtered-categorylinks.sql'.format(args.output_dir, basename)]
    p = subprocess.Popen(cmd, env=env)
    p.wait()


if __name__ == '__main__':
    main()
