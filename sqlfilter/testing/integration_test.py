from __future__ import print_function
import unittest
import subprocess
import tempfile
import os


class IntegrationTest(unittest.TestCase):
    """Test the filtering of pages and pagelinks.
    """

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        print('TMPDIR={0}'.format(self._tmpdir))
        curdir = os.path.dirname(os.path.abspath(__file__))
        self._basedir = os.path.dirname(curdir)
        env = {
            'PATH': os.environ['PATH'],
            'HOME': os.environ['HOME'],
            'GOBIN': self._tmpdir,
        }
        args = [
            'go', 'install', '{0}/cmd/...'.format(self._basedir)
        ]
        p = subprocess.Popen(args, env=env)
        p.wait()

    def test_page_filter(self):
        env = {
            'PATH': self._tmpdir
        }

        args = ['sqlfilter', '--input={0}/testing/testdata/test-page.sql'.format(self._basedir),
                '--page-filter={0}/testing/testdata/page-filter.tsv'.format(self._basedir),
                '--output={0}/page-output.sql'.format(self._tmpdir), '--index-output={0}/indices.txt'.format(self._tmpdir)]
        p = subprocess.Popen(args, env=env)
        p.wait()

        expected = [
            "/* Header */",
            "INSERT INTO `page` VALUES (12,0,'Anarchism','',0,0,0.786172332974311,'20190120003623','20190120011642',878871297,198626,'wikitext',NULL);",
            "/* Footer */"
        ]
        with open(os.path.join(self._tmpdir, 'page-output.sql'), 'r') as fp:
            lines = [l.strip() for l in fp.readlines()]
            self.assertListEqual(lines, expected)

    def test_pagelinks_filter(self):
        env = {
            'PATH': self._tmpdir
        }

        args = ['sqlfilter', '--input={0}/testing/testdata/test-pagelinks.sql'.format(self._basedir),
                '--page-filter={0}/testing/testdata/page-filter.tsv'.format(self._basedir),
                '--index-filter={0}/testing/testdata/test-indices.txt'.format(self._basedir),
                '--output={0}/pagelinks-output.sql'.format(self._tmpdir)]
        p = subprocess.Popen(args, env=env)
        p.wait()

        expected = [
            "/* Header */",
            "INSERT INTO `pagelinks` VALUES (12,0,'foo',0);",
            "/* Footer */"
        ]
        with open(os.path.join(self._tmpdir, 'pagelinks-output.sql'), 'r') as fp:
            lines = [l.strip() for l in fp.readlines()]
            self.assertListEqual(lines, expected)
    
       
if __name__ == '__main__':
    unittest.main()
