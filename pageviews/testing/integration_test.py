from __future__ import print_function
import unittest
import subprocess
import tempfile
import os


class IntegrationTest(unittest.TestCase):
    """Test the interface between mapper and reducer and ensure that generated file has the expected order.
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

    def test_map_reduce(self):
        env = {
            'PATH': self._tmpdir
        }
        num_shards = 4
        args = ['mapper', '--input-path={0}/testing/testdata'.format(self._basedir),
                '--output-path={0}'.format(self._tmpdir), '--num-shards={0}'.format(num_shards)]
        for i in xrange(num_shards):
            cmd = list(args)
            cmd.append('--id={0}'.format(i))
            p = subprocess.Popen(cmd, env=env)
            p.wait()

        args = ['reducer', '--input-path={0}'.format(self._tmpdir), '--output-path={0}'.format(
            self._tmpdir), '--num-shards={0}'.format(num_shards)]
        p = subprocess.Popen(args, env=env)
        p.wait()

        expected = [
            'Albert_Einstein\t13',
            'Jennifer_Aniston\t12',
            'Barack_Obama\t10',
            'Frank_Sinatra\t2'
        ]
        with open(os.path.join(self._tmpdir, 'pagecounts-summary.tsv'), 'r') as fp:
            data = [l.strip() for l in fp.readlines()]
            self.assertListEqual(data, expected)


if __name__ == '__main__':
    unittest.main()
