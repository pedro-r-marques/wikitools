from __future__ import print_function

import subprocess
import tempfile
import os

DATADIR = '../../../data/pageviews'

class Runner(object):
    def __init__(self, datadir):
        self._tmpdir = tempfile.mkdtemp()
        curdir = os.path.dirname(os.path.abspath(__file__))
        self._basedir = os.path.dirname(curdir)
        self._datadir = datadir
    
    def build(self):
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
       
    def map(self, num_shards):
        env = {
            'PATH': self._tmpdir
        }
        args = ['mapper', '--input-path={0}'.format(self._datadir),
                '--output-path={0}'.format(self._datadir), '--num-shards={0}'.format(num_shards)]
        for i in xrange(num_shards):
            cmd = list(args)
            cmd.append('--id={0}'.format(i))
            print('map shard {0}'.format(i))
            p = subprocess.Popen(cmd, env=env)
            p.wait()

    def reduce(self, num_shards):
        env = {
            'PATH': self._tmpdir
        }
        args = ['reducer', '--input-path={0}'.format(self._datadir), '--output-path={0}'.format(
            self._datadir), '--num-shards={0}'.format(num_shards)]
        p = subprocess.Popen(args, env=env)
        p.wait()

def main():
    run = Runner(DATADIR)
    run.build()
    run.map(8)
    run.reduce(8)

if __name__ == '__main__':
    main()