import unittest
import tempfile
import random
import struct
import os

from typing import Tuple

import subprocess

from scipy.sparse import dok_matrix


class TrainIntegrationTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        print('TMPDIR={0}'.format(self._tmpdir))
        random.seed(20190318)

    @staticmethod
    def _generate_matrix(shape: Tuple[int, int], datapoints: int) -> dok_matrix:
        matrix = dok_matrix(shape)
        for _ in range(datapoints):
            while True:
                row = random.randint(0, shape[0] - 1)
                col = random.randint(0, shape[1] - 1)
                if matrix.get((row, col)):
                    continue
                matrix[row, col] = 1.0
                break
        return matrix

    def _generate_testdata(self, matrix: dok_matrix, n_shards: int = 4):
        matrix = matrix.tocoo()
        block_size = int((matrix.shape[0] - 1) / n_shards) + 1
        datadir = os.path.join(self._tmpdir, 'matrix')
        os.makedirs(datadir, exist_ok=True)
        for shard_id in range(n_shards):
            filename = os.path.join(
                datadir, 'data.bin.{0:05d}-of-{1:05d}'.format(shard_id, n_shards))
            with open(filename, 'wb') as file:
                for i in range(matrix.size):
                    row = matrix.row[i]
                    if row < shard_id * block_size or row >= (shard_id+1) * block_size:
                        continue
                    file.write(struct.pack(
                        'IIf', row, matrix.col[i], matrix.data[i]))

    def _generate_dataset(self, n_pages: int, n_categories: int):
        with open(os.path.join(self._tmpdir, 'page-dictionary.txt'), 'w') as file:
            for i in range(n_pages):
                file.write('{0}\n'.format(i))
        with open(os.path.join(self._tmpdir, 'categories.txt'), 'w') as file:
            for i in range(n_categories):
                file.write('{0}\n'.format(i))
        shape = (n_pages, 2*n_categories + n_categories)
        matrix = TrainIntegrationTest._generate_matrix(
            shape, int(shape[0] * shape[1] * 0.1))
        self._generate_testdata(matrix)

    def test_train(self):
        self._generate_dataset(100, 20)
        cmd = ['python', 'train.py', '--data_shards=4', '--embedding_size=4', '--batch_size=16', '--epochs=10',
               '--page_dictionary={0}'.format(os.path.join(
                   self._tmpdir, 'page-dictionary.txt')),
               '--category_file={0}'.format(
                   os.path.join(self._tmpdir, 'categories.txt')),
               '--output_dir={0}'.format(os.path.join(self._tmpdir, 'output')),
               '--logs_dir={0}'.format(os.path.join(self._tmpdir, 'board')),
               os.path.join(self._tmpdir, 'matrix')
               ]
        p = subprocess.Popen(cmd)
        self.assertEqual(p.wait(), 0)

        # load weights and continue
        p = subprocess.Popen(cmd)
        self.assertEqual(p.wait(), 0)


if __name__ == '__main__':
    unittest.main()
