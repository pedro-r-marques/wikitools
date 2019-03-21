from __future__ import print_function
import unittest

import tempfile
import os
import random
import struct

from functools import partial

import numpy as np

from scipy.sparse import dok_matrix

from data_generator import DataGenerator


class DataGeneratorTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        print('TMPDIR={0}'.format(self._tmpdir))
        random.seed(20190318)

    @staticmethod
    def _generate_matrix(shape, datapoints):
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

    def _assert_matrix_equal(self, matrix, X):
        self.assertEqual(matrix.size, X.shape[0])
        for row, col in X:
            self.assertTrue(matrix.get((row, col)))

    def _generate_testdata(self, matrix, n_shards=16):
        matrix = matrix.tocoo()
        block_size = int((matrix.shape[0] - 1) / n_shards) + 1
        for shard_id in range(n_shards):
            filename = os.path.join(
                self._tmpdir, 'data.bin.{0:05d}-of-{1:05d}'.format(shard_id, n_shards))
            with open(filename, 'wb') as file:
                for i in range(matrix.size):
                    row = matrix.row[i]
                    if row < shard_id * block_size or row >= (shard_id+1) * block_size:
                        continue
                    file.write(struct.pack(
                        'IIf', row, matrix.col[i], matrix.data[i]))

    @staticmethod
    def _collect_data(gen, observer=None):
        X_data = []
        y_data = []
        for batch in range(len(gen)):
            X, y = gen[batch]
            if observer:
                observer(batch, X, y)
            X_data.append(X)
            y_data.append(y)
        X = np.vstack(X_data)
        y = np.hstack(y_data)
        return X, y

    def test_basic(self):
        matrix = DataGeneratorTest._generate_matrix((10, 10), 18)
        self.assertEqual(matrix.size, 18)
        self._generate_testdata(matrix, 2)
        gen = DataGenerator(self._tmpdir, 2, batch_size=4)
        self.assertEqual(len(gen), 5)
        X, y = DataGeneratorTest._collect_data(gen)
        self.assertEqual(X.shape, (18, 2))
        self.assertEqual(y.shape, (18, ))
        self._assert_matrix_equal(matrix, X)

    def test_multiple_chunks(self):
        matrix = DataGeneratorTest._generate_matrix((10, 10), 18)
        self._generate_testdata(matrix, 2)
        gen = DataGenerator(self._tmpdir, 2, batch_size=4, chunk_size=4)
        self.assertEqual(len(gen), 5)
        X, y = DataGeneratorTest._collect_data(gen)
        self._assert_matrix_equal(matrix, X)
        self.assertEqual(y.shape, (18, ))

    def _shard_count(self, matrix, n_shards, n, X, y):
        if n > 0:
            return
        block_size = int((matrix.shape[0] - 1) / n_shards) + 1
        shards = set()
        for v in X[:, 0]:
            shard_id = int(v / block_size)
            shards.add(shard_id)
        self._obs_shards = shards

    def test_merge(self):
        matrix = DataGeneratorTest._generate_matrix((100, 100), 2000)
        self._generate_testdata(matrix, 4)
        gen = DataGenerator(self._tmpdir, 4, batch_size=64,
                            shuffle=True, shard_merge=True, chunk_size=128)
        obs = partial(self._shard_count, matrix, 4)
        X, _ = DataGeneratorTest._collect_data(gen, obs)
        self._assert_matrix_equal(matrix, X)
        self.assertEqual(len(self._obs_shards), 4)

    def test_sequential(self):
        matrix = DataGeneratorTest._generate_matrix((100, 100), 2000)
        self._generate_testdata(matrix, 4)
        gen = DataGenerator(self._tmpdir, 4, batch_size=64,
                            shard_merge=False, chunk_size=128)
        obs = partial(self._shard_count, matrix, 4)
        X, _ = DataGeneratorTest._collect_data(gen, obs)
        self._assert_matrix_equal(matrix, X)
        self.assertEqual(len(self._obs_shards), 1)

    def test_no_partial(self):
        matrix = DataGeneratorTest._generate_matrix((100, 100), 999)
        self._generate_testdata(matrix, 4)
        gen = DataGenerator(self._tmpdir, 4, batch_size=64, chunk_size=16, shard_merge=True, allow_partial=False)
        self.assertEqual(len(gen), 15)
        X, _ = DataGeneratorTest._collect_data(gen)
        self.assertEqual(X.shape[0], 15 * 64)
    
    def test_multiple_sizes(self):
        for datapoints in [99, 999, 3333, 4444, 5050, 6060, 9099]:
            matrix = DataGeneratorTest._generate_matrix((100, 100), datapoints)
            self._generate_testdata(matrix, 8)
            gen = DataGenerator(self._tmpdir, 8, batch_size=64, shard_merge=True, chunk_size=7)
            X, _ = DataGeneratorTest._collect_data(gen)
            self._assert_matrix_equal(matrix, X)


if __name__ == '__main__':
    unittest.main()
