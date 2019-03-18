# -*- coding: utf-8 -*-

"""
"""

from __future__ import generators, print_function

import os
import struct
import numpy as np

from tensorflow.python import keras
from tensorflow.keras.utils import Sequence

from dictionary import Dictionary


class Dataset(object):
    def __init__(self):
        self._page_dictionary = Dictionary()
        self._category_dict = Dictionary()

    def load_dictionaries(self, page_dict, category_dict):
        self._page_dictionary.load(page_dict)
        self._category_dict.load(category_dict)

    def get_shape(self):
        rows = self._page_dictionary.size()
        cols = self._page_dictionary.size() * 2 + self._category_dict.size()
        return (rows, cols)


class DataGenerator(Sequence):
    """
    Large matrix distributed across num_shard files.

    Example: 64 files with 3M data points each.

    In order to support data shuffling the code reads a matrix chunk across the multiple shards
    which is expected to be larger than the batch_size. It then serves shuffled permutations of these
    chunks. e.g. 10k lines per file (x64). Then serve permuted 8k chunks for data.
    """

    FORMAT = 'IIf'

    def __init__(self, data_dir, num_shards, batch_size=32, shuffle=False, shard_merge=False, chunk_size=16*1024):
        self._chunk_size = chunk_size
        self._data_dir = data_dir
        self._batch_size = batch_size
        self._shuffle = shuffle
        self._shard_merge = shard_merge
        self._filenames = self._get_filenames(data_dir, num_shards)
        self._data_counts = self._line_counts()
        self._start_epoch()

    def _get_filenames(self, data_dir, num_shards):
        filenames = [os.path.join(data_dir, 'data.bin.{0:05d}-of-{1:05d}'.format(
            shard_id, num_shards)) for shard_id in range(num_shards)]
        return filenames

    def _line_counts(self):
        counts = []
        for filename in self._filenames:
            file_info = os.stat(filename)
            count = file_info.st_size / struct.calcsize(DataGenerator.FORMAT)
            counts.append(count)
        return counts

    def _start_epoch(self):
        self._filedescr = [open(f, 'r') for f in self._filenames]
        self._read_count = 0
        self._read_chunk()

    def _read_chunk(self):
        rows = []
        cols = []
        values = []
        bsize = struct.calcsize(DataGenerator.FORMAT)
        for file in self._filedescr:
            buf = file.read(self._chunk_size * bsize)
            if len(buf) == 0:
                continue
            for i in xrange(len(buf) / bsize):
                elements = struct.unpack_from(
                    DataGenerator.FORMAT, buf, i * bsize)
                rows.append(elements[0])
                cols.append(elements[1])
                values.append(elements[2])
            if not self._shard_merge:
                break

        if not len(values):
            return False

        self._read_count += 1

        X = np.array([rows, cols], dtype=int).T
        y = np.array(values, dtype=float)
        if self._shuffle:
            perm = np.random.permutation(X.shape[0])
            X = X[perm]
            y = y[perm]

        self._X_data = X
        self._y_labels = y
        self._data_offset = 0
        return True

    def __len__(self):
        'Denotes the number of batches per epoch'
        size = np.sum(self._data_counts)
        assert size > 0
        return (size - 1) / self._batch_size + 1

    def size(self):
        return np.sum(self._data_counts)

    def __getitem__(self, index):
        'Generate one batch of data'
        X = np.empty((self._batch_size, 2))
        y = np.empty((self._batch_size,))

        index = 0
        while index < self._batch_size:
            avail = self._X_data.shape[0] - self._data_offset
            if avail == 0:
                if not self._read_chunk():
                    X = np.resize(X, (index, 2))
                    y = np.resize(y, (index,))
                    break
                avail = self._X_data.shape[0]
            need = self._batch_size - index
            use = min(avail, need)
            X[index:index+use, :] = self._X_data[self._data_offset:self._data_offset+use, :]
            y[index:index+use] = self._y_labels[self._data_offset:self._data_offset+use]
            self._data_offset += use
            index += use

        return X, y

    def on_epoch_end(self):
        """Method called at the end of every epoch.
        """
        self._start_epoch()
