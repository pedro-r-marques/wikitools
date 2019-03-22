# -*- coding: utf-8 -*-

"""
"""

from __future__ import generators, print_function

import copy
import functools
import os
import struct
import numpy as np

from tensorflow import keras

from .dictionary import Dictionary


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


class DataGenerator(keras.utils.Sequence):
    """
    Large matrix distributed across num_shard files.

    Example: 64 files with 3M data points each.

    In order to support data shuffling the code reads a matrix chunk across the multiple shards
    which is expected to be larger than the batch_size. It then serves shuffled permutations of these
    chunks. e.g. 10k lines per file (x64). Then serve permuted 8k chunks for data.

    Reads from shard files are done in increments of chunk_size.
    """

    FORMAT = 'IIf'

    def __init__(self, data_dir, num_shards, batch_size=32, chunk_size=16*1024, shuffle=False, shard_merge=False, allow_partial=True):
        self._data_dir = data_dir
        self._batch_size = batch_size
        self._chunk_size = chunk_size
        self._shuffle = shuffle
        self._shard_merge = shard_merge
        self._allow_partial = allow_partial
        self._filenames = self._get_filenames(data_dir, num_shards)
        self._filedescr = [open(f, 'rb') for f in self._filenames]
        self._file_offsets = [0] * len(self._filenames)
        self._data_counts = self._file_size()
        if shard_merge:
            self._build_chunk_sizes()

        # chunk state
        self._current_chunk_id = None
        self._X_data = None
        self._y_labels = None
        self._data_offset = 0

    def __del__(self):
        for file in self._filedescr:
            file.close()

    def _get_filenames(self, data_dir, num_shards):
        filenames = [os.path.join(data_dir, 'data.bin.{0:05d}-of-{1:05d}'.format(
            shard_id, num_shards)) for shard_id in range(num_shards)]
        return filenames

    def _file_size(self):
        """
        Returns the number of records in each file.
        """
        counts = []
        for filename in self._filenames:
            file_info = os.stat(filename)
            count = int(file_info.st_size /
                        struct.calcsize(DataGenerator.FORMAT))
            counts.append(count)
        return counts

    def _build_chunk_sizes(self):
        """
        Not all chunk sizes are the same: upto the min shard file all chunks provide data. Once data is exausted
        from a shard, the chunk size decreases.
        """
        chunk_size_list = []
        sizes = copy.copy(self._data_counts)
        last_index = 0
        while len(sizes):
            m = min(sizes)
            # common_idx is the next chunk at which an fd ends.
            common_idx = int(m / self._chunk_size)
            if common_idx > last_index:
                current_size = self._chunk_size * len(sizes)
                chunk_size_list.append((common_idx, current_size))

            # indexes of blocks that end at common_idx
            chunk_start = common_idx * self._chunk_size
            chunk_end = chunk_start + self._chunk_size
            indices = [i for i, v in enumerate(
                sizes) if v >= chunk_start and v < chunk_end]

            # sum the extra size of the shards that end at common_idx
            def sum_pair(a, b): return a + b
            chunk_last_sum = functools.reduce(sum_pair, map(
                lambda x: sizes[x] - chunk_start, indices))
            indices.reverse()
            for i in indices:
                del sizes[i]

            if chunk_last_sum:
                n_current_size = self._chunk_size * len(sizes)
                chunk_size_list.append(
                    (common_idx + 1, n_current_size + chunk_last_sum))
                last_index = common_idx + 1
            else:
                last_index = common_idx

        # validate: sum of the sizes should equal the total number of records.
        accum = 0
        prev = 0
        for idx, size in chunk_size_list:
            accum += size * (idx - prev)
            prev = idx
        assert accum == sum(self._data_counts)
        self._chunk_size_list = chunk_size_list

    def _get_chunk_id(self, block_id: int) -> int:
        """
        Map block start offset to the chunk that provides the data.
        In case where we are merging data from different chunks, it requires computing the offset
        based on different sizes per interval.
        """
        batch_offset = block_id * self._batch_size
        if not self._shard_merge:
            accum = 0
            offset = 0
            for size in self._data_counts:
                if batch_offset <  size:
                    n_id = int(batch_offset / self._chunk_size)
                    return accum + n_id
                batch_offset -= size
                accum += int((size - 1) / self._chunk_size) + 1

        chunk_index = 0
        prev = 0
        for idx, chunk_size in self._chunk_size_list:
            offset = (idx - prev) * chunk_size
            if offset >= batch_offset:
                # index is calculated in terms of the current size
                ext = int(batch_offset / chunk_size)
                return chunk_index + ext
            batch_offset -= offset
            chunk_index = idx
            prev = idx

        assert False

    def _read_chunk(self, chunk_id):
        # Return immediatly if the chunk is already in memory.
        if chunk_id == self._current_chunk_id:
            return True

        rows = []
        cols = []
        values = []
        bsize = struct.calcsize(DataGenerator.FORMAT)
        rel_chunk_id = chunk_id

        for idx, file in enumerate(self._filedescr):
            if self._shard_merge:
                want_offset = chunk_id * self._chunk_size
            else:
                n_chunks = int((self._data_counts[idx] - 1) / self._chunk_size) + 1
                if rel_chunk_id >= n_chunks:
                    rel_chunk_id -= n_chunks
                    continue
                want_offset = rel_chunk_id * self._chunk_size

            if want_offset > self._data_counts[idx]:
                continue
            if want_offset != self._file_offsets[idx]:
                file.seek(want_offset * bsize)
                self._file_offsets[idx] = want_offset

            buf = file.read(self._chunk_size * bsize)
            if len(buf) == 0:
                continue
            n_records = int(len(buf) / bsize)
            self._file_offsets[idx] += n_records
            for i in range(n_records):
                elements = struct.unpack_from(
                    DataGenerator.FORMAT, buf, i * bsize)
                rows.append(elements[0])
                cols.append(elements[1])
                values.append(elements[2])
            if not self._shard_merge:
                break

        if not len(values):
            return False

        X = np.array([rows, cols], dtype=int).T
        y = np.array(values, dtype=float)
        if self._shuffle:
            perm = np.random.permutation(X.shape[0])
            X = X[perm]
            y = y[perm]

        self._current_chunk_id = chunk_id
        self._X_data = X
        self._y_labels = y
        self._data_offset = 0
        return True

    def __len__(self):
        'Denotes the number of batches per epoch'
        size = np.sum(self._data_counts)
        assert size > 0
        if self._allow_partial:
            return int((size - 1) / self._batch_size) + 1
        return int(size / self._batch_size)

    def size(self):
        return np.sum(self._data_counts)

    def __getitem__(self, index):
        'Generate one batch of data'
        chunk_id = self._get_chunk_id(index)
        assert self._read_chunk(chunk_id)            

        X = np.empty((self._batch_size, 2), dtype=int)
        y = np.empty((self._batch_size,))

        index = 0
        while index < self._batch_size:
            avail = self._X_data.shape[0] - self._data_offset
            if avail == 0:
                chunk_id += 1
                if not self._read_chunk(chunk_id):
                    assert self._allow_partial
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
