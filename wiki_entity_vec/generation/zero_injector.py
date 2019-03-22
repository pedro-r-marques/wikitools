# -*- coding: utf-8 -*-

"""
Read the full matrix into memory, add zeros and save it.
"""

import argparse
import os
import random
import struct

from data_generator import Dataset, DataGenerator

from scipy.sparse import dok_matrix, save_npz

from tqdm import tqdm

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--page_dictionary', required=True)
    parser.add_argument('--category_file', required=True)
    parser.add_argument('--input_dir', required=True)
    parser.add_argument('--shard_id', type=int, required=True)
    parser.add_argument('--num_shards', type=int, default=64)
    parser.add_argument('--output_dir', required=True)
    parser.add_argument('--factor', type=float, default=1.0)
    args = parser.parse_args()

    random.seed(20190308)
    ds = Dataset()
    ds.load_dictionaries(args.page_dictionary, args.category_file)
    shape = ds.get_shape()
    matrix = dok_matrix(shape, dtype=float)

    block_size = (shape[0] - 1) / args.num_shards + 1
    block_start = args.shard_id * block_size
    block_end = min(block_start + block_size, shape[0])
    print('Shard {0}, rows: [{1}, {2})'.format(args.shard_id, block_start, block_end))

    n_rows = block_end - block_start

    gen = DataGenerator(args.input_dir, 64, batch_size=16*1024)

    density = float(gen.size()) / shape[0]
    zeros = int(density * n_rows * args.factor)
    print('Row density: {0}, zeros: {1}'.format(density, zeros))

    print('Reading matrix data in {0} segments'.format(len(gen)))
    for i in tqdm(xrange(len(gen))):
        X, y = gen[i]
        for n in range(X.shape[0]):
            if X[n, 0] >= block_start and X[n, 0] < block_end:
                matrix[X[n, 0], X[n, 1]] = y[n]

    print('Generating {0} zeros for {1} positives'.format(zeros, matrix.size))
    for _ in tqdm(xrange(zeros)):
        while True:
            row = block_start + random.randint(0, block_end - block_start - 1)
            col = random.randint(0, shape[1] - 1)
            if matrix.has_key((row, col)):
                continue
            matrix[row, col] = -1.0
            break

    matrix = matrix.tocoo()
    print('Matrix size: {0}'.format(matrix.size))

    filename = '{0}.bin.{1:05d}-of-{2:05d}'.format('data', args.shard_id, args.num_shards)
    print('Saving {0}...'.format(filename))
    with open(os.path.join(args.output_dir, filename), 'w') as output:
        for i in xrange(matrix.size):
            row = matrix.row[i]
            col = matrix.col[i]
            value = 0.0 if matrix.data[i] == -1.0 else matrix.data[i]
            output.write(struct.pack('IIf', row, col, value))
    
if __name__ == '__main__':
    main()