import argparse
import os
import sys

import numpy as np

if __name__ == '__main__':
    curdir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(os.path.dirname(curdir))

from wiki_entity_vec.model.model import make_model
from wiki_entity_vec.util import data_generator

from tensorflow import keras
from tensorflow.keras import backend as K # pylint: disable=import-error

if sys.platform == 'darwin':
    import matplotlib
    matplotlib.use("TkAgg")

import matplotlib.pyplot as plt

from keras_opt import scipy_optimizer


def load_weights(dir: str, model):
    if not os.path.isdir(dir):
        return
    with open(os.path.join(dir, 'weights.npz'), 'rb') as file:
        npz = np.load(file)
        for i, w in enumerate(model.trainable_weights):
            K.set_value(w, npz[npz.files[i]])

def save_weights(dir: str, model):
    os.makedirs(dir, exist_ok=True)
    np.savez(os.path.join(dir, 'weights.npz'), *K.batch_get_value(model.trainable_weights))

class WeightsSaver(keras.callbacks.Callback):
    def __init__(self, dir, model):
        self._output_dir = dir
        self._model = model

    def on_epoch_end(self, epoch, logs=None):
        save_weights(self._output_dir, self._model)

def history_plot(dir, hist):
    plt.figure()
    plt.plot(hist.history['loss'])
    plt.title('Model loss')
    plt.ylabel('Loss')
    plt.xlabel('Epoch')
    plt.plot()
    plt.savefig(os.path.join(dir, 'history.svg'), format='svg')

def save_result(output_dir: str, x : np.array, embedding_size: int):
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, 'vectors.tsv'), 'w') as file:
        for i in range(int(x.shape[0] / embedding_size)):
            begin = i * embedding_size
            end = begin + embedding_size
            file.write('\t'.join([str(v) for v in x[begin:end]]))
            file.write('\n')


def main():
    parser = argparse.ArgumentParser(
        description='Wikipedia entity model trainer')
    parser.add_argument('--data_shards', type=int, default=64)
    parser.add_argument('--embedding_size', type=int, default=100)
    parser.add_argument('--batch_size', type=int, default=2*1024*1024)
    parser.add_argument('--epochs', type=int, default=100)
    parser.add_argument('--page_dictionary', required=True)
    parser.add_argument('--category_file', required=True)
    parser.add_argument(
        '--output_dir', help='Directory for trained weights and checkpoints')
    parser.add_argument('--load_weights', default=True)
    parser.add_argument('--logs_dir', help='TensorBoard logs directory')
    parser.add_argument('matrix_dir', help='Data matrix directory')
    args = parser.parse_args()

    dataset = data_generator.Dataset()
    dataset.load_dictionaries(args.page_dictionary, args.category_file)

    generator = data_generator.DataGenerator(
        args.matrix_dir, args.data_shards, batch_size=args.batch_size, chunk_size=args.batch_size)
    model = make_model(dataset.get_shape(), args.embedding_size)
    if args.output_dir and args.load_weights:
        load_weights(args.output_dir, model)

    callbacks = []
    if args.output_dir:
        callbacks.append(WeightsSaver(args.output_dir, model))
    if args.logs_dir:
        tbCallback = keras.callbacks.TensorBoard(log_dir=args.logs_dir, write_grads=True, write_graph=True)
        callbacks.append(tbCallback)

    opt = scipy_optimizer.ScipyOptimizer(model)
    result, history = opt.fit_generator(
        generator, callbacks=callbacks, epochs=args.epochs, verbose=False)
    print('Function cost: ', result['fun'])
    print('Optmizer: ', result['message'])
    history_plot(args.output_dir, history)
    save_weights(args.output_dir, model)
    save_result(args.output_dir, result['x'][:dataset.get_shape()[0]*args.embedding_size], args.embedding_size)


if __name__ == '__main__':
    main()
