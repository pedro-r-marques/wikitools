import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import Dot, Embedding, Input, Lambda # pylint: disable=import-error
from tensorflow.keras.models import Model # pylint: disable=import-error
from keras_opt import scipy_optimizer

from typing import Tuple

def make_model(shape : Tuple[int, int], embedding_size: int) -> Model:
    coordinates = Input(shape=(2,), dtype=tf.int32)
    row_embedding = Embedding(shape[0], embedding_size, name='row_embedding', input_length=1)
    col_embedding = Embedding(shape[1], embedding_size, name='col_embedding', input_length=1)
    row = Lambda(lambda x: x[:, 0])(coordinates)
    col = Lambda(lambda x: x[:, 1])(coordinates)
    row_vecs = row_embedding(row)
    col_vecs = col_embedding(col)
    y_r = Dot(1)([row_vecs, col_vecs])

    model = Model(inputs=coordinates, outputs=y_r)
    model.compile(optimizer=scipy_optimizer.GradientObserver(), loss='mean_squared_error')
    return model
