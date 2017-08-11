# author - Richard Liao
# Dec 26 2016
import os
os.environ['KERAS_BACKEND'] = 'theano'


from keras.utils.np_utils import to_categorical
from keras.layers import Dense, Input
from keras.layers import Embedding, GRU, Bidirectional, TimeDistributed
from keras.models import Model
from keras import backend as K
from keras.engine.topology import Layer
from keras import initializations
from sklearn.metrics import classification_report as cr
from sklearn.metrics import confusion_matrix as cm
import numpy as np
from pushbullet import Pushbullet


# Pushbullet Notification
pb = Pushbullet('API key')
pb.push_note('HATTN Job Started!', ' ')

# Set the constants
NUM_WORDS = 78
MAX_SENT_LENGTH = 100
MAX_SENTS = 124
EMBEDDING_DIM = 100
VALIDATION_SPLIT = 0.2
TEST_SPLIT = 0.2
directory = 'data/streamspot/data/all_divs/'

# Read the data and create the labels
data = np.load('kdd_data124.npy')
labels = np.zeros((data.shape[0], 1), dtype='int32')
labels[300:400] = 1
labels = to_categorical(labels)

# Shuffle the data
p = np.random.permutation(data.shape[0])
data = data[p, :, :]
labels = labels[p, :]

# Divide the data into train, valid and test
nb_validation_samples = int(VALIDATION_SPLIT * data.shape[0])
nb_test_samples = int(TEST_SPLIT * data.shape[0])

x_test = data[-nb_test_samples:]
y_test = labels[-nb_test_samples:]

data = data[:-nb_test_samples]
labels = labels[:-nb_test_samples]

x_train = data[:-nb_validation_samples]
y_train = labels[:-nb_validation_samples]
x_val = data[-nb_validation_samples:]
y_val = labels[-nb_validation_samples:]

# pb.push_note('Model creation begins', '!!')
# Create the HATTN Model one layer at a time
embedding_layer = Embedding(NUM_WORDS, EMBEDDING_DIM, input_length=MAX_SENT_LENGTH, trainable=True)


class AttLayer(Layer):
    def __init__(self, **kwargs):
        self.init = initializations.get('normal')
        # self.input_spec = [InputSpec(ndim=3)]
        super(AttLayer, self).__init__(**kwargs)

    def build(self, input_shape):
        assert len(input_shape) == 3
        # self.W = self.init((input_shape[-1],1))
        self.W = self.init((input_shape[-1],))
        # self.input_spec = [InputSpec(shape=input_shape)]
        self.trainable_weights = [self.W]
        super(AttLayer, self).build(input_shape)  # be sure you call this somewhere!

    def call(self, x, mask=None):
        eij = K.tanh(K.dot(x, self.W))

        ai = K.exp(eij)
        weights = ai / K.sum(ai, axis=1).dimshuffle(0, 'x')

        weighted_input = x * weights.dimshuffle(0, 1, 'x')
        return weighted_input.sum(axis=1)

    def get_output_shape_for(self, input_shape):
        return (input_shape[0], input_shape[-1])


sentence_input = Input(shape=(MAX_SENT_LENGTH,), dtype='int32')
embedded_sequences = embedding_layer(sentence_input)
l_lstm = Bidirectional(GRU(100, return_sequences=True))(embedded_sequences)
l_dense = TimeDistributed(Dense(200))(l_lstm)
l_att = AttLayer()(l_dense)
sentEncoder = Model(sentence_input, l_att)

review_input = Input(shape=(MAX_SENTS, MAX_SENT_LENGTH), dtype='int32')
review_encoder = TimeDistributed(sentEncoder)(review_input)
l_lstm_sent = Bidirectional(GRU(100, return_sequences=True))(review_encoder)
l_dense_sent = TimeDistributed(Dense(200))(l_lstm_sent)
l_att_sent = AttLayer()(l_dense_sent)
preds = Dense(2, activation='softmax')(l_att_sent)

model = Model(review_input, preds)

model.compile(loss='categorical_crossentropy', optimizer='rmsprop', metrics=['acc'])

# pb.push_note('Model fitting begins', '!!')
model.fit(x_train, y_train, validation_data=(x_val, y_val), nb_epoch=6, batch_size=50)
model.save('han_model_kdd3.h5')

# Evaluate
# pb.push_note('Model evaluation begins', '!!')
scores = model.evaluate(x_test, y_test, verbose=0)[1] * 100
preds = model.predict(x_test)
np.save('preds_kdd6.npy', preds)
np.save('acts_kdd6.npy', y_test)
tpreds = [0 if p[0] >= p[1] else 1 for p in preds]
tacts = [0 if a[0] >= a[1] else 1 for a in y_test]

print scores
print cr(tacts, tpreds)
print cm(tacts, tpreds)
pb.push_note(str(scores), 'Done')
