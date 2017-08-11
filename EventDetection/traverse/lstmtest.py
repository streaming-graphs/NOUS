import numpy
# from keras.datasets import imdb
from keras.models import Sequential
# from keras.models import Model
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers.embeddings import Embedding
# from keras.preprocessing import sequence
from pushbullet import Pushbullet
import sklearn.metrics as sm
import sys


# fix random seed for reproducibility
numpy.random.seed(7)
# flips = int(sys.argv[1])
# pl = int(sys.argv[2])
pl = int(sys.argv[1])

# Pushbullet Notification
pb = Pushbullet('API Key')
# pb.push_note('Job Started!' + '_' + str(flips) + 'f_pl' + str(pl), 'This is your notice')
pb.push_note('LSTM Job Started!', ' ')

# load the dataset but only keep the top n words, zero the rest
# top_words = 45000
top_words = 225
# (X_train, y_train), (X_test, y_test) = imdb.load_data(num_words=top_words)
# directory = 'data/wordnet/versions/'
directory = 'data/streamspot/data/'
# all_samples = numpy.genfromtxt(directory + 'all_samples_nums_'
# + str(flips) + 'f_pl' + str(pl) + '.txt').astype('int32')
all_samples = numpy.genfromtxt(directory + 'all_samples_' + str(pl) + 'star_big_thin.txt').astype('int32')

y = all_samples[:, -1]
all_samples = all_samples[:, :-1]

tot_idc = all_samples.shape[0]
tr_idc = 2 * tot_idc // 3
va_idc = 5 * tot_idc // 6

y_train = y[:tr_idc]
y_valid = y[tr_idc+1:va_idc]
y_test = y[va_idc+1:]

X_train = all_samples[:tr_idc, :]
X_valid = all_samples[tr_idc+1:va_idc, :]
X_test = all_samples[va_idc+1:, :]

# print y.shape
# print type(X_train)
# print X_train[0]
# print y_train[0]
# print y_train.shape
# print y_test.shape
# print X_train.shape
# print X_test.shape
# exit(0)
# truncate and pad input sequences
max_review_length = 2 * pl + 1
# X_train = sequence.pad_sequences(X_train, maxlen=max_review_length)
# X_test = sequence.pad_sequences(X_test, maxlen=max_review_length)

# create the model
embedding_vector_length = 32
model = Sequential()
model.add(Embedding(top_words, embedding_vector_length, input_length=max_review_length))
model.add(LSTM(100))
model.add(Dense(1, activation='sigmoid'))
model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
# f.write(model.summary())
# f.write('\n')
model.fit(X_train, y_train, validation_data=(X_valid, y_valid), epochs=3, batch_size=64)

# Get embeddings
# embeds = model.get_weights()[0]
# numpy.save('embeds_' + str(flips) + 'f_pl' + str(pl) + '.npy', embeds)
# exit(0)
# Final evaluation of the model
scores = model.evaluate(X_test, y_test, verbose=0)
predicts = model.predict(X_test)

# Save variables for coherence mapping
# numpy.save('X_test' + flips + '.npy', X_test)
# numpy.save('y_test' + flips + '.npy', y_test)
# numpy.save('y_preds' + flips + '.npy', predicts)

# Thresholding
tpredicts = [1 if x >= 0.5 else 0 for x in predicts]

# Get incorrectly classified paths
# idc = []
# for i in range(len(tpredicts)):
#     if tpredicts[i] != y_test[i]:
#         idc.append(i)
# wc = X_test[idc]
# wc_preds = predicts[idc]
# write_this = numpy.hstack((wc, wc_preds))
# numpy.save('wrongly_classified' + flips + '.npy', write_this)
# numpy.save('actual_preds_of_wc.npy', wc_preds)
print('______________________________________________________')
# print('_' + str(flips) + 'f_pl' + str(pl))

acc = 'Accuracy: %.2f%%' % (scores[1]*100)
print(acc)
print('__________________')
print('Confusion Matrix:')
print sm.confusion_matrix(y_test, tpredicts)
labels = ['Positive', 'Negative']
print('__________________')
print('Classification Report:')
print sm.classification_report(y_test, tpredicts, target_names=labels)

# Pushbullet Notification
# pb.push_note(acc + '_' + str(flips) + 'f_pl' + str(pl), "This is your notice")
pb.push_note(acc, 'Done')
