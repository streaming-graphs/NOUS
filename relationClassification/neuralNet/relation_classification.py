import pickle
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from keras.utils import *
import numpy as np
from keras.layers import Dense, Dropout, Flatten, Input, MaxPooling1D, Conv1D, Embedding, LSTM
from keras.models import Sequential, Model
from sklearn.metrics import confusion_matrix
import itertools
import numpy as np
import matplotlib.pyplot as plt
from sklearn import svm, datasets
from sklearn.model_selection import train_test_split
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import f1_score

# Dataset generation
with open('train_data.txt', 'rb') as fp:
    train_data = pickle.load(fp)
with open('train_labels.txt', 'rb') as fp:
    train_labels = pickle.load(fp)
with open('test_data.txt', 'rb') as fp:
    test_data = pickle.load(fp)
with open('test_labels.txt', 'rb') as fp:
    test_labels = pickle.load(fp)

MAX_NB_WORDS = 30000
MAX_SEQUENCE_LENGTH = 60

tokenizer = Tokenizer(nb_words=MAX_NB_WORDS)
tokenizer.fit_on_texts(train_data+test_data)

sequences = tokenizer.texts_to_sequences(train_data)
word_index = tokenizer.word_index
print('Found %s unique tokens.' % len(word_index))
train_data = pad_sequences(sequences, maxlen=MAX_SEQUENCE_LENGTH)

sequences = tokenizer.texts_to_sequences(test_data)
test_data = pad_sequences(sequences, maxlen=MAX_SEQUENCE_LENGTH)

labels_dict = {"Other":0 , "Cause-Effect": 1, "Component-Whole": 2, "Entity-Destination": 3, "Product-Producer": 4, 
               "Entity-Origin": 5, "Member-Collection": 6, "Message-Topic": 7, "Content-Container": 8, 
               "Instrument-Agency": 9 }

train_labels = [labels_dict[i] for i in train_labels]
test_labels = [labels_dict[i] for i in test_labels]
test_labels_ori = test_labels
train_labels = to_categorical(np.asarray(train_labels))
test_labels = to_categorical(np.asarray(test_labels))

# Embedding layer

embeddings_index = {}
f = open('word_embedding.txt')
for line in f:
    values = line.split()
    word = values[0]
    coefs = np.asarray(values[1:], dtype='float32')
    embeddings_index[word] = coefs
f.close()

print('Found %s word vectors.' % len(embeddings_index))

EMBEDDING_DIM = 50
embedding_matrix = np.zeros((len(word_index) + 1, EMBEDDING_DIM))
for word, i in word_index.items():
    embedding_vector = embeddings_index.get(word)
    if embedding_vector is not None:
        # words not found in embedding index will be all-zeros.
        embedding_matrix[i] = embedding_vector

embedding_layer = Embedding(len(word_index) + 1,
                            EMBEDDING_DIM,
                            #weights=[embedding_matrix],
                            input_length=MAX_SEQUENCE_LENGTH,
                            trainable=True) # False


# Model configuration
sequence_input = Input(shape=(MAX_SEQUENCE_LENGTH,), dtype='int32')
embedded_sequences = embedding_layer(sequence_input)
x = LSTM(100, dropout=0.2, recurrent_dropout=0.2)(embedded_sequences) #128, 0.2, 0.2
preds = Dense(len(labels_dict), activation='softmax')(x)

model = Model(sequence_input, preds)
model.compile(loss='categorical_crossentropy',
              optimizer='adam', # rmsprop
              metrics=['acc'])

print model.summary()

model.fit(train_data, train_labels, validation_data=(test_data, test_labels),
          epochs=10, batch_size=10) #24

result = model.predict(test_data)
predicted = [np.argmax(vector) for vector in result]

# Confusion matrix 
def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
    else:
        print('Confusion matrix, without normalization')

    print(cm)

    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, cm[i, j],
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')

cnf_matrix = confusion_matrix(predicted, test_labels_ori)
np.set_printoptions(precision=2)

# Plot normalized confusion matrix
plt.figure(figsize=(20,20))
plot_confusion_matrix(cnf_matrix, classes=labels_dict.keys(), normalize=True,
                      title='Normalized confusion matrix')
plt.show()

# Precision, recall and F1 scores
recall = recall_score(predicted, test_labels_ori, average='macro')  
precision = precision_score(predicted, test_labels_ori,average='macro')
print "Precision and recall scores:"
print precision, recall
print ""
print "F1 score:"
print f1_score(predicted, test_labels_ori,average='macro')


