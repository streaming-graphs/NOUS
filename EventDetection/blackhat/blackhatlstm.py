import numpy
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers.embeddings import Embedding
import sys


# Directoy that contains the data and pattern files
directory = sys.argv[1]

# Reading pattern file
name2num = {'Dev': 1001, 'Admin': 1002, 'Mgmt': 1003, 'IT': 1004, 'HR': 1005,
            'SkypeEmail': 1006, 'CloudServices': 1007, 'Computing': 1008, 'Infra': 1009, 'Ext': 1010}

num2name = {}
for k, v in name2num.items():
    num2name[v] = k

# Get test data from pattern file
pfname = directory + 'random_graph.pattern.txt'
X_test = []
with open(pfname, 'rb') as f:
    for line in f:
        line_list = line.split(',')
        if len(line_list) < 2:
            continue
        else:
            line_list = line_list[:2]
        src, src_id = line_list[0].strip().split('_')
        dest, dest_id = line_list[1].strip().split('_')
        connection = [name2num[src], int(src_id), name2num[dest], int(dest_id)]
        X_test.append(connection)
X_test = numpy.array(X_test)
# numpy.save(directory + 'xtest.npy', X_test)

# Get training data
training = numpy.load(directory + 'training.npy').astype('int32')
# X_test = numpy.load(directory + 'testing.npy').astype('int32')

ytrain = numpy.load(directory + 'training_labels.npy').astype('float32')
# y_test = numpy.load(directory + 'testing_labels.npy').astype('float32')

# Thresholding
ytrain = [1 if x <= 2.26 else 0 for x in ytrain]

# Divide the data
tot_idc = training.shape[0]
tr_idc = tot_idc // 2
va_idc = tr_idc + 1

y_train = ytrain[:tr_idc]
y_valid = ytrain[va_idc:]

X_train = training[:tr_idc, :]
X_valid = training[va_idc:, :]

# Create the model
max_review_length = len(X_train[0])
top_words = 1015
embedding_vector_length = 32
model = Sequential()
model.add(Embedding(top_words, embedding_vector_length, input_length=max_review_length))
model.add(LSTM(100))
model.add(Dense(1, activation='sigmoid'))
model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])
model.fit(X_train, y_train, validation_data=(X_valid, y_valid), epochs=1, batch_size=64)

# Final evaluation of the model
predicts = model.predict(X_test)
tpredicts = [1 if x >= 0.5 else 0 for x in predicts]
# numpy.save(directory + 'tpreds2.npy', tpredicts)


# Output English
def get_msg(sc, ec):
    return ' '.join([num2name[nts[0]] + '_' + str(nts[1]) + ' ' + num2name[nts[2]] + '_' + str(nts[3])
                     for xt, nts in enumerate(X_test[sc:ec, :]) if tpredicts[sc+xt] == 1])


new_lines = ''
with open(pfname, 'r') as wf:
    lines = wf.readlines()
    start_ct = 0
    end_ct = 0
    for i, line in enumerate(lines):
        line_list = line.split(',')
        lll = len(line_list)
        if lll < 2:
            continue
        if lll <= 3:
            end_ct += 1
            new_lines += line
        if lll > 3:
            end_ct += 1
            if all(tpredicts[start_ct:end_ct]):
                msg = '. All edges are abnormal'
            elif all([x == 0 for x in tpredicts[start_ct:end_ct]]):
                msg = ''
            else:
                msg = '. The edge(s) ' + get_msg(start_ct, end_ct) + ' is/are abnormal'
            start_ct = end_ct
            line_list[-1] = line_list[-1].strip()
            new_lines += ', '.join(line_list) + msg + '\n'

    with open(directory + 'random_graph.pattern.decisions.txt', 'w') as wwf:
        for line in new_lines:
            wwf.write(line)
