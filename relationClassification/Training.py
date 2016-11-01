__author__ = 'weituo'

from sklearn.metrics import classification_report
from CNN import *
import cPickle as pk
import os
from IPython.core.debugger import Tracer
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle
from Vectorization import *


## load the data if it exists else call Vectorization.py
if not os.path.exists("./saved-models/trainUse_X.p"):
    loadData()
else:
    print "preprocessed experiment file exists.. loading.."
    X = np.load(open("./saved-models/trainUse_X.p", 'r'))
    y = np.load(open("./saved-models/trainUse_y.p", 'r'))


# treat relation otherthan noedge use as "other"
other_indices = np.where((y != "NoEdge") & (y != "use"))[0]
y[other_indices] = "other"

useful_indices = np.where((y != "NoEdge"))[0]
noedge_indices = np.where((y == "NoEdge"))[0][0:len(useful_indices) / 2]

selector = np.append(useful_indices, noedge_indices)
X = X[selector]
y = y[selector]

X, y = shuffle(X, y, random_state=0) # should we make sure the CNN can learn something if epoch is not enough

x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)


####################
# Training the CNN #
####################
max_w = X.shape[1]
print max_w

x_train = np.reshape(x_train, [-1, max_w, 320, 1])
x_test = np.reshape(x_test, [-1, max_w, 320, 1])

cnn = CNN(input_shape=[max_w, 320, 1], classes=np.unique(y), conv_shape=[4, 55], epochs=50)
cnn.fit(x_train, y_train, x_test, y_test, usePreTrained=True)
#cnn.fit(x_train, y_train, x_test, y_test)

print "done training"
print "testing"


#########
# TEST
########
"""
if not os.path.exists("./saved-models/testUse_X.p"):
    print "no test data file found, check your vectorization process"
else:
    def extractPair(line):
        me1 = re.match(r'<e1.*?>(.+?)</e1>', line)
        me2 = re.match(r'<e2.*?>(.+?)</e2>', line)
        return me1.group(1), me2.group(1)

    testX = np.load(open("./saved-models/testUse_X.p", 'r'))
    testy = np.load(open("./saved-models/testUse_y.p", 'r'))

    testX = np.reshape(testX, [-1, max_w, 320, 1])
    y_pred = np.array([])

    for c, t in enumerate(Batcher.chunks(testX, 1)):
        y_pred = np.append(y_pred, cnn.predict(t))

    print y_pred
    idx = np.array(range(len(testX)))[y_pred == testy]
    mytest_path = "./data/useRelationTest.txt"
    with open(mytest_path, 'r') as reader:
        lines = reader.readlines()
        for i in idx:
            e1str, e2str = extractPair(lines[i])
            print "the %dth sentence is use relation:" % (i)
            print "e1_segment: %s  vs  e2_segment: %s" % (e1str, e2str)

"""
