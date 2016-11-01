__author__ = 'weituo'


from collections import OrderedDict
import re

from Vectorizers.relationmentionvectorizer import *
from sklearn.metrics import classification_report
import cPickle as pk
import os
from IPython.core.debugger import Tracer
#from sklearn.cross_validation import train_test_split
from sklearn.model_selection import train_test_split


class myReader(object):
    """
    input: file path to the  training data file
    output: X array of dict[{'segments':[], 'ent1': int, 'ent2':int}]
            y array of labels ['use', ...]
    """
    def __init__(self, file_path):
        self.X = []
        self.y = []

        self.read_data(file_path)

    def read_data(self, file_path):
        """
        eg: <e1>CNN</e1> is used in our <e2>method</e2> (e1,e2)
        """
        with open(file_path, 'r') as reader:
            for line in reader.readlines():
                cur = dict()
                lst = line.strip().split('.')
                s = lst[0]
                me1 = re.search(r'<e1>(.+?)</e1>', s)
                me2 = re.search(r'<e2>(.+?)</e2>', s)
                e1 = me1.group(1)
                e2 = me2.group(1)
                s = s.replace(',', '').replace('<e1>', '').replace('</e1>','').replace('<e2>','').replace('</e2>', '')
                id1 = s.index(e1)
                id2 = s.index(e2)

                pos1 = -1
                pos2 = -1
                segments = list()
                if id1 < id2:
                    temp = s[:id1].split()
                    segments.extend(temp)
                    pos1 = len(temp) + 1
                    temp = s[id1 + len(e1):id2].split()
                    segments.extend(temp)
                    pos2 = len(temp) + 1
                    temp = s[id2 + len(e2):].split()
                    segments.extend(temp)
                else:
                    temp = s[:id2].split()
                    segments.extend(temp)
                    pos2 = len(temp) + 1
                    temp = s[id2 + len(e2):id1].split()
                    segments.extend(temp)
                    pos1 = len(temp) + 1
                    temp = s[id1 + len(e1):].split()
                    segments.extend(temp)

                total = len(segments)
                mid = total / 2

                pos1 = pos1 - mid  # centered ?
                pos2 = pos2 - mid

                cur['segments'] = segments
                cur['ent1'] = pos1
                cur['ent2'] = pos2
                self.X.append(cur)
                self.y.append('use') # if no label presented in training data

def loadData(train_file_path="./data/useRelationTrain.txt", test_file_path="./data/useRelationTest.txt", mix=True):
    """
    vectorize the new added training data, if mix = True, mix these training data with prevectorized data
    """
    mytrain_path = train_file_path
    mytest_path = test_file_path
    if os.path.exists(mytrain_path):
        print "read relation training data..."
        p = myReader(mytrain_path)
    if os.path.exists(mytest_path):
        print "read relation testing data..."
        p_test = myReader(mytest_path)

    addedNum = len(p.X) # the number of added training data
    testNum = len(p_test.y) # the number of added testing data

    p.X.extend(p_test.X)
    p.y.extend(p_test.y)
    vectorizer = RelationMentionVectorizer(threads=32)
    vectorizer.fit(p.X)
    print "vectorizing data.."
    newx_train = vectorizer.transform(p.X)
    newy_train = np.array(p.y)
    newx_m = newx_train.shape[1]

    if mix:
        print "load prevectorized data ..."
        if not os.path.exists("./saved-models/dataset_X.p"):
            print "preprocessed data file doesn't exist.. "
        else:
            def padding(x, num):
                temp = np.zeros([num, x.shape[1]], np.float32)
                return np.vstack([x, temp])

            print "preprocessed file exists.. loading.."
            X = np.load(open("./saved-models/dataset_X.p", 'r'))
            y = np.load(open("./saved-models/dataset_y.p", 'r'))
            m = X.shape[1]

            # randomly choose 3 times addedNum samples as mix data
            sample_id = np.random.choice(X.shape[0], 3 * addedNum)
            smallX = X[sample_id]
            smallY = y[sample_id]

            # currently different set of data has different m
            if newx_m > m:
                padsize = newx_m - m
                padlst = list()
                for x in smallX:
                    padlst.append(padding(x, padsize))
                smallX = np.array(padlst)
            else:
                padsize = m - newx_m
                padlst = list()
                for x in newx_train:
                    padlst.append(padding(x, padsize))
                newx_train = np.array(padlst)

    totalNum = newx_train.shape[0]
    testX = newx_train[range((totalNum - testNum), totalNum), :]
    testy = newy_train[range((totalNum - testNum), totalNum)]

    tempX = newx_train[range(totalNum - testNum), :]
    tempy = newy_train[range(totalNum - testNum)]
    trainX = np.concatenate((smallX, tempX))
    trainy = np.concatenate((smallY, tempy))

    np.save(open("./saved-models/trainUse_X.p", 'w'), trainX)
    np.save(open("./saved-models/trainUse_y.p", 'w'), trainy)

    np.save(open("./saved-models/testUse_X.p", 'w'), testX)
    np.save(open("./saved-models/testUse_y.p", 'w'), testy)


    print "Done loading !"
if __name__ == '__main__':
    loadData()




