# Generate training data and labels
import pickle

train_data = []
train_labels = []

f = open('SemEval2010_task8_all_data/words/TRAIN_FILE.TXT', 'r')
#f = open('/Users/zhao279/Desktop/development/SemEval2010_task8_all_data/words/TRAIN_FILE.TXT', 'r')

doc = f.readlines()

for i in range(0, len(doc), 4):
    train_data.append(doc[i].split('"')[1])
    label = doc[i+1].split("(")[0]
    label = label.strip()
    train_labels.append(label)

print "Number of training samples:"
print len(train_data)
print len(train_labels)
print ""

with open('train_data.txt', 'wb') as fp:
	pickle.dump(train_data, fp)
with open('train_labels.txt', 'wb') as fp:
	pickle.dump(train_labels, fp)
	

# Generate testing data and labels
test_data = []
test_labels = []

f = open('SemEval2010_task8_all_data/words/TEST_FILE.txt', 'r')
doc = f.readlines()
for i in range(0, len(doc)):
    test_data.append(doc[i].split('"')[1])

f = open('SemEval2010_task8_all_data/words/TEST_FILE_KEY.TXT', 'r')
doc = f.readlines()
for i in range(0, len(doc)):
    test_labels.append(doc[i].strip().split('\t')[1])

print "Number of testing samples:"
print len(test_data)
print len(test_labels)

with open('test_data.txt', 'wb') as fp:
	pickle.dump(test_data, fp)
with open('test_labels.txt', 'wb') as fp:
	pickle.dump(test_labels, fp)