import h5py as h5
import json
import numpy as np
import os


def generate(fname, epoch, batchsize, seed=24, small=False):
    """ read a h5 file and Generate a mini batch of batchsize for 'epoch'
    epochs, small is to sample 100 datapoints for testin"""
    np.random.seed(seed)
    D = h5.File(fname)
    examples = D['x'].shape[0]
    if small: examples = min([examples, 100])
    index = range(examples)
    np.random.shuffle(index)
    assert epoch > 0 and batchsize > 0
    while epoch:
        for i in xrange(0, examples, batchsize):
            minibatch_ind = index[i:i + batchsize]
            minibatch_ind.sort()
            x, y = D['x'][minibatch_ind], D['y'][minibatch_ind]
            mask = (x > 0).astype(np.float32)
            yield x, y, mask
        epoch -= 1
    return


def create_glove_embeddings(vocabpath, glove):
    with open(vocabpath + '/vocab.json') as f:
        data = json.load(f)
    embed_dim = len(open(glove).readline().split(' ')) - 1
    WE = [np.random.normal(0, 0.1, embed_dim).tolist() for _ in data]
    WE[0] = [0.] * embed_dim
    cnt = 0
    with open(glove) as f:
        for line in f:
            line = line.split(' ')
            if line[0] in data:
                WE[data[line[0]]] = [float(e) for e in line[1:]]
                cnt += 1

        np.save(vocabpath + '/embed.npy', np.array(WE))
        print np.array(WE).shape
        print 'Unable to find embeddings for %d tokens = %.3f' % (cnt, cnt * 100. / len(WE))
    return


def preprocess(doc, slenMax, wlenMax):
    """ Take in a string and return a list of list of token-strings of size slenMax X wlenMax
    """

    sent = doc.split('.')
    slen = min([slenMax, len(sent)])
    doc = []
    # print slenMax, wlenMax
    for line in sent[:slen]:
        word = line.split(' ')
        wlen = min([wlenMax, len(word)])

        word = word[:wlen] + ['<STOP>'] * (wlenMax - wlen)
        doc.append(word)
    if slenMax > slen:
        doc.extend([['<STOP>'] * wlenMax for _ in range((slenMax - slen))])
    # print [len(each) for each in doc ]
    return doc


class yelp():
    def __init__(self):
        self.vocab = {}
        self.rev = {}
        fname = 'yelp-2013/vocab.json'
        if os.path.isfile(fname):
            with open(fname) as f:
                self.vocab = json.load(f)
            for each in self.vocab: self.rev[self.vocab[each]] = each

    def read_dataset(self, matrix):
        slenMax, wlenMax = matrix.shape

        for r in range(slenMax):
            res = []
            for c in range(wlenMax):
                if self.rev[matrix[r][c]] != '<STOP>':
                    res.append(self.rev[matrix[r][c]])
            if res: print ' '.join(res)
        return

    def create_datasets(self, slenMax=20, wlenMax=30, small=False):
        def _create(type='train', slenMax=30, wlenMax=30, small=False):
            F = h5.File('yelp-2013/%s.h5' % type)
            cnt, minimum_examples = 0, 100000
            fname = 'yelp-2013-%s.txt.ss' % type
            with open('yelp-2013/%s' % fname) as f:
                for line in f: cnt += 1
            if small: cnt = min([cnt, minimum_examples + 2])
            F.create_dataset('x', [cnt, slenMax, wlenMax], dtype='int32')
            F.create_dataset('y', [cnt, 5], dtype='int32')
            ind, start = 0, time.time()

            with open('yelp-2013/%s' % fname) as f:
                for line in f:
                    line = line.split('\t')
                    lbl, doc = line[-3], line[-1]
                    docMatrix = []
                    for line in preprocess(doc, slenMax, wlenMax):
                        sentence = []
                        for word in line:
                            if type == 'train':
                                wordid = self.vocab[word] = self.vocab.get(word, len(self.vocab))
                            else:
                                wordid = self.vocab.get(word, 1)
                            assert wordid < len(self.vocab)
                            sentence.append(wordid)
                        docMatrix.append(sentence)
                    docMatrix = np.array(docMatrix)
                    assert docMatrix.shape == (slenMax, wlenMax)
                    F['x'][ind] = docMatrix
                    target = [0] * 5
                    target[int(lbl) - 1] = 1
                    F['y'][ind] = target
                    ind += 1
                    if ind > minimum_examples and small: return
                    if ind > 4096: F.flush()
                    if ind % 10000 == 0:  print 'Done with %d examples, in %.3fs' % (ind, time.time() - start)
                return

        self.vocab = {'<STOP>': 0, '<sssss>': 1}
        _create('train', slenMax, wlenMax, small)
        _create('dev', slenMax, wlenMax, small)
        _create('test', slenMax, wlenMax, small)
        with open('yelp-2013/vocab.json', 'w') as f:
            json.dump(self.vocab, f)
        return


if __name__ == '__main__':
    import time

    start = time.time()
    # YELP = yelp()
    # YELP.create_datasets(20,30,False )
    # F = h5.File('yelp-2013/train.h5','r')
    # for x,y,m in generate('yelp-2013/train.h5',1, 24):
    #    YELP.read_dataset(x[0])
    #    raw_input(y[0])
    create_glove_embeddings('yelp-2013', 'glove/glove.6B.100d.txt')

    print 'Time takens %.3f' % (time.time() - start)

