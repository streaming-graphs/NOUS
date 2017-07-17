# Generating word embeddings
import gensim
import os

class MySentences(object):
    def __init__(self, dirname):
        self.dirname = dirname
 
    def __iter__(self):
        for fname in os.listdir(self.dirname):
			if fname.endswith('.txt'):
				#print fname
				for line in open(os.path.join(self.dirname, fname)):
					yield line.split()
 
sentences = MySentences('SemEval2010_task8_all_data/words/') # a memory-friendly iterator
model = gensim.models.Word2Vec(sentences, min_count=1, size=50) # min_count=5 (appear num), size=50 (vector dim)
# save model
model.save('word_embedding_model')
# save word vectors
model.wv.save_word2vec_format('word_embedding.txt', binary=False) 


