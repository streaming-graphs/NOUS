##
# Utility functions for Loading Data
##

from numpy import *
import numpy as np
from collections import defaultdict

class Vocab(object):
  def __init__(self):
    self.word_to_index = {}
    self.index_to_word = {}
    self.word_freq = defaultdict(int)
    self.total_words = 0
    self.unknown = '<unk>'
    self.add_word(self.unknown, count=0)

  def add_word(self, word, count=1):
    if word not in self.word_to_index:
      index = len(self.word_to_index)
      self.word_to_index[word] = index
      self.index_to_word[index] = word
    self.word_freq[word] += count

  def construct(self, words):
    for word in words:
      self.add_word(word)
    self.total_words = float(sum(self.word_freq.values()))
    print '{} total words with {} uniques'.format(self.total_words, len(self.word_freq))

  def encode(self, word):
    if word not in self.word_to_index:
      word = self.unknown
    return self.word_to_index[word]

  def decode(self, index):
    return self.index_to_word[index]

  def __len__(self):
    return len(self.word_freq)

def invert_dict(d):
    return {v:k for k,v in d.iteritems()}

def load_wv(vocabfile, wvfile):
    wv = loadtxt(wvfile, dtype=float)
    with open(vocabfile) as fd:
        words = [line.strip() for line in fd]
    num_to_word = dict(enumerate(words))
    word_to_num = invert_dict(num_to_word)
    return wv, word_to_num, num_to_word
    
def load_vertex(vertexfile):
    with open(vertexfile) as fd:
        words = [line.strip() for line in fd]
    num_to_word = dict(enumerate(words))
    word_to_num = invert_dict(num_to_word)
    return word_to_num, num_to_word

def calculate_perplexity(log_probs):
  # https://web.stanford.edu/class/cs124/lec/languagemodeling.pdf
  perp = 0
  for p in log_probs:
    perp += -p
  return np.exp(perp / len(log_probs))

def get_pathsearch_dataset(dataset):
  fn = 'data/{}.dummy.dat'
  for line in open(fn.format(dataset)):
	  count = 0
	  word_array = []
	  for word in line.strip().split(","):
		  word_array.append(int(word))
	  word_len = len(word_array)
	  x = word_array[:-1]
	  y = word_array[1:]
	  yield (x,y)
	  
def get_pathsearch_dataset_batch(dataset, batch_size):
    fn = 'data/{}.dummy.dat'
    data_array = []   
    label_array = []
    dest_array = []
    for line in open(fn.format(dataset)):
        count = 0        
        word_array = []
        for word in line.strip().split(","):
            word_array.append(int(word))
            
        data_array.append(word_array[:-1])
        label_array.append(word_array[1:])
        dest_array.append(word_array[-1])
    data_len = len(data_array)    
    epoch_size = data_len//batch_size
    for i in range(epoch_size):
        x = data_array[i*batch_size:(i+1)*batch_size]    
        y = label_array[i*batch_size:(i+1)*batch_size]
        dest_id = dest_array[i*batch_size:(i+1)*batch_size]
        yield (x, y, dest_id)       
        
def get_pathsearch_dataset_batch_varlen(dataset, batch_size):
	fn = 'data/{}.dummy.dat'
	data_array = []   
	label_array = []
	dest_array = []
	seqL_array = []
	
	len_max = 2
	max_steps = 5
	for line in open(fn.format(dataset)):
		count = 0        
		word_array = []
		for word in line.strip().split(","):
			word_array.append(int(word)) 		
		data_len = len(word_array)
		seqL_array.append(data_len-1)  
		dest_array.append(word_array[-1])
		if data_len < max_steps:
			for ii in xrange(max_steps-data_len):
				word_array.append(int(0.0))
		data_array.append(word_array[:-1])
		label_array.append(word_array[1:])	
		
	#sen_size = data_len//len_max
	#if data_len == 5:
		#sen_size = sen_size+1    
		#for i in range(sen_size):        
			#data_array.append(word_array[i:(i+len_max)])    
			#label_array.append(word_array[i+1:(i+len_max+1)])
			#dest_array.append(word_array[-1])	
			
	data_len = len(data_array)    
	epoch_size = data_len//batch_size	
	for i in range(epoch_size):
		x = data_array[i*batch_size:(i+1)*batch_size]    
		y = label_array[i*batch_size:(i+1)*batch_size]
		dest_id = dest_array[i*batch_size:(i+1)*batch_size]
		seqL = seqL_array[i*batch_size:(i+1)*batch_size]
		yield (x,y, dest_id, seqL)
        
def get_nbrs_info(fname):
	count = 0 
	nbrs_array = []
	nbrs_dict = {'-100':'-100'}
	for line in open(fname):    
		word_array = []
		for word in line.strip().split(","):
			word_array.append(float(word)) 
		nbrs_array.append(word_array)
		key = float(count)
		count += 1
		items = word_array
		dict_temp = {key:items}
		nbrs_dict.update(dict_temp)
	return nbrs_dict	
	
def get_pathsearch_dataset_batch_G(dataset, batch_size):
    fn = 'data/{}.dummy.dat'
    data_array = []   
    label_array = []
    dest_array = []
    count = 0
    for line in open(fn.format(dataset)):
        word_array = []
        vec = line.strip()[1:-1].split(", ")
        word_array = [int(i[1:-1]) for i in vec]              
        data_array.append(word_array[:-1])
        label_array.append(word_array[1:])
        dest_array.append(word_array[-1])
    data_len = len(data_array)    
    epoch_size = data_len//batch_size
    for i in range(epoch_size):
        x = data_array[i*batch_size:(i+1)*batch_size]    
        y = label_array[i*batch_size:(i+1)*batch_size]
        dest_id = dest_array[i*batch_size:(i+1)*batch_size]
        yield (x, y, dest_id)       
        
def get_pathsearch_dataset_batch_varlen_G(dataset, batch_size):
    fn = 'data/{}.dummy.dat'
    data_array = []   
    label_array = []
    dest_array = []
    len_max = 2
    count = 0
    for line in open(fn.format(dataset)):        
        word_array = []        
        vec = line.strip()[1:-1].split(", ")
        word_array = [int(i[1:-1]) for i in vec]   
         
        data_len = len(word_array)
        sen_size = data_len//len_max
        if data_len == 5:
            sen_size = sen_size+1    
        for i in range(sen_size):        
            data_array.append(word_array[i:(i+len_max)])    
            label_array.append(word_array[i+1:(i+len_max+1)])
            dest_array.append(word_array[-1])        
    
    data_len = len(data_array)    
    epoch_size = data_len//batch_size
    for i in range(epoch_size):
        x = data_array[i*batch_size:(i+1)*batch_size]    
        y = label_array[i*batch_size:(i+1)*batch_size]
        dest_id = dest_array[i*batch_size:(i+1)*batch_size]
        yield (x,y, dest_id)
        
def get_nbrs_info_G(fname):			
	count = 0 
	nbrs_dict = {'-100':'-100'}
	for line in open(fname):    
		word_array = []
		vec = line.strip()[:-2].split(";;")
		word_array = [int(i) for i in vec]		
		key = word_array[0]
		items = word_array[1:]
		dict_temp = {key:items}
		nbrs_dict.update(dict_temp)		
	return nbrs_dict 
	
def sample(a, temperature=1.0):
    # helper function to sample an index from a probability array
    # from https://github.com/fchollet/keras/blob/master/examples/lstm_text_generation.py
    a = np.log(a) / temperature
    a = np.exp(a) / np.sum(np.exp(a))
    return np.argmax(np.random.multinomial(1, a, 1))
