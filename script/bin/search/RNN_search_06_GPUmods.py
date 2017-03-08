# This code is attempting to learn an RNN model for sequence generation on knowledge based graphs
# Version v01: Implementation on stochastic gradient descent wherein gradient updates are made w.r.t a single example
# Version v02: Implementation of batch gradient descent algorithm
# Version v03: Implementation of sequence generation with fixed length of inputs in the train, valid, and test datasets
# Version v04: Implementation of sequence generation taking into account the neighbor nodes information
# Version v05: Implementation of sequence generation with variable length of inputs in the train, valid, and test datasets

# This version deals with the real-world Graph dataset along with wordvectors files in binary format

import numpy as np
import tensorflow as tf
import pandas as pd

import getpass
import sys
import time

import load_data as l_data 
from copy import deepcopy
from tensorflow.python.ops.seq2seq import sequence_loss
from numpy import *
from Xavier_initialization import xavier_weight_init
from matrix_mul import block_mat_mul


class Config():
	""" Holds model hyperparameters and data
	
	Model objects are passed a Config object at instantiation
	"""
	batch_size = 10
	embed_size = 300
	vocab_size = 3330070
	hidden_size = 100
	num_steps = 4
	max_steps = 5
	max_epochs = 1 #16
	early_stopping = 2
	dropout = 0.9
	lr = 0.001	
	num_mat_blocks = 29
	
class RNNSearch():
	
	def load_pathembeddings(self):
		fname = 'data/vertexWord2Vec/vertexWord2VecNormalized.hd5'
		store = pd.HDFStore(fname, 'r')
		df = store['vertexWord2VecNormalized']
		self.embed_matrix = df.values
		#temp = pd.read_hdf(fname)
		#self.embed_matrix = temp.values
		df = 0
		store.close()			
		print np.shape(self.embed_matrix)
		
		#fname = 'data/vertexWord2Vec/vertexWord2VecNormalized.txt'
		#word_array = []
		#for line in open(fname):    
			#vec = line.strip()[1:-1].split(", ")
			#cleanVec = [float(i[1:-1]) for i in vec]
			#word_array.append(cleanVec)
		#self.embed_matrix = word_array
		#print np.shape(self.embed_matrix)
	
	def add_placeholders(self):
		self.input_placeholder = tf.placeholder(tf.int32, shape=[None, self.config.num_steps], name = 'Input')
		self.labels_placeholder = tf.placeholder(tf.int32, shape=[None, self.config.num_steps], name = 'Target')	
		self.dest_vertex_ID = tf.placeholder(tf.int32, shape=[None, 1], name = 'Destination_ID')					
		
	def add_embedding(self):			
		self.L = tf.placeholder(tf.float32, shape=[self.config.vocab_size, self.config.embed_size], name = 'Node_Embeddings')	
		window = tf.nn.embedding_lookup(self.L, self.input_placeholder)	
		inputs = [tf.squeeze(x, [1]) for x in tf.split(1, self.config.num_steps, window)]
		return inputs
		
	def add_dest_embedding(self):
		dest_window = tf.nn.embedding_lookup(self.L, self.dest_vertex_ID)	
		dest_embedding = tf.squeeze(dest_window, [1])
		return dest_embedding		
	
	def add_projection(self, rnn_outputs):
		with tf.variable_scope("Softmax", reuse= None):
			U = tf.get_variable("U", [self.config.hidden_size,self.config.vocab_size], initializer=xavier_weight_init())  
			b_2 = tf.get_variable("b_2", [self.config.vocab_size, ], initializer=xavier_weight_init())
			z = []
			num_blocksize = self.config.vocab_size/self.config.num_mat_blocks
			for elems in rnn_outputs:	
				temp = block_mat_mul(U, elems, num_blocksize, self.config.num_mat_blocks, self.config.batch_size)
				print np.shape(temp)
				temp = tf.pack(temp)
				print np.shape(temp)
				temp = tf.reshape(temp,(self.config.batch_size,self.config.num_mat_blocks*num_blocksize))
				print np.shape(temp)
				z.append(temp+b_2)				 
			#z = [tf.matmul(elems, U) + b_2 for elems in rnn_outputs]	
		outputs = z
		return outputs		
		
	def add_loss_op(self, output):
		"""Adds loss ops to the computational graph."""		
		all_ones = [tf.ones([self.config.batch_size * self.config.num_steps])]
		cross_entropy = sequence_loss([output], [tf.reshape(self.labels_placeholder, [-1])], all_ones, self.config.vocab_size)        
		tf.add_to_collection('total_loss', cross_entropy)
		loss = tf.add_n(tf.get_collection('total_loss'))		
		return loss
		
	def add_training_op(self, loss):
		"""Sets up the training Ops."""
		optimizer = tf.train.AdamOptimizer(self.config.lr)
		global_step = tf.Variable(0, name='global_step', trainable=False)
		train_op = optimizer.minimize(loss, global_step=global_step)
		return train_op
		
	def __init__(self, config):
		self.config = config
		self.load_pathembeddings()
		self.add_placeholders()
		self.inputs = self.add_embedding()
		self.dest_embedding = self.add_dest_embedding()
		self.rnn_outputs = self.add_model(self.inputs, self.dest_embedding)
		self.outputs = self.add_projection(self.rnn_outputs)		
		self.predictions = [tf.nn.softmax(tf.cast(o, 'float32')) for o in self.outputs]
		output = tf.reshape(tf.concat(1, self.outputs), [-1, self.config.vocab_size])
		self.calculate_loss = self.add_loss_op(output)
		self.train_step = self.add_training_op(self.calculate_loss)
		
	def add_model(self, inputs, dest_embedding):
		"""Creates the RNN search model."""
		with tf.variable_scope("RNN", reuse= None) as scope:
			self.initial_state = tf.zeros([self.config.batch_size, self.config.hidden_size])
			state = self.initial_state
			H = tf.get_variable("H", [self.config.hidden_size,self.config.hidden_size], initializer=xavier_weight_init())
			I = tf.get_variable("I", [self.config.embed_size,self.config.hidden_size], initializer=xavier_weight_init())
			b_1 = tf.get_variable("b_1", [self.config.hidden_size,], initializer=xavier_weight_init())	
			rnn_outputs = []
						
			count = 0															
			for elems in inputs:
				dest_vec = tf.cast(dest_embedding,'float32')
				if count == 0:						
					elems = tf.reshape(tf.cast(elems,'float32'),[self.config.batch_size, self.config.embed_size])
					state = tf.sigmoid(tf.matmul(state,H) + tf.matmul(elems,I)+ b_1 + tf.matmul(dest_vec,I))									
				else:
					elems = tf.reshape(tf.cast(elems,'float32'),[self.config.batch_size, self.config.embed_size])	
					#state = tf.sigmoid(tf.matmul(state,H) + tf.matmul(elems,I)+ b_1)	
					state = tf.sigmoid(tf.matmul(state,H) + tf.matmul(elems,I)+ b_1 + tf.matmul(dest_vec,I))					
				scope.reuse_variables()
				rnn_outputs.append(state)
				count += 1				
			self.final_state = rnn_outputs[-1]
		return rnn_outputs
		
	def run_epoch(self, session, dataset, train_op=None, verbose=10):
		config = self.config
		dp = config.dropout
		if not train_op:
			train_op = tf.no_op()
			dp = 1
		total_steps = sum(1 for x in enumerate(l_data.get_pathsearch_dataset_batch_G(dataset, self.config.batch_size)))
		print total_steps
		total_loss = []
		state = self.initial_state.eval()	
		word2vec = self.embed_matrix	
		for step, (x, y, dest_ID) in enumerate(l_data.get_pathsearch_dataset_batch_G(dataset, self.config.batch_size)):
			dummy, self.config.num_steps = np.shape(x)	
			print x, y, step, self.config.num_steps, dest_ID	
			x = np.reshape(x,[self.config.batch_size, self.config.num_steps])
			y = np.reshape(y,[self.config.batch_size, self.config.num_steps])
			dest_ID = np.reshape(dest_ID,[self.config.batch_size, 1])
			feed = {self.input_placeholder: x,
					self.labels_placeholder: y,
					self.dest_vertex_ID: dest_ID,
					self.L: word2vec,
					self.initial_state: state}
			loss, state, _ = session.run([self.calculate_loss, self.final_state, train_op], feed_dict=feed)
			total_loss.append(loss)
			if verbose and step % verbose == 0:
				sys.stdout.write('\r{} / {} : pp = {}'.format(
				step, total_steps, np.exp(np.mean(total_loss))))
				sys.stdout.flush()
		if verbose:
			sys.stdout.write('\r')
		return np.exp(np.mean(total_loss))

def generate_seq(session, model, config, start_ID, dest_ID, nbrs_dict, temp):
	
	state = model.initial_state.eval() 
	word2vec = model.embed_matrix
	
	start_ID = np.reshape(start_ID , [1,1])
	dest_ID = np.reshape(dest_ID , [1,1])
	tokens = np.zeros([1,config.max_steps+1])
	tokens[0,0] = start_ID 
	match_count = 0
	for i in xrange(config.max_steps):	
		feed = {model.input_placeholder: np.reshape(tokens[0,i],[1,1]),			
			model.dest_vertex_ID: dest_ID,
			model.L: word2vec,
			model.initial_state: state}
		state, y_pred = session.run([model.final_state, model.predictions[-1]], feed_dict=feed)  
		
		idx_nbrs = nbrs_dict[tokens[0,i]]		
		y_pred_reduced = []
		for ii in xrange(len(idx_nbrs)):
			y_pred_reduced.append(y_pred[0,idx_nbrs[ii]])		
		
		id_pred = argsort(y_pred_reduced)		
		next_seq_idx = idx_nbrs[id_pred[-1]] 
		count = 1
		while next_seq_idx == tokens[0,i] or next_seq_idx in tokens[0,:]:
			count += 1
			if count > len(idx_nbrs):
				break
			next_seq_idx = idx_nbrs[id_pred[-count]] 
		tokens[0,i+1] =  next_seq_idx  						
		if next_seq_idx == dest_ID:
			match_count += 1
			break
	output = [tokens]
	return (output, i+1, match_count)
	
def train_RNNSearch():
	config = Config()
	
	# We create the training model 
	with tf.variable_scope('RNNSearch') as scope:
		model = RNNSearch(config)
			
	init = tf.initialize_all_variables()
	saver = tf.train.Saver()	
	
	gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction = 0.9)
	with tf.Session(config=tf.ConfigProto(gpu_options = gpu_options)) as session:
		best_val_pp = float('inf')
		best_val_epoch = 0
		session.run(init)		
		for epoch in xrange(config.max_epochs):
			print 'Epoch {}'.format(epoch)
			start = time.time()
			###		
			train_pp = model.run_epoch(session, dataset = 'graph_train_path',train_op=model.train_step) 	
			valid_pp = model.run_epoch(session, dataset = 'graph_valid_path')  
			
			print 'Training perplexity: {}'.format(train_pp)
			print 'Validation perplexity: {}'.format(valid_pp)
			if valid_pp < best_val_pp:
				best_val_pp = valid_pp
				best_val_epoch = epoch
				saver.save(session, './pathsearch_rnnlm_06.weights')
			if epoch - best_val_epoch > config.early_stopping:
				break
			print 'Total time: {}'.format(time.time() - start)
		
		saver.restore(session, 'pathsearch_rnnlm_06.weights')
		print "Hello there! Trained Network Weights have been loaded..."
		
		test_pp = model.run_epoch(session, dataset = 'graph_test_path')
		print '=-=' * 5
		print 'Test perplexity: {}'.format(test_pp)
		print '=-=' * 5  						

def test_RNNSearch():
	config = Config()
	gen_config = deepcopy(config)
	gen_config.batch_size = gen_config.num_steps = 1
			
	# We create the generative model
	with tf.variable_scope('RNNSearch') as scope:
		model = RNNSearch(config)
		scope.reuse_variables()			
		gen_model = RNNSearch(gen_config)
	
	init = tf.initialize_all_variables()
	saver = tf.train.Saver()	
	
	with tf.Session() as session:
		best_val_pp = float('inf')
		best_val_epoch = 0
		session.run(init)			
		
		saver.restore(session, 'pathsearch_rnnlm_06.weights')
		print "Hello there! Trained Network Weights have been loaded..."		
		
		nbrs_fname = 'data/graph_nbrs_info.dat'
		vertex_labels_fname = 'data/vertexNames.txt'		
		(vertex_ID, vertex_label) = l_data.load_vertex(vertex_labels_fname)
		nbrs_dict = l_data.get_nbrs_info_G(nbrs_fname)		
		print "Hello Again! Neighborhood Information has been loaded..."
		
		fname = 'data/TestFile.txt'
		test_start_ID = []
		test_dest_ID = []
		for line in open(fname):    
			vec = line.strip().split(";;")
			test_start_ID.append(vertex_ID[vec[0]])
			test_dest_ID.append(vertex_ID[vec[1]])
	
		num_tests = len(test_start_ID)
		num_success = 0			
		for i in range(num_tests):
			start = time.time()
			start_ID = test_start_ID[i]
			dest_ID = test_dest_ID[i]
			(sequence_out, len_seq, match_count) = generate_seq(session, gen_model, gen_config, start_ID, dest_ID, nbrs_dict, temp = 1.0)  
			num_success += match_count
			#print 'Generated Sequence is: {}'.format(sequence_out)
			[m1, m2, m3] = np.shape(sequence_out)
			gen_idx = np.reshape(sequence_out,(m3,))
			sequence_out_labels = []
			count = 0
			for ii in gen_idx:
				sequence_out_labels.append(vertex_label[int(ii)])
				if count >= len_seq:
					break 
				count += 1 
			print '-----xxxxxxxxxxxxxxx-----------'			
			print 'Source : {} ----- Destination : {}'.format(vertex_label[start_ID], vertex_label[dest_ID])
			print 'Generated Sequence from RNN Model is: {}'.format(sequence_out_labels)
			print 'Does Source-Destination Match Happens: 1- Yes; 0- No: --->{}'.format(match_count)			
			print 'Total time for this Sequence Generation: {}'.format(time.time() - start)
			print '-----xxxxxxxxxxxxxxx-----------'
		print 'Test Accuracy based on Number of Successful Path Search within {} steps is: {} %'.format(gen_config.max_steps, 100*num_success/num_tests)
      		
if __name__ == "__main__":
    train_RNNSearch()
    test_RNNSearch()
		
		
		
		
	
		
	

