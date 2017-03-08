# This code is attempting to learn a basic RNN/LSTM model for finding the vector representation of the Graph nodes (yago dataset)
# Implementation of basic RNN cell for running in gpu node in PUMA; 
# Variable length sequence is handled automatically

import numpy as np
import tensorflow as tf
import pickle as pk
from scipy.cluster.vq import kmeans,vq

import getpass
import sys
import time

import load_data_yago_Graph2Vec as yago_data 
from copy import deepcopy
from tensorflow.python.ops.seq2seq import sequence_loss
from numpy import *
from Xavier_initialization import xavier_weight_init

class Config():
	""" Holds model hyperparameters and data
	
	Model objects are passed a Config object at instantiation
	"""
	batch_size = 50
	embed_size = 5
	hidden_size = 10
	num_steps = 2
	max_steps = num_steps+1
	num_layers = 2	# Changes for Multilayer LSTM cell		
	max_epochs = 1
	early_stopping = 2
	dropout = 0.9
	lr = 0.01
	
class RNNSearch():
	
	def load_data(self):
		Node_IDs_fname ='/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/step7/yago2data/vertexDictionary.out'
		[self.encode_word, self.decode_word] = yago_data.load_vertex(Node_IDs_fname)		 
		self.vocab_size = len(self.encode_word)
	
	def add_placeholders(self):
		self.input_placeholder = tf.placeholder(tf.int32, shape=[None, self.config.num_steps], name = 'Input')
		self.labels_placeholder = tf.placeholder(tf.int32, shape=[None, self.config.num_steps], name = 'Target')	
		self.dest_vertex_ID = tf.placeholder(tf.int32, shape=[None, 1], name = 'Destination_ID')	
		self.seq_len = tf.placeholder(tf.int32, shape=[None,1], name = 'SeqLength')				
		
	def add_embedding(self):
		self.L = tf.get_variable('Embedding', [self.vocab_size, self.config.embed_size], initializer=xavier_weight_init())		
		window = tf.nn.embedding_lookup(self.L, self.input_placeholder)	
		inputs = [tf.squeeze(x, [1]) for x in tf.split(1, self.config.num_steps, window)]
		return inputs
	
	def add_projection(self, rnn_outputs):
		with tf.variable_scope("Softmax", reuse= None):
			U = tf.get_variable("U", [self.config.hidden_size,self.vocab_size], initializer=xavier_weight_init())  
			b_2 = tf.get_variable("b_2", [self.vocab_size, ], initializer=xavier_weight_init())				
			z = [tf.matmul(elems, U) + b_2 for elems in rnn_outputs]
		outputs = z
		return outputs		
		
	def add_loss_op(self, output):
		"""Adds loss ops to the computational graph."""		
		all_ones = [tf.ones([self.config.batch_size * self.config.num_steps])]
		cross_entropy = sequence_loss([output], [tf.reshape(self.labels_placeholder, [-1])], all_ones, self.vocab_size)        
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
		self.load_data()
		self.add_placeholders()
		self.inputs = self.add_embedding()
		self.rnn_outputs = self.add_model(self.inputs, self.seq_len)
		self.outputs = self.add_projection(self.rnn_outputs)		
		self.predictions = [tf.nn.softmax(tf.cast(o, 'float32')) for o in self.outputs]
		output = tf.reshape(tf.concat(1, self.outputs), [-1, self.vocab_size])
		self.calculate_loss = self.add_loss_op(output)
		self.train_step = self.add_training_op(self.calculate_loss)
		
	def add_model(self, inputs, seq_len):
		"""Creates the LSTM model."""
		#with tf.variable_scope("RNN", reuse= None) as scope:
			#lstm_cell = tf.nn.rnn_cell.BasicLSTMCell(self.config.hidden_size, input_size = None, forget_bias=0.0, state_is_tuple=True)
			#RNN_cell = lstm_cell
			##RNN_cell = tf.nn.rnn_cell.MultiRNNCell([lstm_cell] * self.config.num_layers, state_is_tuple=True)
			
			#self.initial_state = RNN_cell.zero_state(self.config.batch_size, dtype = tf.float32)
			#state = self.initial_state
			
			#rnn_outputs = []
			#temp = tf.reshape(seq_len,[-1])
			#inputs = tf.reshape(inputs, [self.config.num_steps, self.config.batch_size, self.config.embed_size] )
			#(cell_output, state) = tf.nn.dynamic_rnn(RNN_cell, inputs, initial_state=state, sequence_length=temp, time_major = True)
			#cell_output = [tf.squeeze(x, [0]) for x in tf.split(0, self.config.num_steps, cell_output)]
			#rnn_outputs = cell_output	
			#self.final_state = state		
		#return rnn_outputs
		
		"""Creates the RNN model."""
		with tf.variable_scope("RNN") as scope: 
			RNN_cell_basic = tf.nn.rnn_cell.BasicRNNCell(self.config.hidden_size, input_size = None)
			RNN_cell = RNN_cell_basic
			
			self.initial_state = RNN_cell.zero_state(self.config.batch_size, dtype = tf.float32)
			state = self.initial_state
			rnn_outputs = []				
			
			for elems in inputs:
				(cell_output, state) = RNN_cell(elems, state)	
				scope.reuse_variables()
				rnn_outputs.append(cell_output)			
			self.final_state = state
		return rnn_outputs
		
	def run_epoch(self, session, dataset, train_op=None, verbose=10):
		config = self.config
		dp = config.dropout
		if not train_op:
			train_op = tf.no_op()
			dp = 1
		total_steps = sum(1 for x in enumerate(yago_data.get_pathsearch_dataset_batch_varlen(dataset, self.config.batch_size, self.config.max_steps)))
		print total_steps
		total_loss = []
		state = session.run(self.initial_state) 		
		for step, (x, y, dest_ID, seqL) in enumerate(yago_data.get_pathsearch_dataset_batch_varlen(dataset, self.config.batch_size, self.config.max_steps)):
			dummy, self.config.num_steps = np.shape(x)	
			#print x, y, step, self.config.num_steps, dest_ID, seqL	
			x = np.reshape(x,[self.config.batch_size, self.config.num_steps])
			y = np.reshape(y,[self.config.batch_size, self.config.num_steps])
			dest_ID = np.reshape(dest_ID,[self.config.batch_size, 1])
			seqL = np.reshape(seqL,[self.config.batch_size, 1])
			feed = {self.input_placeholder: x,
					self.labels_placeholder: y,
					self.seq_len: seqL,
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

def test_RNNSearch():
	config = Config()
	gen_config = deepcopy(config)
	gen_config.batch_size = gen_config.num_steps = 1
		
	# We create the training model and generative model
	with tf.variable_scope('RNNSearch') as scope:
		model = RNNSearch(config)
		# This instructs gen_model to reuse the same variables as the model above		
		scope.reuse_variables()    
		gen_model = RNNSearch(gen_config)
		
	init = tf.initialize_all_variables()
	saver = tf.train.Saver()
	
	with tf.Session() as session:
		best_val_pp = float('inf')
		best_val_epoch = 0
		session.run(init)
		#for epoch in xrange(config.max_epochs):
		#	print 'Epoch {}'.format(epoch)
		#	start = time.time()
		#	###
		#	train_pp = model.run_epoch(session, dataset = 'Combined_examples',train_op=model.train_step) 					
		#	print 'Training perplexity: {}'.format(train_pp)
		#	if train_pp < best_val_pp:
		#		best_val_pp = train_pp
		#		best_val_epoch = epoch
		#		saver.save(session, './yagoGraph2VecSearch_RNN.weights')
		#	if epoch - best_val_epoch > config.early_stopping:
		#		break
		#	print 'Total time: {}'.format(time.time() - start)
		print("Starting work\n")
        datadir="/people/d3x771/projects/knowledgeGraph/PathSearchBenchmark/data/yago2/"
        ckpt=tf.train.get_checkpoint_state(datadir)
        if ckpt and ckpt.model_checkpoint_path:
            print("loading model\n")
            saver.restore(session, ckpt.model_checkpoint_path)
            print("restored session\n")
            temp = session.run(model.L)
            print np.shape(temp)
            print temp[1,:]
		
		    # Implementing k-means clustering algorithm with in-built scipy function
		    # clustering with k clusters
            print("Starting clustering\n")
            num_clusters = 500
            features_vec = temp
            cluster_centroid,_ = kmeans(features_vec,num_clusters)
            # assign each sample to a cluster
            print("Clustering completed, addiogning each adata poin to a cluster\n")
            idx_array,_ = vq(features_vec,cluster_centroid)
        
            # Writing the cluster-ids and cluster centorid vectors in output files
            print("Saving Data \n")
            f = open("yagoGraph2VecSearch_idx_cluster_500.txt", "w")
            pk.dump(idx_array, f)
            f.close()
        
            f = open("yagoGraph2VecSearch_Cluster_Centroids_500.txt", "w")
            pk.dump(cluster_centroid, f)
            f.close()
					
if __name__ == "__main__":
    test_RNNSearch()
	
		
		
		
		
	
		
	

