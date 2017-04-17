# implement social based BPR model and make the original code generic (training/test partition)
# paper resource: http://cseweb.ucsd.edu/~jmcauley/pdfs/cikm14.pdf
# for evaluation, use HR, ARHR, and AUC
# Author: Baichuan Zhang

import os;
import sys;
import math;
import numpy as np;
from scipy import linalg;
from operator import itemgetter;
import time;
import random;
import networkx as nx;

# parse training file (Matlab index style)
# user and item index starting from 1

# build data strucutre
def Parse(Trainfile, Testfile, subject_graph_file, subj_dict_file):
	user_unpuritem_dict = {};
	user_puritem_dict = {};
	user_index = -1;
	# training file parse
	for line in Trainfile:
		user_index = user_index + 1;
		linetuple = line.strip().split(" ");
		if user_index == 0:
			num_user = int(linetuple[0]);
			num_item = int(linetuple[1]);
			num_nnz = int(linetuple[2]);
			total_item_list = range(1, num_item+1);
			user_list = range(1, num_user+1);
		else:
			pur_item_list = [];
			for pos in range(0,len(linetuple)):
				item_index = int(linetuple[pos]);
				pur_item_list.append(item_index);
					
			unpur_item_list = list(set(total_item_list)-set(pur_item_list));			
			user_unpuritem_dict[user_index] = unpur_item_list;
			user_puritem_dict[user_index] = pur_item_list;
	# test file parse
	recom_item_test = [];
	linecount = 0;
	for line in Testfile:
		linecount = linecount + 1;
		line = line.strip();
		if linecount == 1:
			linetuple = line.strip().split(" ");
			num_test = int(linetuple[2]);
		elif line != "":
			linetuple = line.split(" ");
			recom_item_test.append(map(int, linetuple));
		else:
			recom_item_test.append([]);

	# generate the subject entity based graph as side information for prediction
#	subject_graph_file = open("imports_graph.tsv", "r");
#	subj_dict_file = open("subj_entity_dict.txt", "r");
	subj_index_dict = {};
	for line in subj_dict_file:
		linetuple = line.strip().split(" ");
		subj_index_dict[linetuple[0]] = int(linetuple[1]);
#	print str(subj_index_dict);

	G = nx.Graph();
	for line in subject_graph_file:
		linetuple = line.strip().split("\t");
		e_1 = subj_index_dict[linetuple[0][linetuple[0].find("<")+1:linetuple[0].rfind(">")].strip()];
		e_2 = subj_index_dict[linetuple[1][linetuple[1].find("<")+1:linetuple[1].rfind(">")].strip()];
#		print str(e_1);
#		print str(e_2);
		G.add_edge(e_1, e_2, weight = 1);
#	print "number of nodes in graph = " + str(G.number_of_nodes());	
#	print str(G.nodes());

	# build the adjacnecy list of each node as dict
	G_neighbor_dict = {};
	nodelist = G.nodes();
	for node_ in range(0,len(nodelist)):
		G_neighbor_dict[int(nodelist[node_])] = G.neighbors(int(nodelist[node_]));
	
	return [num_user, num_item, num_nnz, user_list, user_puritem_dict, user_unpuritem_dict, recom_item_test, num_test, G_neighbor_dict];


# compute HR, ARHR, AUC for evaluation
def Evaluate(P, Q, B, user_unpuritem_dict, recom_item_test, N, num_test):
	
	two_dimen_sorted_list = [];
	for k, v in user_unpuritem_dict.items():
		tuple_list = [];
		user_index = int(k);
		unpur_item_list = v;
		for pos in range(0,len(v)):
			unpur_item_index = int(v[pos]);
			P_row = P[user_index-1,:];
			Q_row = Q[unpur_item_index-1,:];
			
			recom_score = float(np.inner(P_row, Q_row) + B[unpur_item_index-1]);
			triple = (user_index, unpur_item_index, recom_score);
			tuple_list.append(triple);
	
		sorted_tuple_list = sorted(tuple_list, key=itemgetter(2), reverse = True);	
		two_dimen_sorted_list.append(sorted_tuple_list);
	
	# recommend top-N unpurchased items for each user
	
	user_recomitem_dict = {};
	
	for i in range(0,len(two_dimen_sorted_list)):
		sorted_tuple_list = two_dimen_sorted_list[i];
		recom_item_list = [];
		length = int(len(sorted_tuple_list));
		if N < length:
			for j in range(0,N):
				if j == 0:
					user_index = sorted_tuple_list[j][0];
					recom_item_index = sorted_tuple_list[j][1];
					recom_item_list.append(recom_item_index);
				else:
					recom_item_index = sorted_tuple_list[j][1];
					recom_item_list.append(recom_item_index);

			user_recomitem_dict[user_index] = recom_item_list;

		else:

			for j in range(0,len(sorted_tuple_list)):
				if j == 0:
					user_index = sorted_tuple_list[j][0];
					recom_item_index = sorted_tuple_list[j][1];
					recom_item_list.append(recom_item_index);
				else:
					recom_item_index = sorted_tuple_list[j][1];
					recom_item_list.append(recom_item_index);

			user_recomitem_dict[user_index] = recom_item_list;

	# compute the AUC metric and takes some time to modify since the logic is changed
	user_rankedunpuritem_dict = {};
	for m in range(0,len(two_dimen_sorted_list)):
		total_unpur_itemlist = [];
		for n in range(0,len(two_dimen_sorted_list[m])):
			if n == 0:
				user_index_ = two_dimen_sorted_list[m][n][0];
				total_unpur_itemlist.append(int(two_dimen_sorted_list[m][n][1]));
			else:
				total_unpur_itemlist.append(int(two_dimen_sorted_list[m][n][1]));	
		user_rankedunpuritem_dict[user_index_] = total_unpur_itemlist;

	auc = 0.0;
	active_test = 0;
	for q, r in user_rankedunpuritem_dict.items():
		unpur_itemindex_list = recom_item_test[q-1];
		if unpur_itemindex_list != []:
			for auc_ in range(0,len(unpur_itemindex_list)):
				curr_test_item = int(unpur_itemindex_list[auc_]);
				# remove the current test item in testlist
				removal_list = list(set(unpur_itemindex_list)-set([curr_test_item]));
				# cannot use set operation because want to keep the initial order of list
				comp_list = [x for x in r if x not in removal_list];
				active_test = active_test + 1;
				unpur_rank = int(comp_list.index(curr_test_item)) + 1;
				auc = auc + float(int(len(comp_list)-unpur_rank))/(len(comp_list)-1);

	nor_auc = float(auc)/(active_test);

	# calculate HR and ARHR and easy to update HR and ARHR

	HR_count = 0;
	rank_final = 0.0;

	for k, v in user_recomitem_dict.items():
		test_list = recom_item_test[k-1];	
		for te_ in range(0, len(test_list)):
			item_index = int(test_list[te_]);
			if item_index in v:
				HR_count = HR_count + 1;
				rank = int(v.index(item_index))+1;
				rank_final = rank_final + float(1)/rank;

	HR = float(HR_count)/num_test;
	ARHR = float(rank_final)/num_test;
	
	return [HR, ARHR, nor_auc];	

# code common derivative term
def Derivative(P, Q, B, a, b, c):

	r_abc = float((np.inner(P[a-1,:], Q[b-1,:]) + B[b-1]) - (np.inner(P[a-1,:], Q[c-1,:]) + B[c-1]));
#	print str(r_abc);
	deri_ = float(1+math.exp(-r_abc));
	deri = ((float(1)/(deri_))-1);
#	print str(deri_);
	return deri;

def Original_BPR(u, i, j, alpha, lamda, P, Q, B):
	# update P_u, Q_i, Q_j, B_i and B_j
	P[u-1,:] = P[u-1,:] - alpha*(Derivative(P, Q, B, u, i, j) * np.subtract(Q[i-1,:], Q[j-1,:]) + 2*lamda*P[u-1,:]);
	Q[i-1,:] = Q[i-1,:] - alpha*(Derivative(P, Q, B, u, i, j) * P[u-1,:] + 2*lamda*Q[i-1,:]);
	Q[j-1,:] = Q[j-1,:] - alpha*(Derivative(P, Q, B, u, i, j) * (-1) * P[u-1,:] + 2*lamda*Q[j-1,:]);
	B[i-1] = B[i-1] - alpha*(Derivative(P, Q, B, u, i, j) * 1 + 2*lamda*B[i-1]);
	B[j-1] = B[j-1] - alpha*(Derivative(P, Q, B, u, i, j) * (-1) + 2*lamda*B[j-1]);

	return [P, Q, B];
 
def Social_BPR(u, i, k, j, alpha, lamda, P, Q, B):
	# update each vector
	P[u-1,:] = P[u-1,:] - alpha*(Derivative(P, Q, B, u, i, k)*np.subtract(Q[i-1,:], Q[k-1,:]) + Derivative(P, Q, B, u, k, j)*np.subtract(Q[k-1,:], Q[j-1,:]) + 2*lamda*P[u-1,:]);
	Q[i-1,:] = Q[i-1,:] - alpha*(Derivative(P, Q, B, u, i, k)*P[u-1,:] + 2*lamda*Q[i-1,:]);
	Q[k-1,:] = Q[k-1,:] - alpha*(Derivative(P, Q, B, u, i, k)*(-1)*P[u-1,:]+Derivative(P, Q, B, u, k, j)*P[u-1,:] + 2*lamda*Q[k-1,:]);
	Q[j-1,:] = Q[j-1,:] - alpha*(Derivative(P, Q, B, u, k, j) * (-1)*P[u-1,:] + 2*lamda*Q[j-1,:]);
	B[i-1] = B[i-1] - alpha*(Derivative(P, Q, B, u, i, k)*1 + 2*lamda*B[i-1]);
	B[k-1] = B[k-1] - alpha*(Derivative(P, Q, B, u, i, k) *(-1) + Derivative(P, Q, B, u, k, j)*1 + 2*lamda*B[k-1]);
	B[j-1] = B[j-1] - alpha*(Derivative(P, Q, B, u, k, j)*(-1) + 2*lamda*B[j-1]);
	
	return [P, Q, B];

# initialization model parameters [P, Q, B]
def Initialization(K, num_user, num_item):

	# initilize P as a size m*k matrix with mean = 0 and std = 0.1 and same as matrix Q (n*k). And initialize vector B as (1*n)
	mean = 0.0;
	std = 0.1;

	# initilize P
	num_sample_P = int(num_user * K);
	P_list = np.random.normal(mean, std, num_sample_P);

	P_matrix_start = [];
	count_P = 0;
	while count_P < num_sample_P - 1:
		p = P_list[count_P:count_P + K];
		P_matrix_start.append(p);
		count_P = count_P + K;

	P = np.array(P_matrix_start);	

	# initilize Q
	num_sample_Q = int(num_item * K);
	Q_list = np.random.normal(mean, std, num_sample_Q);

	Q_matrix_start = [];
	count_Q = 0;
	while count_Q < num_sample_Q - 1:
		q = Q_list[count_Q:count_Q + K];
		Q_matrix_start.append(q);
		count_Q = count_Q + K;

	Q = np.array(Q_matrix_start);	

	# initialize B
	B_ = np.random.normal(mean, std, num_item);
	B = B_.tolist();

	return [P, Q, B];


# model main part
def Main_BPR(num_user, num_item, num_nnz, num_test, user_list, user_puritem_dict, user_unpuritem_dict, recom_item_test, G_neighbor_dict, K, alpha, lamda, N):

	P, Q, B = Initialization(K, num_user, num_item);
	convergence_file = open("conver.txt", "w");
	# calculate the HR, ARHR and AUC at the initilization step
	HR, ARHR, nor_auc = Evaluate(P, Q, B, user_unpuritem_dict, recom_item_test, N, num_test);
	convergence_file.write(str(HR) + "\t" + str(ARHR) + "\t" + str(nor_auc) + os.linesep);
	print "K=" + str(K) + ", " + "HR=" + str(HR) + ", " + "ARHR=" + str(ARHR) + ", " + "AUC=" + str(nor_auc);
	ns = 0;
	C_1 = 0;
	C_2 = 0;
	iter_ = int(num_nnz*100*50);
	for i in range(0, iter_):
		# uniform sampling
		user_index = random.sample(user_list, 1)[0];
		pur_item_index = random.sample(user_puritem_dict[user_index], 1)[0];
		if user_index in G_neighbor_dict.keys():
			friendlist = G_neighbor_dict[user_index];
			friend_index = random.sample(friendlist, 1)[0];
			friend_pur_item_list = user_puritem_dict[friend_index];
			diff_list1 = list(set(friend_pur_item_list)-set(user_puritem_dict[user_index]));
			diff_list2 = list(set(user_unpuritem_dict[user_index]) - set(friend_pur_item_list));
			if diff_list1 != [] and diff_list2 != []:
				friend_item = random.sample(diff_list1, 1)[0];
				unpur_item_index = random.sample(diff_list2, 1)[0];
				P, Q, B = Social_BPR(user_index, pur_item_index, friend_item, unpur_item_index, alpha, lamda, P, Q, B);	
				C_1 = C_1 + 1;
			else:
				unpur_item_index = random.sample(user_unpuritem_dict[user_index], 1)[0];
				P, Q, B = Original_BPR(user_index, pur_item_index, unpur_item_index, alpha, lamda, P, Q, B);
				C_2 = C_2 + 1;
		else:
			unpur_item_index = random.sample(user_unpuritem_dict[user_index], 1)[0];
			P, Q, B = Original_BPR(user_index, pur_item_index, unpur_item_index, alpha, lamda, P, Q, B);
			C_2 = C_2 + 1;

		ns = ns + 1;
		if ns % (num_nnz * 100) == 1:
			HR, ARHR, nor_auc = Evaluate(P, Q, B, user_unpuritem_dict, recom_item_test, N, num_test);
			print "K=" + str(K) + ", " + "HR=" + str(HR) + ", " + "ARHR=" + str(ARHR) + ", " + "AUC =" + str(nor_auc);						
			convergence_file.write(str(HR) + "\t" + str(ARHR) + "\t" + str(nor_auc) + os.linesep);
#	print str(HR) + "," + str(ARHR) + "," + str(nor_auc);	
	social_rate = float(C_1)/(C_1+C_2);
	print "social_rate = " + str(social_rate);

	return [P, Q, B];

if __name__ == "__main__":

	Trainfile = open('train.txt', 'r');
	Testfile = open('test.txt', 'r');
	K = 50;
	alpha = 0.02;
	lamda = 0.005;
	N = 10;
	subject_graph_file = open(sys.argv[1]);
	subj_dict_file = open(sys.argv[2]);
	num_user, num_item, num_nnz, user_list, user_puritem_dict, user_unpuritem_dict, recom_item_test, num_test, G_neighbor_dict = Parse(Trainfile, Testfile, subject_graph_file, subj_dict_file);
	P, Q, B = Main_BPR(num_user, num_item, num_nnz, num_test, user_list, user_puritem_dict, user_unpuritem_dict, recom_item_test, G_neighbor_dict, K, alpha, lamda, N);
