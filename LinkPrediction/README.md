# NOUS Link Prediction
## 1 Introduction 
Implements Bayesian Personalized Ranking model for top N item recommendation, uses stochastic gradient descent for learning model parameters

## 2 Build and Execute Hello World Program(s):
### 2.1 Prerequisites
python 2.7+
Python modules used:
numpy, scipy, networkx

### 2.2 Run Hello World
$cd $NOUS_HOME/LinkPrediction/examples
$python ../src/social_BPR.py graph_deadsWith.txt subj_entity.txt

For the output, it will print out the HR, ARHR, and AUC for each iteration

## Publications
Baichuan Zhang, Sutanay Choudhury, Mohammad Al Hasan, Xia Ning, Khushbu Agarwal, Sumit Purohit, and Paola Pesntez Cabrera: Trust from the past: Bayesian Personalized Ranking based Link Prediction in Knowledge Graphs, published in SDM Workshop on Mining Networks and Graphs (MNG 2016), Miami, FL.
