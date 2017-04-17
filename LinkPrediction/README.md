# NOUS Link Prediction
## 1 .Introduction : Given a knowledge base, predicts link labels between entities. Implements Link Prediction algorithm as decsribed in http://cseweb.ucsd.edu/~jmcauley/pdfs/cikm14.pdf
## 2. Build and Execute Hello World Program(s):
### 2.1 Prerequisites
python 2.7+
Python modules used:
numpy, scipy, networkx

### 2.2 Run Hello World
python src/social_BPR.py examples/graph_deadsWith.txt examples/subj_entity.txt

For the output, it will print out the HR, ARHR, and AUC for each iteration

## Publications
For the model implementation details, please refer to the following paper:
http://cseweb.ucsd.edu/~jmcauley/pdfs/cikm14.pdf
