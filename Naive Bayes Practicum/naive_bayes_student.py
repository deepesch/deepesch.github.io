import operator
from collections import Counter, defaultdict
import numpy as np

class NaiveBayes(object):

    def __init__(self, alpha=1):
        self.prior = {}
        self.per_feature_per_label = {}
        self.feature_sum_per_label = {}
        self.likelihood = {}
        self.posterior = {}
        self.alpha = alpha
        self.p = None

    def compute_prior(self, y):
        c = Counter(y)
        num_classes = float(len(y))
        self.prior = {k:v/num_classes for k,v in c.iteritems()}

    def compute_likelihood(self, X, y):
        self.per_feature_per_label = defaultdict(lambda: np.zeros(X.shape[1]))

        for e in zip(X, y):
            self.per_feature_per_label[e[1]] += e[0] #Syi
        
        self.feature_sum_per_label = {
            k:sum(map(float, v)) 
            for k, v in self.per_feature_per_label.iteritems()
        }
        
        self.likelihood = {
            class_:
            (self.per_feature_per_label[class_]+1)/(self.feature_sum_per_label[class_] 
                                                    + X.shape[1]
                                                    )  
            for class_ in np.unique(y)
        }

    def fit(self, X, y):
        self.p = X.shape[1]
        self.compute_prior(y)
        self.compute_likelihood(X, y)

    def predict(self, X):        
        log_posterior = {k:np.log(v) for k,v in self.likelihood.iteritems()} #log of p(xi|y)
        log_prior = {k:np.log(v) for k,v in self.prior.iteritems()} #log of p(y)
        
        classes = [ ]
        
        for i in xrange(X.shape[0]):
            self.posterior =  {k: np.dot(X[i],(log_posterior[k])) + (log_prior[k]) for k in log_posterior} #xi * log * ( p(xi|y) ) + log * ( (p(y) )
            classes.append(max(self.posterior.iteritems(), key=operator.itemgetter(1))[0])
        
        
        return np.array(classes)

    def score(self, X, y):
        return np.mean(y == self.predict(X))
        
