# -*- coding: utf-8 -*-
"""
Created on Thu Oct 11 22:37:03 2018

@author: david zhangll
thzll2001@gmail.com
lstm to predict the weblog 
"""

import numpy as np
import pandas as pd
import os

#read the directory of output 
dirpath="F:/req_sec/"

lines = []

for a in os.listdir(dirpath):
    if a[0]!='.' and a[0]!='_':
        with open(dirpath+a) as file:
            for line in file: 
                line=line.replace(")"," ")
                items=line.split(",")
                line = items[1].strip() 
                lines.append(line) 


from numpy import array
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import TimeDistributed
from keras.layers import LSTM
import math
import matplotlib.pylab as plt
import numpy as np

trainlst=lines[0:4000]
len(lines)

length = len(trainlst)

seqX = np.zeros(length)
seqy = np.zeros(length)

for a in range(1,length):
    seqX[a]=(int(lines[a-1]))
    seqy[a]=(int(lines[a]))

#length=length-1

X = seqX.reshape(length, 1, 1)
y = seqy.reshape(length, 1)

X.shape
y.shape

n_neurons = 200
#length
n_batch = 5
n_epoch = 20

model = Sequential()
model.add(LSTM(n_neurons, input_shape=(1, 1) ))
model.add(Dense(1))
model.compile(loss='mean_squared_error', optimizer='adam')

model.fit(X, y, epochs=n_epoch, batch_size=n_batch, verbose=2)

result = model.predict(X, batch_size=n_batch, verbose=0)
newGraph = list()
for value in result:
    newGraph.append(value)

plt.plot(newGraph)
plt.plot(seqX)
plt.plot(seqy)
plt.legend(["Prediction", "Test X", "Test y"])
plt.show()

