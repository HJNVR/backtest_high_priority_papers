import os 
import json 
import boto3
import shutil
import pandas as pd


with open('../config/back_test.json') as config_file:
    data = json.load(config_file)

'''
n = data['number_of_computers'] # number of computers read from backtest.json

# clear existing folders containg name of node to avoid redudant, ex: node1, node2, ...
for f in os.listdir('../config/'):
    if 'node' in f:
        shutil.rmtree('../config/' + f)

# create folders starting with node with the same number as number of computers, ex: node1, node2, ... noden
for i in range(1, n+1):
        if not os.path.isdir('../config/node'+str(i)):
            os.mkdir('../config/node'+str(i))
'''
# create a dictionary with keys starting from 1, and values are years+month, ex: {1:200001, 2:200002 ... }

d = {}
count = 1 
for i in range(int(data['first_train_end_ym'][:4]), int(data['end_date'][:4])+1):
    for j in range(12):
        if len(str(j+1)) >= 2:
            d[count] = str(i)+str(j+1)
        else:
            d[count] = str(i)+'0'+str(j+1)
        count += 1 

l = [] # a list of date keys to be removed
for key, val in d.items():
    if key < [k for k, v in d.items() if v == data['first_train_end_ym']][0] or \
        key > [k for k, v in d.items() if v == data['end_date']][0]:
        l.append(key)

for i in l:
    del d[i]

train_end_l = []
end_date_l = []

for key in list(d.keys())[:-1]:
    train_end_l.append(d[key])
    end_date_l.append(str(int(d[key+1])))
'''
l_configs = []  # a list of config dict for each month

for key in list(d.keys())[:-1]:
    data['first_train_end_ym'] = d[key]
    data['end_date'] = str(int(d[key+1]))
    json_object = json.dumps(data, indent = 1)
    l_configs.append(json_object)
'''
# generate status.csv file

status = pd.DataFrame()
status['task_id'] = range(1, len(train_end_l)+1)
status['task_status'] = 'P'
status['first_train_end_ym'] = train_end_l
status['end_date'] = end_date_l
status['start_time_date'] = None
status['end_time_date'] = None
status['execution_time'] = None
#status['health_condition'] = None
status['heart_beat_time'] = None
status.to_csv('status.csv')

def upload_file():
    s3 = boto3.client('s3')
    s3.upload_file('status.csv', 'backtest-11-papers', 'status.csv')

upload_file()
'''
if data['divide_load_equally'].lower() == 'yes': # equally split
    count = 1 # the first sub json file should start from 1
    for i in range(1, n+1):
        if i != n:
            for j in range(len(l_configs) // n):
                with open("../config/node"+str(i)+"/back_test"+str(count)+".json", "w") as outfile:
                    outfile.write(l_configs[count-1])
                    count += 1
        else:
            for k in range(count, len(l_configs)+1):
                with open("../config/node"+str(i)+"/back_test"+str(k)+".json", "w") as outfile:
                    outfile.write(l_configs[k-1])

elif data['divide_load_equally'].lower() == 'no': # split according to ratios
    if len(data['load']) != n: # logic check # 1: check length of load
        raise Exception('Length of load does not match with number_of_computers in back_test.json')
    if sum(data['load']) != 1: # logic check # 2: check sum of load
        raise Exception('Config files allocation is not complete, sum of load should be 1 in back_test.json')
    count = 1 
    for i in range(1, n+1):
        if i != n:
            for j in range(count, int(len(l_configs) * data['load'][i-1])+count): # update count for every loop
                with open("../config/node"+str(i)+"/back_test"+str(count)+".json", "w") as outfile:
                    outfile.write(l_configs[count-1])
                    count += 1
        else:
            for k in range(count, len(l_configs)+1):
                with open("../config/node"+str(i)+"/back_test"+str(k)+".json", "w") as outfile:
                    outfile.write(l_configs[k-1])
else: # logic check # 3: check divide_load_equially value
    raise Exception('Please type only yes/no for divide_load_equally in back_test.json')
'''

#print('Pleas check config folder for node folders of '+ str(n) + ' computers')