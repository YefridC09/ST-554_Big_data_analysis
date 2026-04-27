import pandas as pd
import time
# We import the models that we are going to use
#And get the dataframe that we are going to use to take samples from
power_stm = pd.read_csv('power_streaming_data.csv')

for i in range(20):
    #We create a sample from the dataframe
    sample = power_stm.sample(n = 5, random_state = i) 
    # and export that sample as a csv file that is going to be used in the streaming part.
    sample.to_csv(f'power_data/sample{i}.csv', index = False)
    time.sleep(10)
    
