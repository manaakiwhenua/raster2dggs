# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 09:23:46 2023

this code takes in sorted parquet files and aggregates based on h3index. 

@author: ArdoJ
"""

import pandas as pd
import dask
import dask.dataframe as dd
import h3pandas
import pandas as pd 
import pyarrow
import time
import numpy as np
# from dask.distributed import Client
# client = Client()
# print(client.dashboard_link)


#DEFINE FILE INPUT AND PARQUET OUTPUT
f = 'C:/Users/ArdoJ/Documents/NRC LUM/H3hex/codes/test_sorted'
Out= "test_aggregated_pqt"

H3res= 13 

ddf=dd.read_parquet(f)
ddf = ddf.set_index(ddf.index, npartitions='auto')


st = time.time()

ddf = ddf.map_partitions(lambda df: df.groupby('h3_13').mean().round(1))    # MAKE SURE YOU SET THE COLUMN YOU WANT TO AGGREGATE BY


# h3ag

ddf.to_parquet(Out, engine='pyarrow', compression='ZSTD')
t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
et = time.time()
elapsed_time = et - st
print('Aggregating completed at:', current_time, 'time taken:', elapsed_time/60)