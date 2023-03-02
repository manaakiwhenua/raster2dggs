# -*- coding: utf-8 -*-
"""
Created on Fri Jan 27 09:06:48 2023

This code uses dask to repartition parquet files into parent cells, thus eliminating future border issues.

@author: ArdoJ
"""

import pandas as pd
import dask
import dask.dataframe as dd
import h3pandas
import h3
import pandas as pd 
import pyarrow
import time
import numpy as npg
import gc

# from dask.distributed import Client
# client = Client()
# print(client.dashboard_link)

gc.disable()

f = 'C:/Users/ArdoJ/Documents/NRC LUM/H3hex/codes/test'
Out= "test_sorted"   

ddf=dd.read_parquet(f)

ddf= ddf.set_index('h3_05')                             #MAKE SURE YOU SET THE RESOLUTION CORRECTLY

uniqueh3 = list(ddf.index.unique().compute())

ddf = ddf.repartition(divisions=sorted(uniqueh3 + [uniqueh3[-1]]))

ddf.to_parquet(Out, engine='pyarrow', compression='ZSTD')