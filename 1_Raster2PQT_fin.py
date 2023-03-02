# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 11:57:50 2023

This code utilizes rioxarray, rasterio, h3pandas and pandas libraries to ingest raster images 
(driven by gdal) and assigns it to H3 dggs, while exporting it to a parquet file.

@author: ArdoJ
"""
import xarray
import rioxarray
import pandas as pd
import h3pandas
import rasterio
from rasterio import crs, MemoryFile
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
import time
import multiprocessing as mp
import threading
import concurrent.futures



#DEFINE FILE INPUT AND PARQUET OUTPUT
f = 'C:/Users/ArdoJ/Documents/NRC LUM/H3hex/resampled_image.tif'

Out= 'test'


H3res= 13                    #INPUT DESIRED H3 INDEX
parentres= H3res-8

allcores= mp.cpu_count()
num_workers=mp.cpu_count()-1


#######################################################################################################################################################
print("Number of cpu : ", num_workers, "out of :", allcores)

t = time.localtime()
#converting crs to WGS84 and converting raster type to vrt to avoid large reading problems

epsg_to = 4326

subset=[]


    
#Looping through raster blocks and indexing to H3
def h3func(sdf, nodata):
    subset = sdf.to_dataframe(name='value').reset_index().drop('spatial_ref', axis=1)
    h3index= subset.h3.geo_to_h3(H3res, lat_col='y', lng_col='x').drop(columns= ['x','y']).dropna()
    h3index= h3index.h3.h3_to_parent(parentres)
    h3index = h3index[h3index.value != nodata].reset_index()
    table = pa.Table.from_pandas(h3index)
    return table

with rasterio.open(f) as src:
    print('Source CRS:' +str(src.crs))
    with WarpedVRT(src,resampling=1,src_crs=src.crs,crs=crs.CRS.from_epsg(epsg_to),warp_mem_limit=12000) as vrt:
        print('Destination CRS:' +str(vrt.crs))
        da= rioxarray.open_rasterio(vrt).chunk({'y':'auto','x':'auto','band':1})
        da= da.squeeze().drop("spatial_ref").drop("band")
        da.name= "data"

        #time indicator
        t = time.localtime()
        current_time = time.strftime("%H:%M:%S", t)
        st = time.time()
        print('looping and indexing started at:', current_time, 'with H3 resolution of:', H3res, ',and H3 parent resolution of:', parentres, 'Using core workers:', num_workers)
        
        
        windows = [window for ij, window in vrt.block_windows()]

        read_lock = threading.Lock()
        write_lock = threading.Lock()

        def process(window):
            with read_lock:
                sdf = da.rio.isel_window(window)
                
            result = h3func(sdf, vrt.nodata)
            
            with write_lock:
                pq.write_to_dataset(result, root_path=Out, compression='ZSTD')

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_workers
        ) as executor:
            executor.map(process, windows) 


#time indicator
t = time.localtime()
current_time = time.strftime("%H:%M:%S", t)
et = time.time()
elapsed_time = et - st
print('H3 indexing completed at:', current_time,'Using', num_workers, 'Cores, Time taken:', elapsed_time/60 ,'min')

