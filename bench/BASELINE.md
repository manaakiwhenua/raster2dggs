# Profiling baseline

Regenerate with `python bench/run_baseline.py`. This file is committed so
that `git diff` after a change shows the performance delta directly.

**Timings are machine-specific.** A diff is only meaningful when both sides
were measured on the same machine; this is a local development aid, not a
cross-machine comparison and not a CI gate. Run-to-run noise is a few
percent, so marginal diffs are not signal — the changes worth reading here
are order-of-magnitude.

Indentation in the tables is nesting, not addition: inner phases are
measured inside their parent. See the `--profile` section of the README.

| | |
|---|---|
| raster2dggs | 0.13.1 |
| Python | 3.12.13 |
| rasterio / GDAL | 1.5.0 / 3.12.1 |
| platform | Linux x86_64 |
| CPU count | 8 |

## geohash --point value (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2
  pixels_read: 74,888
  rows_indexed: 74,888
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      0.131    51.3%        1     131.437
    window_total                   0.130    50.6%        2      64.779
      read_block                   0.007     2.7%        2       3.512
      reshape                      0.001     0.2%        2       0.317
      reproject                    0.032    12.5%        2      16.001
      build_frame                  0.001     0.2%        2       0.258
      dggs_index                   0.061    23.9%        2      30.583
      arrow_build                  0.003     1.3%        2       1.645
      parquet_write                0.019     7.5%        2       9.650
  stage2.total                     0.027    10.4%        1      26.713
  --------------------------------------------------------------------
  wall clock                       0.256   100.0%
  Stage 1 concurrency: 0.99x (0.130s of work in 0.131s wall)
```

## maidenhead --point value (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2
  pixels_read: 74,888
  rows_indexed: 74,888
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      0.490    79.2%        1     489.935
    window_total                   0.488    78.8%        2     243.919
      read_block                   0.007     1.2%        2       3.713
      reshape                      0.001     0.1%        2       0.326
      reproject                    0.033     5.3%        2      16.307
      build_frame                  0.001     0.1%        2       0.311
      dggs_index                   0.418    67.5%        2     208.899
      arrow_build                  0.003     0.5%        2       1.539
      parquet_write                0.020     3.2%        2       9.766
  stage2.total                     0.026     4.3%        1      26.388
  --------------------------------------------------------------------
  wall clock                       0.619   100.0%
  Stage 1 concurrency: 1.00x (0.488s of work in 0.490s wall)
```

## s2 --point value (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2
  pixels_read: 74,888
  rows_indexed: 74,888
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      1.137    89.2%        1    1137.042
    window_total                   1.135    89.0%        2     567.317
      read_block                   0.008     0.7%        2       4.158
      reshape                      0.001     0.1%        2       0.351
      reproject                    0.037     2.9%        2      18.589
      build_frame                  0.001     0.1%        2       0.339
      dggs_index                   1.055    82.8%        2     527.670
      arrow_build                  0.003     0.3%        2       1.733
      parquet_write                0.021     1.6%        2      10.506
  stage2.total                     0.031     2.4%        1      31.043
  --------------------------------------------------------------------
  wall clock                       1.275   100.0%
  Stage 1 concurrency: 1.00x (1.135s of work in 1.137s wall)
```

## a5 --point value (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2
  pixels_read: 74,888
  rows_indexed: 74,888
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      1.723    92.9%        1    1723.115
    window_total                   1.721    92.7%        2     860.441
      read_block                   0.007     0.4%        2       3.642
      reshape                      0.001     0.0%        2       0.331
      reproject                    0.032     1.7%        2      16.183
      build_frame                  0.001     0.0%        2       0.327
      dggs_index                   1.652    89.1%        2     826.203
      arrow_build                  0.003     0.2%        2       1.616
      parquet_write                0.019     1.0%        2       9.375
  stage2.total                     0.025     1.4%        1      25.402
  --------------------------------------------------------------------
  wall clock                       1.855   100.0%
  Stage 1 concurrency: 1.00x (1.721s of work in 1.723s wall)
```

## rhp --point value (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2
  pixels_read: 74,888
  rows_indexed: 74,888
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      4.124    93.4%        1    4123.911
    window_total                   4.121    93.3%        2    2060.744
      read_block                   0.008     0.2%        2       4.037
      reshape                      0.001     0.0%        2       0.334
      reproject                    0.037     0.8%        2      18.428
      build_frame                  0.001     0.0%        2       0.378
      dggs_index                   4.043    91.6%        2    2021.728
      arrow_build                  0.004     0.1%        2       2.170
      parquet_write                0.022     0.5%        2      10.867
  stage2.total                     0.030     0.7%        1      30.438
  --------------------------------------------------------------------
  wall clock                       4.415   100.0%
  Stage 1 concurrency: 1.00x (4.121s of work in 4.124s wall)
```

## isea4r --point value (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2
  pixels_read: 74,888
  rows_indexed: 74,888
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      0.617    80.8%        1     616.887
    window_total                   0.615    80.5%        2     307.287
      read_block                   0.007     1.0%        2       3.684
      reshape                      0.001     0.1%        2       0.359
      reproject                    0.032     4.2%        2      16.072
      build_frame                  0.001     0.1%        2       0.296
      dggs_index                   0.547    71.7%        2     273.702
      arrow_build                  0.003     0.4%        2       1.528
      parquet_write                0.018     2.3%        2       8.910
  stage2.total                     0.029     3.9%        1      29.498
  --------------------------------------------------------------------
  wall clock                       0.764   100.0%
  Stage 1 concurrency: 1.00x (0.615s of work in 0.617s wall)
```

## H3 --point value (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2
  pixels_read: 74,888
  rows_indexed: 74,888
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      0.207    59.8%        1     206.979
    window_total                   0.205    59.2%        2     102.484
      read_block                   0.008     2.2%        2       3.769
      reshape                      0.001     0.2%        2       0.371
      reproject                    0.033     9.7%        2      16.706
      build_frame                  0.001     0.2%        2       0.281
      dggs_index                   0.132    38.0%        2      65.820
      arrow_build                  0.003     1.0%        2       1.731
      parquet_write                0.022     6.4%        2      11.016
  stage2.total                     0.039    11.2%        1      38.878
  --------------------------------------------------------------------
  wall clock                       0.346   100.0%
  Stage 1 concurrency: 0.99x (0.205s of work in 0.207s wall)
```

## H3 --overlay weighted (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      0.221    63.3%        1     220.900
    window_total                   0.219    62.7%        2     109.394
      (not broken down)            0.044    12.7%        -           -
      cells_in_bbox                0.001     0.4%        2       0.616
      cell_polygons                0.019     5.4%        2       9.383
      exactextract                 0.152    43.6%        2      76.058
      parquet_write                0.002     0.7%        2       1.219
  stage2.total                     0.018     5.0%        1      17.582
  --------------------------------------------------------------------
  wall clock                       0.349   100.0%
  Stage 1 concurrency: 0.99x (0.219s of work in 0.221s wall)
```

## H3 --sample bilinear (small, 3-band)

```
Profile
  bands: 3
  block_shape: 256x256
  internally_tiled: True
  raster_size: 253x296
  threads: 1
  windows: 2

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      0.169    54.1%        1     169.310
    window_total                   0.167    53.4%        2      83.607
      (not broken down)            0.076    24.4%        -           -
      read_block                   0.013     4.2%        2       6.504
      cells_in_bbox                0.028     9.0%        2      14.081
      cells_to_lonlat              0.038    12.2%        2      19.135
      parquet_write                0.011     3.6%        2       5.658
  stage2.total                     0.032    10.1%        1      31.688
  --------------------------------------------------------------------
  wall clock                       0.313   100.0%
  Stage 1 concurrency: 0.99x (0.167s of work in 0.169s wall)
```

## H3 --point value (full DEM, 1-band, single-threaded)

```
Profile
  bands: 1
  block_shape: 256x256
  internally_tiled: True
  raster_size: 4977x9661
  threads: 1
  windows: 760
  pixels_read: 48,082,797
  rows_indexed: 9,107,964
  valid pixels: 18.9% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     25.226    92.4%        1   25226.180
    window_total                  25.205    92.3%      760      33.165
      (not broken down)            1.953     7.2%        -           -
      read_block                   1.932     7.1%      760       2.542
      reshape                      0.251     0.9%      760       0.330
      reproject                    2.000     7.3%      760       2.632
      build_frame                  0.191     0.7%      760       0.251
      dggs_index                  16.819    61.6%      760      22.130
      arrow_build                  0.450     1.6%      760       0.592
      parquet_write                1.609     5.9%      227       7.090
  stage2.total                     1.891     6.9%        1    1891.467
  --------------------------------------------------------------------
  wall clock                      27.307   100.0%
  Stage 1 concurrency: 1.00x (25.205s of work in 25.226s wall)
```

## H3 --point value (full DEM, 1-band, default threads)

```
Profile
  bands: 1
  block_shape: 256x256
  internally_tiled: True
  raster_size: 4977x9661
  threads: 7
  windows: 760
  pixels_read: 48,082,797
  rows_indexed: 9,107,964
  valid pixels: 18.9% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     23.379    91.8%        1   23379.142
    window_total                 163.483   642.3%      760     215.110
      (not broken down)           11.178    43.9%        -           -
      read_block                  31.126   122.3%      760      40.955
      reshape                     20.587    80.9%      760      27.088
      reproject                    6.741    26.5%      760       8.870
      build_frame                  1.657     6.5%      760       2.180
      dggs_index                  68.625   269.6%      760      90.297
      arrow_build                 16.592    65.2%      760      21.831
      parquet_write                6.978    27.4%      227      30.740
  stage2.total                     1.866     7.3%        1    1865.812
  --------------------------------------------------------------------
  wall clock                      25.454   100.0%
  Stage 1 concurrency: 6.99x (163.483s of work in 23.379s wall)
```

## H3 --overlay weighted (full DEM, 1-band, default threads)

```
Profile
  bands: 1
  block_shape: 256x256
  internally_tiled: True
  raster_size: 4977x9661
  threads: 7
  windows: 760

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     32.191    98.8%        1   32190.960
    window_total                 224.845   690.3%      760     295.849
      (not broken down)           47.817   146.8%        -           -
      cells_in_bbox                0.212     0.7%      760       0.280
      cell_polygons               19.438    59.7%      760      25.577
      exactextract               153.821   472.3%      760     202.397
      parquet_write                3.555    10.9%      291      12.218
  stage2.total                     0.221     0.7%        1     220.886
  --------------------------------------------------------------------
  wall clock                      32.570   100.0%
  Stage 1 concurrency: 6.98x (224.845s of work in 32.191s wall)
```

## H3 --sample bilinear (full DEM, 1-band, default threads)

```
Profile
  bands: 1
  block_shape: 256x256
  internally_tiled: True
  raster_size: 4977x9661
  threads: 7
  windows: 760

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      6.124    95.4%        1    6124.001
    window_total                  42.315   659.0%      760      55.677
      (not broken down)           10.393   161.9%        -           -
      read_block                  29.507   459.5%      760      38.825
      cells_in_bbox                0.128     2.0%      760       0.169
      cells_to_lonlat              0.278     4.3%      760       0.366
      parquet_write                2.008    31.3%      207       9.701
  stage2.total                     0.153     2.4%        1     152.707
  --------------------------------------------------------------------
  wall clock                       6.421   100.0%
  Stage 1 concurrency: 6.91x (42.315s of work in 6.124s wall)
```

## H3 --point value, 3 aggregations (full DEM, 1-band, default threads)

```
Profile
  bands: 1
  block_shape: 256x256
  internally_tiled: True
  raster_size: 4977x9661
  threads: 7
  windows: 760
  pixels_read: 48,082,797
  rows_indexed: 9,107,964
  valid pixels: 18.9% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     22.417    84.8%        1   22417.296
    window_total                 156.746   592.9%      760     206.245
      (not broken down)           10.830    41.0%        -           -
      read_block                  30.510   115.4%      760      40.144
      reshape                     17.520    66.3%      760      23.052
      reproject                    7.147    27.0%      760       9.403
      build_frame                  1.765     6.7%      760       2.323
      dggs_index                  67.715   256.1%      760      89.099
      arrow_build                 14.190    53.7%      760      18.671
      parquet_write                7.070    26.7%      227      31.145
  stage2.total                     3.838    14.5%        1    3837.934
  --------------------------------------------------------------------
  wall clock                      26.437   100.0%
  Stage 1 concurrency: 6.99x (156.746s of work in 22.417s wall)
```

## H3 --point value (10-band int16, ~89% valid, single-threaded)

```
Profile
  bands: 10
  block_shape: 128x128
  internally_tiled: True
  raster_size: 2574x2484
  threads: 1
  windows: 420
  pixels_read: 6,393,816
  rows_indexed: 6,020,195
  valid pixels: 94.2% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     18.022    86.4%        1   18022.288
    window_total                  18.007    86.3%      420      42.874
      (not broken down)            1.207     5.8%        -           -
      read_block                   1.461     7.0%      420       3.480
      reshape                      0.189     0.9%      420       0.451
      reproject                    1.271     6.1%      420       3.027
      build_frame                  0.144     0.7%      420       0.343
      dggs_index                  10.039    48.1%      420      23.903
      arrow_build                  1.154     5.5%      420       2.749
      parquet_write                2.540    12.2%      398       6.382
  stage2.total                     2.391    11.5%        1    2391.362
  --------------------------------------------------------------------
  wall clock                      20.866   100.0%
  Stage 1 concurrency: 1.00x (18.007s of work in 18.022s wall)
```

## H3 --point value (10-band int16, ~89% valid, default threads)

```
Profile
  bands: 10
  block_shape: 128x128
  internally_tiled: True
  raster_size: 2574x2484
  threads: 7
  windows: 420
  pixels_read: 6,393,816
  rows_indexed: 6,020,195
  valid pixels: 94.2% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     16.798    84.8%        1   16798.382
    window_total                 117.308   592.1%      420     279.305
      (not broken down)            8.166    41.2%        -           -
      read_block                  16.251    82.0%      420      38.692
      reshape                     17.666    89.2%      420      42.063
      reproject                    4.428    22.4%      420      10.543
      build_frame                 10.626    53.6%      420      25.300
      dggs_index                  29.233   147.6%      420      69.603
      arrow_build                 20.636   104.2%      420      49.133
      parquet_write               10.302    52.0%      398      25.884
  stage2.total                     2.541    12.8%        1    2540.991
  --------------------------------------------------------------------
  wall clock                      19.811   100.0%
  Stage 1 concurrency: 6.98x (117.308s of work in 16.798s wall)
```

## H3 --point value (3-band uint16, no nodata, striped, single-threaded)

```
Profile
  bands: 3
  block_shape: 1001x1
  internally_tiled: False
  raster_size: 1001x1001
  threads: 1
  windows: 1001
  pixels_read: 1,002,001
  rows_indexed: 1,002,001
  valid pixels: 100.0% of those read

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     14.254    61.9%        1   14254.430
    window_total                  14.227    61.8%     1001      14.213
      (not broken down)            2.659    11.6%        -           -
      read_block                   2.322    10.1%     1001       2.319
      reshape                      0.067     0.3%     1001       0.067
      reproject                    0.241     1.0%     1001       0.241
      build_frame                  0.195     0.8%     1001       0.195
      dggs_index                   4.615    20.0%     1001       4.610
      arrow_build                  2.020     8.8%     1001       2.018
      parquet_write                2.108     9.2%     1001       2.105
  stage2.total                     7.305    31.7%        1    7304.915
  --------------------------------------------------------------------
  wall clock                      23.022   100.0%
  Stage 1 concurrency: 1.00x (14.227s of work in 14.254s wall)
```
