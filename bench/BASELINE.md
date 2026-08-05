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
  stage1.wall                      0.172    56.8%        1     171.697
    window_total                   0.169    56.0%        2      84.735
      read_block                   0.008     2.5%        2       3.765
      reshape                      0.001     0.2%        2       0.351
      reproject                    0.035    11.5%        2      17.326
      build_frame                  0.001     0.3%        2       0.396
      dggs_index                   0.098    32.3%        2      48.848
      arrow_build                  0.003     1.1%        2       1.720
      parquet_write                0.019     6.2%        2       9.451
  stage2.total                     0.029     9.7%        1      29.233
  --------------------------------------------------------------------
  wall clock                       0.302   100.0%
  Stage 1 concurrency: 0.99x (0.169s of work in 0.172s wall)
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
  stage1.wall                      0.152    53.9%        1     151.655
    window_total                   0.149    53.1%        2      74.743
      read_block                   0.007     2.6%        2       3.659
      reshape                      0.001     0.2%        2       0.336
      reproject                    0.035    12.4%        2      17.384
      build_frame                  0.001     0.2%        2       0.265
      dggs_index                   0.079    28.2%        2      39.653
      arrow_build                  0.004     1.3%        2       1.856
      parquet_write                0.017     6.2%        2       8.724
  stage2.total                     0.026     9.3%        1      26.290
  --------------------------------------------------------------------
  wall clock                       0.281   100.0%
  Stage 1 concurrency: 0.99x (0.149s of work in 0.152s wall)
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
  stage1.wall                      0.199    59.4%        1     199.336
    window_total                   0.197    58.8%        2      98.598
      read_block                   0.007     2.1%        2       3.595
      reshape                      0.001     0.2%        2       0.313
      reproject                    0.032     9.6%        2      16.165
      build_frame                  0.001     0.2%        2       0.258
      dggs_index                   0.128    38.1%        2      63.824
      arrow_build                  0.004     1.1%        2       1.767
      parquet_write                0.020     6.0%        2      10.026
  stage2.total                     0.038    11.2%        1      37.561
  --------------------------------------------------------------------
  wall clock                       0.335   100.0%
  Stage 1 concurrency: 0.99x (0.197s of work in 0.199s wall)
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
  stage1.wall                      0.225    61.4%        1     224.748
    window_total                   0.223    60.9%        2     111.328
      read_block                   0.007     1.9%        2       3.524
      reshape                      0.001     0.2%        2       0.328
      reproject                    0.034     9.4%        2      17.156
      build_frame                  0.001     0.1%        2       0.266
      dggs_index                   0.147    40.3%        2      73.665
      arrow_build                  0.003     1.0%        2       1.738
      parquet_write                0.024     6.4%        2      11.754
  stage2.total                     0.040    10.8%        1      39.527
  --------------------------------------------------------------------
  wall clock                       0.366   100.0%
  Stage 1 concurrency: 0.99x (0.223s of work in 0.225s wall)
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
  stage1.wall                      0.169    55.7%        1     169.037
    window_total                   0.167    55.0%        2      83.451
      read_block                   0.008     2.5%        2       3.757
      reshape                      0.001     0.2%        2       0.321
      reproject                    0.032    10.4%        2      15.850
      build_frame                  0.001     0.2%        2       0.256
      dggs_index                   0.100    32.9%        2      49.942
      arrow_build                  0.003     1.0%        2       1.563
      parquet_write                0.018     6.0%        2       9.052
  stage2.total                     0.031    10.3%        1      31.410
  --------------------------------------------------------------------
  wall clock                       0.304   100.0%
  Stage 1 concurrency: 0.99x (0.167s of work in 0.169s wall)
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
  stage1.wall                      0.175    57.4%        1     175.291
    window_total                   0.173    56.7%        2      86.540
      read_block                   0.007     2.3%        2       3.510
      reshape                      0.001     0.2%        2       0.339
      reproject                    0.031    10.3%        2      15.742
      build_frame                  0.001     0.2%        2       0.261
      dggs_index                   0.103    33.9%        2      51.654
      arrow_build                  0.004     1.2%        2       1.765
      parquet_write                0.021     6.9%        2      10.511
  stage2.total                     0.029     9.4%        1      28.724
  --------------------------------------------------------------------
  wall clock                       0.305   100.0%
  Stage 1 concurrency: 0.99x (0.173s of work in 0.175s wall)
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
  stage1.wall                      0.211    59.6%        1     211.184
    window_total                   0.209    59.0%        2     104.568
      read_block                   0.008     2.3%        2       4.000
      reshape                      0.001     0.2%        2       0.330
      reproject                    0.033     9.4%        2      16.571
      build_frame                  0.001     0.2%        2       0.302
      dggs_index                   0.137    38.7%        2      68.545
      arrow_build                  0.003     0.9%        2       1.647
      parquet_write                0.021     5.8%        2      10.258
  stage2.total                     0.039    11.0%        1      38.938
  --------------------------------------------------------------------
  wall clock                       0.354   100.0%
  Stage 1 concurrency: 0.99x (0.209s of work in 0.211s wall)
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
  stage1.wall                      0.238    63.6%        1     237.895
    window_total                   0.236    63.0%        2     117.882
      (not broken down)            0.046    12.2%        -           -
      cells_in_bbox                0.001     0.4%        2       0.746
      cell_polygons                0.017     4.4%        2       8.308
      exactextract                 0.169    45.1%        2      84.429
      parquet_write                0.003     0.8%        2       1.547
  stage2.total                     0.020     5.3%        1      19.804
  --------------------------------------------------------------------
  wall clock                       0.374   100.0%
  Stage 1 concurrency: 0.99x (0.236s of work in 0.238s wall)
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
  stage1.wall                      0.151    51.3%        1     151.155
    window_total                   0.149    50.6%        2      74.555
      (not broken down)            0.064    21.9%        -           -
      read_block                   0.012     4.0%        2       5.882
      cells_in_bbox                0.026     9.0%        2      13.246
      cells_to_lonlat              0.035    11.9%        2      17.487
      parquet_write                0.011     3.9%        2       5.706
  stage2.total                     0.030    10.3%        1      30.238
  --------------------------------------------------------------------
  wall clock                       0.295   100.0%
  Stage 1 concurrency: 0.99x (0.149s of work in 0.151s wall)
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
  stage1.wall                     22.617    91.3%        1   22616.637
    window_total                  22.598    91.3%      760      29.734
      (not broken down)            1.817     7.3%        -           -
      read_block                   1.807     7.3%      760       2.377
      reshape                      0.222     0.9%      760       0.292
      reproject                    1.820     7.4%      760       2.395
      build_frame                  0.172     0.7%      760       0.226
      dggs_index                  14.857    60.0%      760      19.548
      arrow_build                  0.420     1.7%      760       0.553
      parquet_write                1.483     6.0%      227       6.534
  stage2.total                     1.974     8.0%        1    1973.929
  --------------------------------------------------------------------
  wall clock                      24.763   100.0%
  Stage 1 concurrency: 1.00x (22.598s of work in 22.617s wall)
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
  stage1.wall                     21.853    91.7%        1   21852.694
    window_total                 152.795   641.1%      760     201.047
      (not broken down)           10.074    42.3%        -           -
      read_block                  29.004   121.7%      760      38.164
      reshape                     19.209    80.6%      760      25.275
      reproject                    7.421    31.1%      760       9.764
      build_frame                  1.493     6.3%      760       1.965
      dggs_index                  65.589   275.2%      760      86.301
      arrow_build                 13.575    57.0%      760      17.862
      parquet_write                6.431    27.0%      227      28.330
  stage2.total                     1.801     7.6%        1    1801.174
  --------------------------------------------------------------------
  wall clock                      23.834   100.0%
  Stage 1 concurrency: 6.99x (152.795s of work in 21.853s wall)
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
  stage1.wall                     31.584    98.8%        1   31584.360
    window_total                 220.569   689.8%      760     290.222
      (not broken down)           46.830   146.5%        -           -
      cells_in_bbox                0.209     0.7%      760       0.275
      cell_polygons               20.849    65.2%      760      27.433
      exactextract               149.186   466.6%      760     196.298
      parquet_write                3.494    10.9%      291      12.007
  stage2.total                     0.231     0.7%        1     231.190
  --------------------------------------------------------------------
  wall clock                      31.974   100.0%
  Stage 1 concurrency: 6.98x (220.569s of work in 31.584s wall)
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
  stage1.wall                      6.419    95.5%        1    6418.520
    window_total                  44.271   658.7%      760      58.251
      (not broken down)           10.546   156.9%        -           -
      read_block                  31.238   464.8%      760      41.103
      cells_in_bbox                0.137     2.0%      760       0.180
      cells_to_lonlat              0.287     4.3%      760       0.378
      parquet_write                2.062    30.7%      207       9.959
  stage2.total                     0.157     2.3%        1     156.571
  --------------------------------------------------------------------
  wall clock                       6.721   100.0%
  Stage 1 concurrency: 6.90x (44.271s of work in 6.419s wall)
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
  stage1.wall                     17.879    86.5%        1   17879.494
    window_total                  17.865    86.4%      420      42.535
      (not broken down)            1.238     6.0%        -           -
      read_block                   1.459     7.1%      420       3.474
      reshape                      0.209     1.0%      420       0.498
      reproject                    1.249     6.0%      420       2.974
      build_frame                  0.144     0.7%      420       0.343
      dggs_index                   9.853    47.6%      420      23.460
      arrow_build                  1.184     5.7%      420       2.819
      parquet_write                2.528    12.2%      398       6.351
  stage2.total                     2.335    11.3%        1    2334.830
  --------------------------------------------------------------------
  wall clock                      20.681   100.0%
  Stage 1 concurrency: 1.00x (17.865s of work in 17.879s wall)
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
  stage1.wall                     15.900    85.1%        1   15900.436
    window_total                 111.075   594.4%      420     264.465
      (not broken down)            7.362    39.4%        -           -
      read_block                  15.921    85.2%      420      37.907
      reshape                     16.699    89.4%      420      39.759
      reproject                    4.506    24.1%      420      10.730
      build_frame                 10.202    54.6%      420      24.291
      dggs_index                  26.933   144.1%      420      64.126
      arrow_build                 19.742   105.6%      420      47.006
      parquet_write                9.709    52.0%      398      24.395
  stage2.total                     2.319    12.4%        1    2319.002
  --------------------------------------------------------------------
  wall clock                      18.687   100.0%
  Stage 1 concurrency: 6.99x (111.075s of work in 15.900s wall)
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
  stage1.wall                     13.317    61.8%        1   13316.889
    window_total                  13.294    61.7%     1001      13.280
      (not broken down)            2.528    11.7%        -           -
      read_block                   2.225    10.3%     1001       2.222
      reshape                      0.062     0.3%     1001       0.062
      reproject                    0.229     1.1%     1001       0.228
      build_frame                  0.184     0.9%     1001       0.183
      dggs_index                   4.275    19.8%     1001       4.270
      arrow_build                  1.874     8.7%     1001       1.872
      parquet_write                1.917     8.9%     1001       1.915
  stage2.total                     6.855    31.8%        1    6854.737
  --------------------------------------------------------------------
  wall clock                      21.541   100.0%
  Stage 1 concurrency: 1.00x (13.294s of work in 13.317s wall)
```
