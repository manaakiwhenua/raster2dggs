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
  stage1.wall                      0.134    50.9%        1     133.505
    window_total                   0.132    50.2%        2      65.755
      read_block                   0.008     3.0%        2       3.980
      reshape                      0.001     0.3%        2       0.332
      reproject                    0.032    12.3%        2      16.167
      build_frame                  0.001     0.2%        2       0.287
      dggs_index                   0.062    23.7%        2      31.029
      arrow_build                  0.003     1.1%        2       1.484
      parquet_write                0.018     7.0%        2       9.193
  stage2.total                     0.027    10.3%        1      27.089
  --------------------------------------------------------------------
  wall clock                       0.262   100.0%
  Stage 1 concurrency: 0.99x (0.132s of work in 0.134s wall)
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
  stage1.wall                      0.517    79.3%        1     516.805
    window_total                   0.515    79.0%        2     257.262
      read_block                   0.008     1.2%        2       3.917
      reshape                      0.001     0.1%        2       0.339
      reproject                    0.038     5.8%        2      18.907
      build_frame                  0.001     0.1%        2       0.347
      dggs_index                   0.439    67.3%        2     219.286
      arrow_build                  0.003     0.5%        2       1.601
      parquet_write                0.019     2.9%        2       9.469
  stage2.total                     0.028     4.4%        1      28.424
  --------------------------------------------------------------------
  wall clock                       0.652   100.0%
  Stage 1 concurrency: 1.00x (0.515s of work in 0.517s wall)
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
  stage1.wall                      0.935    88.2%        1     935.122
    window_total                   0.933    88.0%        2     466.468
      read_block                   0.007     0.7%        2       3.631
      reshape                      0.001     0.1%        2       0.351
      reproject                    0.034     3.2%        2      17.074
      build_frame                  0.001     0.1%        2       0.271
      dggs_index                   0.863    81.4%        2     431.649
      arrow_build                  0.003     0.3%        2       1.462
      parquet_write                0.018     1.7%        2       9.179
  stage2.total                     0.028     2.6%        1      27.506
  --------------------------------------------------------------------
  wall clock                       1.060   100.0%
  Stage 1 concurrency: 1.00x (0.933s of work in 0.935s wall)
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
  stage1.wall                      1.729    91.7%        1    1728.678
    window_total                   1.726    91.6%        2     863.074
      read_block                   0.009     0.5%        2       4.311
      reshape                      0.001     0.0%        2       0.432
      reproject                    0.034     1.8%        2      17.175
      build_frame                  0.001     0.0%        2       0.311
      dggs_index                   1.651    87.5%        2     825.251
      arrow_build                  0.004     0.2%        2       1.857
      parquet_write                0.020     1.1%        2      10.182
  stage2.total                     0.033     1.8%        1      33.282
  --------------------------------------------------------------------
  wall clock                       1.885   100.0%
  Stage 1 concurrency: 1.00x (1.726s of work in 1.729s wall)
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
  stage1.wall                      3.955    93.6%        1    3955.323
    window_total                   3.953    93.6%        2    1976.506
      read_block                   0.008     0.2%        2       3.940
      reshape                      0.001     0.0%        2       0.455
      reproject                    0.036     0.8%        2      17.942
      build_frame                  0.001     0.0%        2       0.309
      dggs_index                   3.880    91.8%        2    1940.000
      arrow_build                  0.003     0.1%        2       1.616
      parquet_write                0.019     0.5%        2       9.527
  stage2.total                     0.026     0.6%        1      25.634
  --------------------------------------------------------------------
  wall clock                       4.225   100.0%
  Stage 1 concurrency: 1.00x (3.953s of work in 3.955s wall)
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
  stage1.wall                      0.634    81.0%        1     633.591
    window_total                   0.631    80.7%        2     315.610
      read_block                   0.008     1.0%        2       4.002
      reshape                      0.001     0.1%        2       0.480
      reproject                    0.033     4.2%        2      16.620
      build_frame                  0.001     0.1%        2       0.311
      dggs_index                   0.561    71.6%        2     280.351
      arrow_build                  0.003     0.4%        2       1.703
      parquet_write                0.019     2.4%        2       9.382
  stage2.total                     0.027     3.4%        1      26.791
  --------------------------------------------------------------------
  wall clock                       0.783   100.0%
  Stage 1 concurrency: 1.00x (0.631s of work in 0.634s wall)
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
  stage1.wall                      0.239    60.5%        1     239.145
    window_total                   0.237    59.9%        2     118.425
      read_block                   0.008     2.1%        2       4.118
      reshape                      0.001     0.2%        2       0.423
      reproject                    0.036     9.1%        2      18.070
      build_frame                  0.001     0.2%        2       0.334
      dggs_index                   0.152    38.3%        2      75.788
      arrow_build                  0.004     1.1%        2       2.147
      parquet_write                0.029     7.3%        2      14.524
  stage2.total                     0.045    11.3%        1      44.511
  --------------------------------------------------------------------
  wall clock                       0.395   100.0%
  Stage 1 concurrency: 0.99x (0.237s of work in 0.239s wall)
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
  stage1.wall                      0.236    65.6%        1     236.207
    window_total                   0.234    65.0%        2     117.006
      (not broken down)            0.046    12.7%        -           -
      cells_in_bbox                0.002     0.5%        2       0.847
      cell_polygons                0.016     4.5%        2       8.149
      exactextract                 0.168    46.7%        2      83.985
      parquet_write                0.002     0.7%        2       1.193
  stage2.total                     0.018     4.9%        1      17.569
  --------------------------------------------------------------------
  wall clock                       0.360   100.0%
  Stage 1 concurrency: 0.99x (0.234s of work in 0.236s wall)
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
  stage1.wall                      0.157    51.3%        1     157.162
    window_total                   0.155    50.6%        2      77.520
      (not broken down)            0.061    20.1%        -           -
      read_block                   0.012     3.8%        2       5.889
      cells_in_bbox                0.030     9.8%        2      15.020
      cells_to_lonlat              0.038    12.5%        2      19.110
      parquet_write                0.014     4.4%        2       6.770
  stage2.total                     0.029     9.5%        1      29.137
  --------------------------------------------------------------------
  wall clock                       0.306   100.0%
  Stage 1 concurrency: 0.99x (0.155s of work in 0.157s wall)
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
  stage1.wall                     23.306    91.9%        1   23305.771
    window_total                  23.286    91.9%      760      30.640
      (not broken down)            1.844     7.3%        -           -
      read_block                   1.854     7.3%      760       2.440
      reshape                      0.241     1.0%      760       0.317
      reproject                    1.897     7.5%      760       2.496
      build_frame                  0.180     0.7%      760       0.237
      dggs_index                  15.309    60.4%      760      20.143
      arrow_build                  0.424     1.7%      760       0.558
      parquet_write                1.536     6.1%      227       6.765
  stage2.total                     1.855     7.3%        1    1855.179
  --------------------------------------------------------------------
  wall clock                      25.348   100.0%
  Stage 1 concurrency: 1.00x (23.286s of work in 23.306s wall)
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
  stage1.wall                     22.401    91.8%        1   22401.029
    window_total                 156.605   641.4%      760     206.060
      (not broken down)           10.600    43.4%        -           -
      read_block                  33.854   138.7%      760      44.545
      reshape                     20.700    84.8%      760      27.237
      reproject                    6.286    25.7%      760       8.271
      build_frame                  1.442     5.9%      760       1.898
      dggs_index                  63.495   260.1%      760      83.546
      arrow_build                 13.086    53.6%      760      17.218
      parquet_write                7.142    29.3%      227      31.461
  stage2.total                     1.824     7.5%        1    1823.798
  --------------------------------------------------------------------
  wall clock                      24.414   100.0%
  Stage 1 concurrency: 6.99x (156.605s of work in 22.401s wall)
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
  stage1.wall                     32.948    98.7%        1   32947.688
    window_total                 230.181   689.9%      760     302.870
      (not broken down)           49.109   147.2%        -           -
      cells_in_bbox                0.214     0.6%      760       0.281
      cell_polygons               20.741    62.2%      760      27.291
      exactextract               156.570   469.2%      760     206.013
      parquet_write                3.548    10.6%      291      12.192
  stage2.total                     0.238     0.7%        1     237.937
  --------------------------------------------------------------------
  wall clock                      33.367   100.0%
  Stage 1 concurrency: 6.99x (230.181s of work in 32.948s wall)
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
  stage1.wall                      6.418    95.3%        1    6418.307
    window_total                  44.324   658.1%      760      58.322
      (not broken down)           10.947   162.5%        -           -
      read_block                  30.887   458.6%      760      40.641
      cells_in_bbox                0.132     2.0%      760       0.174
      cells_to_lonlat              0.247     3.7%      760       0.326
      parquet_write                2.111    31.3%      207      10.198
  stage2.total                     0.158     2.4%        1     158.278
  --------------------------------------------------------------------
  wall clock                       6.735   100.0%
  Stage 1 concurrency: 6.91x (44.324s of work in 6.418s wall)
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
  stage1.wall                     18.517    86.2%        1   18517.264
    window_total                  18.502    86.1%      420      44.052
      (not broken down)            1.272     5.9%        -           -
      read_block                   1.500     7.0%      420       3.571
      reshape                      0.192     0.9%      420       0.456
      reproject                    1.306     6.1%      420       3.109
      build_frame                  0.150     0.7%      420       0.357
      dggs_index                  10.317    48.0%      420      24.565
      arrow_build                  1.164     5.4%      420       2.771
      parquet_write                2.602    12.1%      398       6.537
  stage2.total                     2.499    11.6%        1    2499.308
  --------------------------------------------------------------------
  wall clock                      21.491   100.0%
  Stage 1 concurrency: 1.00x (18.502s of work in 18.517s wall)
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
  stage1.wall                     17.390    86.0%        1   17390.487
    window_total                 121.432   600.7%      420     289.125
      (not broken down)            8.060    39.9%        -           -
      read_block                  16.992    84.1%      420      40.457
      reshape                     17.014    84.2%      420      40.510
      reproject                    4.715    23.3%      420      11.225
      build_frame                 11.788    58.3%      420      28.066
      dggs_index                  32.000   158.3%      420      76.190
      arrow_build                 21.019   104.0%      420      50.046
      parquet_write                9.845    48.7%      398      24.737
  stage2.total                     2.389    11.8%        1    2389.404
  --------------------------------------------------------------------
  wall clock                      20.216   100.0%
  Stage 1 concurrency: 6.98x (121.432s of work in 17.390s wall)
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
  stage1.wall                     15.492    55.5%        1   15492.097
    window_total                  15.465    55.4%     1001      15.449
      (not broken down)            2.863    10.3%        -           -
      read_block                   2.505     9.0%     1001       2.503
      reshape                      0.069     0.2%     1001       0.069
      reproject                    0.254     0.9%     1001       0.253
      build_frame                  0.203     0.7%     1001       0.203
      dggs_index                   5.067    18.2%     1001       5.062
      arrow_build                  2.105     7.5%     1001       2.103
      parquet_write                2.398     8.6%     1001       2.396
  stage2.total                    10.762    38.5%        1   10762.126
  --------------------------------------------------------------------
  wall clock                      27.919   100.0%
  Stage 1 concurrency: 1.00x (15.465s of work in 15.492s wall)
```
