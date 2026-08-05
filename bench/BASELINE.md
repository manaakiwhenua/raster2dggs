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
  stage1.wall                      0.207    58.6%        1     207.384
    window_total                   0.205    58.0%        2     102.573
      read_block                   0.008     2.1%        2       3.773
      reshape                      0.001     0.2%        2       0.379
      reproject                    0.032     9.0%        2      16.008
      build_frame                  0.001     0.2%        2       0.266
      dggs_index                   0.133    37.5%        2      66.356
      arrow_build                  0.003     1.0%        2       1.721
      parquet_write                0.022     6.3%        2      11.073
  stage2.total                     0.041    11.5%        1      40.829
  --------------------------------------------------------------------
  wall clock                       0.354   100.0%
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
  stage1.wall                      0.260    60.4%        1     259.937
    window_total                   0.258    59.9%        2     128.765
      (not broken down)            0.051    11.9%        -           -
      cells_in_bbox                0.002     0.6%        2       1.188
      cell_polygons                0.021     4.9%        2      10.468
      exactextract                 0.180    41.8%        2      90.025
      parquet_write                0.003     0.6%        2       1.381
  stage2.total                     0.026     6.1%        1      26.383
  --------------------------------------------------------------------
  wall clock                       0.430   100.0%
  Stage 1 concurrency: 0.99x (0.258s of work in 0.260s wall)
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
  stage1.wall                      0.147    50.3%        1     147.339
    window_total                   0.145    49.5%        2      72.515
      (not broken down)            0.063    21.7%        -           -
      read_block                   0.011     3.8%        2       5.497
      cells_in_bbox                0.026     8.8%        2      12.871
      cells_to_lonlat              0.034    11.7%        2      17.108
      parquet_write                0.011     3.6%        2       5.309
  stage2.total                     0.033    11.4%        1      33.353
  --------------------------------------------------------------------
  wall clock                       0.293   100.0%
  Stage 1 concurrency: 0.98x (0.145s of work in 0.147s wall)
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
  stage1.wall                     22.722    92.1%        1   22721.536
    window_total                  22.703    92.0%      760      29.872
      (not broken down)            1.765     7.2%        -           -
      read_block                   1.780     7.2%      760       2.342
      reshape                      0.220     0.9%      760       0.289
      reproject                    1.824     7.4%      760       2.400
      build_frame                  0.172     0.7%      760       0.226
      dggs_index                  15.053    61.0%      760      19.807
      arrow_build                  0.414     1.7%      760       0.545
      parquet_write                1.475     6.0%      227       6.496
  stage2.total                     1.777     7.2%        1    1777.288
  --------------------------------------------------------------------
  wall clock                      24.668   100.0%
  Stage 1 concurrency: 1.00x (22.703s of work in 22.722s wall)
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
  stage1.wall                     21.593    91.8%        1   21592.852
    window_total                 150.976   642.1%      760     198.652
      (not broken down)           10.118    43.0%        -           -
      read_block                  31.445   133.7%      760      41.375
      reshape                     19.410    82.5%      760      25.540
      reproject                    6.493    27.6%      760       8.543
      build_frame                  1.489     6.3%      760       1.959
      dggs_index                  60.694   258.1%      760      79.861
      arrow_build                 14.677    62.4%      760      19.311
      parquet_write                6.650    28.3%      227      29.294
  stage2.total                     1.738     7.4%        1    1737.562
  --------------------------------------------------------------------
  wall clock                      23.513   100.0%
  Stage 1 concurrency: 6.99x (150.976s of work in 21.593s wall)
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
  stage1.wall                     31.379    98.8%        1   31378.615
    window_total                 219.287   690.4%      760     288.536
      (not broken down)           46.757   147.2%        -           -
      cells_in_bbox                0.203     0.6%      760       0.267
      cell_polygons               19.794    62.3%      760      26.044
      exactextract               149.000   469.1%      760     196.052
      parquet_write                3.534    11.1%      291      12.145
  stage2.total                     0.225     0.7%        1     224.837
  --------------------------------------------------------------------
  wall clock                      31.762   100.0%
  Stage 1 concurrency: 6.99x (219.287s of work in 31.379s wall)
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
  stage1.wall                      6.073    95.4%        1    6073.144
    window_total                  41.911   658.3%      760      55.146
      (not broken down)            9.850   154.7%        -           -
      read_block                  29.746   467.2%      760      39.140
      cells_in_bbox                0.128     2.0%      760       0.169
      cells_to_lonlat              0.313     4.9%      760       0.412
      parquet_write                1.874    29.4%      207       9.051
  stage2.total                     0.151     2.4%        1     151.031
  --------------------------------------------------------------------
  wall clock                       6.367   100.0%
  Stage 1 concurrency: 6.90x (41.911s of work in 6.073s wall)
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
  stage1.wall                     17.280    86.3%        1   17280.072
    window_total                  17.266    86.3%      420      41.109
      (not broken down)            1.165     5.8%        -           -
      read_block                   1.443     7.2%      420       3.435
      reshape                      0.181     0.9%      420       0.432
      reproject                    1.235     6.2%      420       2.940
      build_frame                  0.136     0.7%      420       0.323
      dggs_index                   9.498    47.4%      420      22.615
      arrow_build                  1.149     5.7%      420       2.735
      parquet_write                2.459    12.3%      398       6.178
  stage2.total                     2.315    11.6%        1    2315.257
  --------------------------------------------------------------------
  wall clock                      20.018   100.0%
  Stage 1 concurrency: 1.00x (17.266s of work in 17.280s wall)
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
  stage1.wall                     15.671    85.3%        1   15670.974
    window_total                 109.400   595.1%      420     260.476
      (not broken down)            8.192    44.6%        -           -
      read_block                  18.659   101.5%      420      44.426
      reshape                     14.452    78.6%      420      34.410
      reproject                    4.295    23.4%      420      10.227
      build_frame                 10.044    54.6%      420      23.914
      dggs_index                  27.754   151.0%      420      66.080
      arrow_build                 16.068    87.4%      420      38.257
      parquet_write                9.936    54.1%      398      24.965
  stage2.total                     2.295    12.5%        1    2295.158
  --------------------------------------------------------------------
  wall clock                      18.382   100.0%
  Stage 1 concurrency: 6.98x (109.400s of work in 15.671s wall)
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
  stage1.wall                     12.825    61.6%        1   12824.786
    window_total                  12.801    61.5%     1001      12.788
      (not broken down)            2.453    11.8%        -           -
      read_block                   2.104    10.1%     1001       2.102
      reshape                      0.063     0.3%     1001       0.063
      reproject                    0.228     1.1%     1001       0.228
      build_frame                  0.183     0.9%     1001       0.183
      dggs_index                   4.281    20.6%     1001       4.277
      arrow_build                  1.708     8.2%     1001       1.707
      parquet_write                1.780     8.6%     1001       1.778
  stage2.total                     6.620    31.8%        1    6619.941
  --------------------------------------------------------------------
  wall clock                      20.815   100.0%
  Stage 1 concurrency: 1.00x (12.801s of work in 12.825s wall)
```
