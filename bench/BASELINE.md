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

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                      0.256    64.5%        1     255.875
    window_total                   0.254    64.0%        2     126.916
      read_block                   0.008     2.0%        2       3.939
      reproject                    0.033     8.4%        2      16.667
      reshape                      0.001     0.2%        2       0.354
      dggs_index                   0.182    45.8%        2      90.851
      arrow_build                  0.003     0.8%        2       1.557
      parquet_write                0.021     5.2%        2      10.339
  stage2.total                     0.039     9.7%        1      38.583
  --------------------------------------------------------------------
  wall clock                       0.397   100.0%
  Stage 1 concurrency: 0.99x (0.254s of work in 0.256s wall)
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
  stage1.wall                      0.238    66.1%        1     237.508
    window_total                   0.235    65.5%        2     117.651
      (not broken down)            0.043    12.0%        -           -
      cells_in_bbox                0.001     0.3%        2       0.616
      cell_polygons                0.017     4.6%        2       8.299
      exactextract                 0.172    47.9%        2      85.997
      parquet_write                0.002     0.7%        2       1.249
  stage2.total                     0.019     5.2%        1      18.543
  --------------------------------------------------------------------
  wall clock                       0.359   100.0%
  Stage 1 concurrency: 0.99x (0.235s of work in 0.238s wall)
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
  stage1.wall                      0.145    50.8%        1     144.961
    window_total                   0.143    50.1%        2      71.441
      (not broken down)            0.060    21.1%        -           -
      read_block                   0.012     4.1%        2       5.905
      cells_in_bbox                0.024     8.6%        2      12.199
      cells_to_lonlat              0.035    12.4%        2      17.636
      parquet_write                0.011     3.9%        2       5.594
  stage2.total                     0.029    10.2%        1      29.023
  --------------------------------------------------------------------
  wall clock                       0.285   100.0%
  Stage 1 concurrency: 0.99x (0.143s of work in 0.145s wall)
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

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     36.896    95.0%        1   36896.057
    window_total                  36.877    95.0%      760      48.522
      (not broken down)            1.885     4.9%        -           -
      read_block                   1.817     4.7%      760       2.391
      reproject                    9.182    23.6%      760      12.082
      reshape                      0.489     1.3%      760       0.644
      dggs_index                  21.598    55.6%      760      28.419
      arrow_build                  0.420     1.1%      760       0.553
      parquet_write                1.485     3.8%      227       6.541
  stage2.total                     1.771     4.6%        1    1770.996
  --------------------------------------------------------------------
  wall clock                      38.836   100.0%
  Stage 1 concurrency: 1.00x (36.877s of work in 36.896s wall)
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

  phase                          seconds   % wall    calls     ms/call
  --------------------------------------------------------------------
  stage1.wall                     28.860    93.7%        1   28860.094
    window_total                 201.848   655.3%      760     265.590
      (not broken down)           10.768    35.0%        -           -
      read_block                  31.460   102.1%      760      41.395
      reproject                   21.940    71.2%      760      28.869
      reshape                     32.090   104.2%      760      42.224
      dggs_index                  73.310   238.0%      760      96.461
      arrow_build                 23.929    77.7%      760      31.486
      parquet_write                8.350    27.1%      227      36.783
  stage2.total                     1.769     5.7%        1    1769.435
  --------------------------------------------------------------------
  wall clock                      30.804   100.0%
  Stage 1 concurrency: 6.99x (201.848s of work in 28.860s wall)
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
  stage1.wall                     31.088    98.8%        1   31087.826
    window_total                 217.260   690.6%      760     285.869
      (not broken down)           46.834   148.9%        -           -
      cells_in_bbox                0.201     0.6%      760       0.265
      cell_polygons               20.156    64.1%      760      26.520
      exactextract               146.668   466.2%      760     192.985
      parquet_write                3.401    10.8%      291      11.687
  stage2.total                     0.214     0.7%        1     213.614
  --------------------------------------------------------------------
  wall clock                      31.461   100.0%
  Stage 1 concurrency: 6.99x (217.260s of work in 31.088s wall)
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
  stage1.wall                      6.032    95.4%        1    6031.738
    window_total                  41.659   658.8%      760      54.815
      (not broken down)            9.551   151.0%        -           -
      read_block                  29.878   472.5%      760      39.314
      cells_in_bbox                0.126     2.0%      760       0.166
      cells_to_lonlat              0.223     3.5%      760       0.294
      parquet_write                1.880    29.7%      207       9.083
  stage2.total                     0.151     2.4%        1     151.124
  --------------------------------------------------------------------
  wall clock                       6.323   100.0%
  Stage 1 concurrency: 6.91x (41.659s of work in 6.032s wall)
```
