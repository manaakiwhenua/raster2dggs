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
  stage1.wall                      0.533    76.4%        1     533.038
    window_total                   0.531    76.0%        2     265.344
      read_block                   0.126    18.0%        2      62.769
      reproject                    0.079    11.4%        2      39.725
      reshape                      0.081    11.6%        2      40.546
      dggs_index                   0.198    28.3%        2      98.920
      arrow_build                  0.004     0.5%        2       1.839
      parquet_write                0.027     3.8%        2      13.271
  stage2.total                     0.039     5.6%        1      39.092
  --------------------------------------------------------------------
  wall clock                       0.698   100.0%
  Stage 1 concurrency: 1.00x (0.531s of work in 0.533s wall)
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
  stage1.wall                      0.246    65.6%        1     245.991
    window_total                   0.244    65.0%        2     121.943
      (not broken down)            0.046    12.3%        -           -
      cells_in_bbox                0.002     0.5%        2       0.860
      cell_polygons                0.017     4.5%        2       8.465
      exactextract                 0.177    47.1%        2      88.370
      parquet_write                0.003     0.7%        2       1.254
  stage2.total                     0.021     5.6%        1      21.011
  --------------------------------------------------------------------
  wall clock                       0.375   100.0%
  Stage 1 concurrency: 0.99x (0.244s of work in 0.246s wall)
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
  stage1.wall                      0.170    52.7%        1     170.190
    window_total                   0.168    52.0%        2      84.096
      (not broken down)            0.075    23.2%        -           -
      read_block                   0.013     4.1%        2       6.592
      cells_in_bbox                0.030     9.3%        2      14.968
      cells_to_lonlat              0.038    11.9%        2      19.196
      parquet_write                0.012     3.7%        2       5.913
  stage2.total                     0.031     9.7%        1      31.413
  --------------------------------------------------------------------
  wall clock                       0.323   100.0%
  Stage 1 concurrency: 0.99x (0.168s of work in 0.170s wall)
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
  stage1.wall                     62.524    96.9%        1   62524.361
    window_total                  62.504    96.9%      760      82.242
      read_block                  17.774    27.5%      760      23.387
      reproject                    9.701    15.0%      760      12.764
      reshape                      6.638    10.3%      760       8.735
      dggs_index                  23.680    36.7%      760      31.157
      arrow_build                  0.380     0.6%      760       0.500
      parquet_write                1.621     2.5%      227       7.140
  stage2.total                     1.811     2.8%        1    1811.463
  --------------------------------------------------------------------
  wall clock                      64.528   100.0%
  Stage 1 concurrency: 1.00x (62.504s of work in 62.524s wall)
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
  stage1.wall                     51.684    96.5%        1   51684.492
    window_total                 361.389   674.8%      760     475.512
      (not broken down)           25.847    48.3%        -           -
      read_block                 125.135   233.7%      760     164.651
      reproject                   29.304    54.7%      760      38.558
      reshape                    110.479   206.3%      760     145.367
      dggs_index                  53.670   100.2%      760      70.618
      arrow_build                 10.907    20.4%      760      14.352
      parquet_write                6.047    11.3%      227      26.637
  stage2.total                     1.697     3.2%        1    1696.975
  --------------------------------------------------------------------
  wall clock                      53.555   100.0%
  Stage 1 concurrency: 6.99x (361.389s of work in 51.684s wall)
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
  stage1.wall                     30.550    98.8%        1   30550.269
    window_total                 213.486   690.5%      760     280.903
      (not broken down)           45.883   148.4%        -           -
      cells_in_bbox                0.203     0.7%      760       0.267
      cell_polygons               19.520    63.1%      760      25.684
      exactextract               144.374   467.0%      760     189.966
      parquet_write                3.506    11.3%      291      12.048
  stage2.total                     0.215     0.7%        1     215.203
  --------------------------------------------------------------------
  wall clock                      30.918   100.0%
  Stage 1 concurrency: 6.99x (213.486s of work in 30.550s wall)
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
  stage1.wall                      6.008    95.2%        1    6007.500
    window_total                  41.507   658.0%      760      54.615
      (not broken down)            9.540   151.2%        -           -
      read_block                  29.666   470.3%      760      39.035
      cells_in_bbox                0.124     2.0%      760       0.163
      cells_to_lonlat              0.265     4.2%      760       0.349
      parquet_write                1.912    30.3%      207       9.235
  stage2.total                     0.149     2.4%        1     148.551
  --------------------------------------------------------------------
  wall clock                       6.308   100.0%
  Stage 1 concurrency: 6.91x (41.507s of work in 6.008s wall)
```
