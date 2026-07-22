from numbers import Number
from typing import Callable, List, Tuple, Union, Optional

import pandas as pd
import pyarrow as pa
import xarray as xr
import numpy as np

from .. import constants as const
from ..histogram import HistogramSpec, build_histogram
from ..interfaces import IRasterIndexer


def _is_nan(v) -> bool:
    try:
        return bool(np.isnan(v))
    except Exception:
        return False


def _col_is_uniform(series: pd.Series) -> bool:
    """Return True if every value in series is identical. Handles unhashable types (e.g. dicts)."""
    try:
        return series.nunique(dropna=False) == 1
    except TypeError:
        first = series.iloc[0]
        return all(v == first for v in series)


def _mask_is_nodata(series: pd.Series, nodata) -> pd.Series:
    """Return boolean mask where True means the pixel is nodata."""
    if nodata is None:
        return pd.Series(False, index=series.index)
    if _is_nan(nodata):
        return series.isna()
    else:
        # Sentinel nodata: also treat unexpected NaNs as nodata
        return series.isna() | (series == nodata)


class RasterIndexer(IRasterIndexer):
    """
    Provides a partial implementation for raster indexers integrating a
        specific DGGS. It should never be instantiated directly because
        many methods raise a NotImplementedError by design. The methods
        should be implemented by the child classes deriving from this
        interface instead.
        If specialised behaviour is required, methods may be
        re-implemented by derived classes.
    """

    def __init__(self, dggs: str):
        """
        Value used across all child classes
        """
        self.dggs = dggs

    def __dask_tokenize__(self):
        """
        Only include stable, immutable fields that define behaviour
        """
        return (type(self).__name__, self.dggs)

    def index_col(self, resolution):
        pad_width = const.zero_padding(self.dggs)
        return f"{self.dggs}_{resolution:0{pad_width}d}"

    def partition_col(self, parent_resolution):
        pad_width = const.zero_padding(self.dggs)
        return f"{self.dggs}_{parent_resolution:0{pad_width}d}"

    def band_cols(self, df: pd.DataFrame):
        return [c for c in df.columns if not c.startswith(f"{self.dggs}_")]

    @staticmethod
    def valid_set(cells: set) -> set:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()

    @staticmethod
    def parent_cells(cells: set, resolution) -> map:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError

    def single_parent_cells(self, cells, resolution) -> map:
        """
        Return exactly one parent cell ID per input cell, for partitioning.

        The default delegates to parent_cells(), which returns one parent per
        cell for most DGGS. Subclasses where parent_cells() may return multiple
        parents per cell (e.g. ISEA3H vertex children have up to 3 parents per
        level) must override this to return exactly one representative parent.
        """
        return self.parent_cells(cells, resolution)

    def expected_count(self, parent: str, resolution: int):
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError

    def index_func(
        self,
        sdf: xr.DataArray,
        resolution: int,
        parent_res: int,
        nodata: Number = np.nan,
        band_labels: Tuple[str] = None,
        nodata_policy: str = "omit",
        emit_nodata_value: Optional[Number] = None,
        transformer=None,
    ) -> pa.Table:
        sdf: pd.DataFrame = (
            sdf.to_dataframe().drop(columns=["spatial_ref"]).reset_index()
        )
        if transformer is not None:
            lons, lats = transformer.transform(sdf["x"].values, sdf["y"].values)
            sdf["x"] = lons
            sdf["y"] = lats
        nodata_mask = _mask_is_nodata(sdf[const.DEFAULT_NAME], nodata)
        if nodata_policy.lower() == "omit":
            sdf = sdf[~nodata_mask].copy()
        elif nodata_policy.lower() == "emit":
            sdf = sdf.copy()
            fill_value = emit_nodata_value if emit_nodata_value is not None else nodata
            if pd.isna(fill_value):
                if pd.api.types.is_integer_dtype(sdf[const.DEFAULT_NAME]):
                    sdf[const.DEFAULT_NAME] = sdf[const.DEFAULT_NAME].astype(float)
                sdf.loc[nodata_mask, const.DEFAULT_NAME] = np.nan
            else:
                dtype = sdf[const.DEFAULT_NAME].dtype
                sdf.loc[nodata_mask, const.DEFAULT_NAME] = dtype.type(fill_value)
        else:
            raise ValueError(f"Unknown nodata policy: {nodata_policy}")
        wide = pd.pivot_table(
            sdf, values=const.DEFAULT_NAME, index=["x", "y"], columns=["band"]
        ).reset_index()
        wide = self._index_window(wide, resolution, parent_res)
        bands = sorted(sdf["band"].unique())
        if band_labels is None:
            band_labels = tuple(str(b) for b in bands)
        wide = wide.rename(columns=dict(zip(bands, band_labels)))
        return pa.Table.from_pandas(wide, preserve_index=False)

    def cells_to_lonlat_arrays(self, cells: pd.Series) -> tuple[np.ndarray, np.ndarray]:
        """
        Return (lons, lats) as numpy arrays for a Series of cell IDs.

        Subclasses should override this to call their DGGS library directly and
        avoid constructing shapely Point objects as intermediaries.
        This fallback delegates to cell_to_point, which every subclass implements.
        """
        pts = [self.cell_to_point(c) for c in cells]
        return np.array([p.x for p in pts]), np.array([p.y for p in pts])

    def _index_window(
        self,
        wide: pd.DataFrame,
        resolution: int,
        parent_res: int,
    ) -> pd.DataFrame:
        """
        Receives a pivoted wide DataFrame with x/y columns and band value columns.
        Must return it with x/y dropped and DGGS cell-index + parent-partition columns added.
        Needs to be implemented by child class.
        """
        raise NotImplementedError()

    def parent_groupby(
        self,
        df: pd.DataFrame,
        resolution: int,
        parent_res: int,
        aggfuncs: List[Tuple[str, Union[str, Callable]]],
        decimals: Optional[int],
    ) -> pd.DataFrame:
        """
        Aggregate DGGS cell values per parent partition.

        aggfuncs is a list of (name, callable_or_str) pairs.
        Single-element list → scalar output per band (existing behaviour).
        Multi-element list → struct output keyed by aggregation name.
        """
        index_col = self.index_col(resolution)
        partition_col = self.partition_col(parent_res)
        df = df.set_index(index_col)

        if len(aggfuncs) == 1:
            _, func = aggfuncs[0]
            agg = df.groupby([partition_col, index_col], sort=False, observed=True).agg(
                func
            )
            if decimals is None:
                gb = agg
            elif decimals > 0:
                # Promote to float64 before rounding: float32 cannot represent
                # most decimal fractions exactly, and integer agg results (e.g.
                # sum/min/max on integer rasters) must also become float when
                # decimal rounding is requested so the data matches the schema.
                to_promote = agg.select_dtypes(include=["float32", "integer"]).columns
                if len(to_promote):
                    agg = agg.astype({c: "float64" for c in to_promote})
                gb = agg.round(decimals)
            else:
                gb = agg.round(decimals).astype("Int64")
            gb = gb.reset_index(level=0)
            gb.index.name = index_col
            return gb
        else:
            # Multi-agg: run each function separately, combine into per-band structs.
            per_agg = {}
            for agg_name, func in aggfuncs:
                r = df.groupby(
                    [partition_col, index_col], sort=False, observed=True
                ).agg(func)
                if decimals is not None:
                    if decimals > 0:
                        to_promote = r.select_dtypes(
                            include=["float32", "integer"]
                        ).columns
                        if len(to_promote):
                            r = r.astype({c: "float64" for c in to_promote})
                    r = r.round(decimals)
                    if decimals <= 0:
                        r = r.astype(
                            {c: "Int64" for c in r.columns if c != partition_col}
                        )
                per_agg[agg_name] = r.reset_index(level=0)

            base = next(iter(per_agg.values()))
            result = pd.DataFrame(
                {partition_col: base[partition_col]}, index=base.index
            )
            result.index.name = index_col
            for col in self.band_cols(base):
                result[col] = [
                    {agg_name: per_agg[agg_name].at[idx, col] for agg_name in per_agg}
                    for idx in result.index
                ]
            return result

    def parent_groupby_nn(
        self,
        df: pd.DataFrame,
        resolution: int,
        parent_res: int,
        decimals: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        For --transfer sample: deduplicate cells that appear in more than one
        window partition. Because each cell's sample pixel belongs to exactly one
        window, all duplicates carry identical values; .first() is sufficient.
        Applies the same decimals rounding/casting as parent_groupby.
        """
        index_col = self.index_col(resolution)
        partition_col = self.partition_col(parent_res)
        df = df.set_index(index_col)
        gb = df.groupby([partition_col, index_col], sort=False, observed=True).first()
        if decimals is None:
            pass
        elif decimals > 0:
            to_promote = gb.select_dtypes(include=["float32", "integer"]).columns
            if len(to_promote):
                gb = gb.astype({c: "float64" for c in to_promote})
            gb = gb.round(decimals)
        else:
            # Only cast numeric columns to Int64 -- object columns (e.g. list/dict
            # values from --overlay list/histogram routed through this dedup path)
            # must be left untouched; .astype("Int64") on the whole frame raises.
            gb = gb.round(decimals)
            to_cast = gb.select_dtypes(
                include=["float32", "float64", "integer"]
            ).columns
            if len(to_cast):
                gb = gb.astype({c: "Int64" for c in to_cast})
        gb = gb.reset_index(level=0)
        gb.index.name = index_col
        return gb

    #: Set to True in subclasses that implement cells_in_bbox.
    SUPPORTS_CELL_ENUMERATION: bool = False

    def cells_in_bbox(
        self,
        min_lon: float,
        min_lat: float,
        max_lon: float,
        max_lat: float,
        resolution: int,
    ) -> set:
        """
        Return cell IDs whose centres fall within the WGS84 bounding box.

        Must be overridden by subclasses that set SUPPORTS_CELL_ENUMERATION = True.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support spatial cell enumeration. "
            "--transfer sample requires cell enumeration; use a DGGS that supports it."
        )

    def _collect_lists(
        self,
        df: pd.DataFrame,
        resolution: int,
        parent_res: int,
    ) -> pd.DataFrame:
        """Group by cell, collecting all contributing pixel values into lists per band."""
        index_col = self.index_col(resolution)
        partition_col = self.partition_col(parent_res)
        df = df.set_index(index_col)
        gb = df.groupby([partition_col, index_col], sort=False, observed=True).agg(list)
        gb = gb.reset_index(level=0)
        gb.index.name = index_col
        return gb

    def parent_groupby_list(
        self,
        df: pd.DataFrame,
        resolution: int,
        parent_res: int,
        decimals: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Collect all contributing pixel values per DGGS cell into lists.
        Used with --out list. Applies rounding element-wise if decimals is not None.
        """
        gb = self._collect_lists(df, resolution, parent_res)
        for col in self.band_cols(gb):
            if decimals is not None and decimals <= 0:
                gb[col] = gb[col].map(
                    lambda lst: sorted(int(round(float(v), decimals)) for v in lst)
                )
            elif decimals is not None:
                gb[col] = gb[col].map(
                    lambda lst: sorted(round(float(v), decimals) for v in lst)
                )
            else:
                gb[col] = gb[col].map(sorted)
        return gb

    def parent_groupby_histogram(
        self,
        df: pd.DataFrame,
        resolution: int,
        parent_res: int,
        decimals: Optional[int] = None,
        hist_spec: Optional[HistogramSpec] = None,
    ) -> pd.DataFrame:
        """
        Collect contributing pixel values per DGGS cell into a histogram.
        Used with --out histogram. Each band column becomes a dict
        {"values": [...], "counts": [...]} -- see
        raster2dggs.histogram.build_histogram for the binning/weighting/
        normalization semantics controlled by hist_spec (None gives an
        exact-value, unweighted histogram).
        """
        gb = self._collect_lists(df, resolution, parent_res)
        cell_areas = None
        if (
            hist_spec is not None
            and hist_spec.normalize == const.HistNormalize.CELL_AREA
        ):
            lons, lats = self.cells_to_lonlat_arrays(pd.Series(gb.index))
            cell_areas = [
                self.cell_area_m2(resolution, lat, lon) for lat, lon in zip(lats, lons)
            ]
        for col in self.band_cols(gb):
            if cell_areas is not None:
                gb[col] = [
                    build_histogram(
                        vals, spec=hist_spec, decimals=decimals, cell_area=area
                    )
                    for vals, area in zip(gb[col], cell_areas)
                ]
            else:
                gb[col] = gb[col].map(
                    lambda vals: build_histogram(
                        vals, spec=hist_spec, decimals=decimals
                    )
                )
        return gb

    @staticmethod
    def cell_to_children_size(cell, desired_resolution: int) -> int:
        """
        Needs to be implemented by child class
        """
        raise NotImplementedError()

    def compaction(
        self, df: pd.DataFrame, resolution: int, parent_res: int
    ) -> pd.DataFrame:
        """
        Returns a compacted version of the input dataframe.
        Compaction only occurs if all values (i.e. bands) of the input
            share common values across all sibling cells.
        Compaction will not be performed beyond parent_res.
        It assumes that the input has unique DGGS cell values
            as the index.
        """
        unprocessed_indices = self.valid_set(set(df.index))
        if not unprocessed_indices:
            return df
        band_cols = self.band_cols(df)
        compaction_map = {}

        for r in range(parent_res, resolution):
            parent_cells = self.parent_cells(unprocessed_indices, r)
            parent_groups = df.loc[list(unprocessed_indices)].groupby(
                list(parent_cells)
            )
            for parent, group in parent_groups:
                if isinstance(parent, tuple) and len(parent) == 1:
                    parent = parent[0]
                if parent in compaction_map:
                    continue
                expected_count = self.expected_count(parent, resolution)
                if len(group) == expected_count and all(
                    _col_is_uniform(group[c]) for c in band_cols
                ):
                    compact_row = group.iloc[0]
                    compact_row.name = parent  # Rename the index to the parent cell
                    compaction_map[parent] = compact_row
                    unprocessed_indices -= set(group.index)
        compacted_df = pd.DataFrame(list(compaction_map.values()))
        remaining_df = df.loc[list(unprocessed_indices)]
        result_df = pd.concat([compacted_df, remaining_df])
        result_df = result_df.rename_axis(df.index.name)
        return result_df
