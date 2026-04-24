"""
@author: alpha-beta-soup
"""

from abc import abstractmethod
from functools import lru_cache
from typing import List

import dggal
import pandas as pd
import shapely

from raster2dggs.indexers.rasterindexer import RasterIndexer

# Instantiate DGGAL
dggal.pydggal_setup(dggal.Application(appGlobals=globals()))


DGGAL_NULL_ZONE: int = 2**64 - 1


class DGGALRasterIndexer(RasterIndexer):
    """
    Provides integration for DGGRSs depending on the DGGAL API.
    """

    @property
    @abstractmethod
    def dggrs(self) -> dggal.DGGRS:
        raise NotImplementedError

    @lru_cache(maxsize=None)
    def _get_parent(self, zone: int) -> int | None:
        """
        Get immediate parent with caching.
        Used recursively, the LRU cache will naturally evict leaf cells which don't benefit from caching.
        """
        # NB  All zones of GNOSIS Global Grid and *9R have single parents, whereas *3H zones have one parent if they are a centroid child, and three parents otherwise if they are a vertex child.
        # *7H zones have one parent "logically" but possibly more "geometrically". *7H zones are considered to have only one parent for the purposes of this tool, which conforms to the same assumption as H3, which has a refinement ratio of 7.
        # See dggrs.getMaxParents()
        if self.dggs in ("isea3h", "ivea3h", "rtea3h"):
            centroid_parent = self.dggrs.getZoneCentroidParent(zone)
            if centroid_parent != DGGAL_NULL_ZONE:
                return centroid_parent

        parents = self.dggrs.getZoneParents(zone)
        parent = next(
            (p for p in parents if self.dggrs.isZoneCentroidChild(zone)), parents[0]
        )
        return int(parent)

    def _get_ancestor(self, zone: int, levels_up: int = 1) -> int:
        """Get ancestor by repeatedly calling cached parent lookup."""
        parent = zone
        for _ in range(levels_up):
            parent = self._get_parent(int(parent))
        return parent

    def _index_window(self, wide, resolution: int, parent_res: int):
        cells = [
            self.dggrs.getZoneFromWGS84Centroid(resolution, dggal.GeoPoint(lon, lat))
            for lon, lat in zip(wide["y"], wide["x"])
        ]  # Vectorised
        dggrs_parent = [
            self._get_ancestor(zone, resolution - parent_res) for zone in cells
        ]
        wide = wide.drop(columns=["x", "y"])
        wide[self.index_col(resolution)] = pd.Series(
            map(self.dggrs.getZoneTextID, cells), index=wide.index
        )
        wide[self.partition_col(parent_res)] = pd.Series(
            map(self.dggrs.getZoneTextID, dggrs_parent), index=wide.index
        )
        return wide

    def cell_to_children_size(self, cell: str, desired_resolution: int) -> int:
        """
        Determine total number of children at some offset resolution

        Implementation of interface function.
        """
        zone = self.dggrs.getZoneFromTextID(cell)
        current_resolution = self.dggrs.getZoneLevel(zone)
        depth = desired_resolution - current_resolution

        if self.dggs in ("isea3h", "ivea3h", "rtea3h"):
            return self.dggrs.countSubZones(zone, depth)

        return self.dggrs.getRefinementRatio() ** depth

    @staticmethod
    def valid_set(cells: set) -> set[str]:
        """
        Implementation of interface function.
        """
        return set(filter(lambda c: (not pd.isna(c)), cells))

    def parent_cells(self, cells: set, resolution: int) -> map:
        """
        Implementation of interface function.
        """
        child_resolution = self.dggrs.getZoneLevel(
            self.dggrs.getZoneFromTextID(next(iter(cells)))
        )
        relative_depth = child_resolution - resolution

        if self.dggs in ("isea3h", "ivea3h", "rtea3h"):
            all_parents = []
            for zone_text in cells:
                zone = self.dggrs.getZoneFromTextID(zone_text)
                # Get ancestors via all parent paths
                ancestors = self._get_all_ancestors_3h(zone, relative_depth)
                all_parents.extend(self.dggrs.getZoneTextID(a) for a in ancestors)
            return all_parents

        return map(
            lambda zone: self.dggrs.getZoneTextID(
                self._get_ancestor(self.dggrs.getZoneFromTextID(zone), relative_depth)
            ),
            cells,
        )

    def _get_all_ancestors_3h(self, zone: int, levels_up: int) -> set:
        """
        For 3H DGGRS, get all ancestors at specified level via all parent paths.
        A vertex child has 3 parents, so this returns up to 3^levels_up ancestors.
        """
        if levels_up == 0:
            return {zone}

        # Get immediate parents
        parents = map(int, self.dggrs.getZoneParents(zone))

        if levels_up == 1:
            return set(parents)

        # Recursively get ancestors through all parent paths
        all_ancestors = set()
        for parent in parents:
            ancestors = self._get_all_ancestors_3h(parent, levels_up - 1)
            all_ancestors.update(ancestors)

        return all_ancestors

    def expected_count(self, parent: str, resolution: int):
        """
        Implementation of interface function.
        """
        return self.cell_to_children_size(parent, resolution)

    def cell_to_point(self, cell: str) -> shapely.geometry.Point:
        geo_point: dggal.GeoPoint = self.dggrs.getZoneWGS84Centroid(
            self.dggrs.getZoneFromTextID(cell)
        )
        return shapely.Point(geo_point.lon, geo_point.lat)

    def cell_to_polygon(
        self, cell: str, edgeRefinement: int = 0
    ) -> shapely.geometry.Polygon:
        geo_points: List[dggal.GeoPoint] = self.dggrs.getZoneRefinedWGS84Vertices(
            self.dggrs.getZoneFromTextID(cell), edgeRefinement
        )
        return shapely.Polygon(tuple([(p.lon, p.lat) for p in geo_points]))

    def compaction(
        self, df: pd.DataFrame, resolution: int, parent_res: int
    ) -> pd.DataFrame:
        if self.dggs in ("isea3h", "ivea3h", "rtea3h"):
            return self._compaction_3h(df, resolution, parent_res)
        return super().compaction(df, resolution, parent_res)

    def _compaction_3h(
        self, df: pd.DataFrame, resolution: int, parent_res: int
    ) -> pd.DataFrame:
        """
        Compact cells with identical values for multi-parent DGGRS (refinement ratio 3).

        For hexagonal grids with refinement ratio 3 (ISEA3H, IVEA3H, RTEA3H), vertex
        children have multiple parents in the hierarchy.
        The same logic should apply for the *4H case.

        This method performs bottom-up compaction where:

        1. Cells are grouped by their band values
        2. For each level from finest (resolution) to coarsest (parent_res):
            - A parent cell replaces its children if ALL children are present and share
              identical values across all bands
        3. A child cell is removed only if ALL of its parents successfully compact
        4. Vertex children can contribute to multiple parent compactions simultaneously

        This approach respects the multi-parent topology while maximizing data reduction,
        ensuring that regions with homogeneous values are represented at the coarsest
        appropriate level.

        Args:
            df: DataFrame indexed by fine-resolution cell IDs with band value columns
            resolution: Finest resolution level of input cells
            parent_res: Coarsest resolution level to compact to (lower limit)

        Returns:
            DataFrame with compacted cells at mixed resolutions (between parent_res and
            resolution), maintaining the fine-resolution column as index and including
            the parent_res ancestor for partitioning.

        Note:
            For single-parent DGGRS, use the standard compaction method.
        """

        band_cols = self.band_cols(df)
        index_col = self.index_col(resolution)
        partition_col = self.partition_col(parent_res)

        @lru_cache(maxsize=100000)
        def get_level(cell_id: str) -> int:
            return self.dggrs.getZoneLevel(self.dggrs.getZoneFromTextID(cell_id))

        @lru_cache(maxsize=100000)
        def get_parents(cell_id: str) -> tuple:
            zone = self.dggrs.getZoneFromTextID(cell_id)
            return tuple(
                self.dggrs.getZoneTextID(p) for p in self.dggrs.getZoneParents(zone)
            )

        @lru_cache(maxsize=10000)
        def get_child_count(parent_id: str) -> int:
            zone = self.dggrs.getZoneFromTextID(parent_id)
            return self.dggrs.countSubZones(zone, 1)

        # Initialise cell data and active set
        cell_data = {
            cid: df.loc[cid, [partition_col] + band_cols].to_dict() for cid in df.index
        }
        active_cells = set(df.index)

        # Compact level by level from fine to coarse
        for current_level in range(resolution, parent_res, -1):
            level_cells = {
                cid for cid in active_cells if get_level(cid) == current_level
            }

            if not level_cells:
                continue

            # Build parent->children and child->parents mappings
            parent_groups = {}
            child_to_parents = {}

            for child_id in level_cells:
                parents = get_parents(child_id)
                child_to_parents[child_id] = parents

                for parent_id in parents:
                    parent_groups.setdefault(parent_id, []).append(child_id)

            # Identify compactable parents (all children present with equal values)
            compactable = {}
            for parent_id, children in parent_groups.items():
                if len(children) != get_child_count(parent_id):
                    continue

                # Check if all band values are qual across children
                first_data = cell_data[children[0]]
                if all(
                    all(cell_data[c][band] == first_data[band] for c in children)
                    for band in band_cols
                ):
                    # compactable[parent_id] = (children, first_data.copy())
                    compactable[parent_id] = first_data.copy()

            # Remove children where ALL parents compact
            children_to_remove = {
                child
                for child, parents in child_to_parents.items()
                if all(parent in compactable for parent in parents)
            }

            # Update active cells
            active_cells -= children_to_remove
            for child_id in children_to_remove:
                del cell_data[child_id]

            # Add compacted parents
            for parent_id, data in compactable.items():
                cell_data[parent_id] = data
                active_cells.add(parent_id)

        # Build result dataframe with both fine and parent resolution columns
        if not active_cells:
            return pd.DataFrame(
                columns=[index_col, partition_col] + band_cols
            ).set_index(index_col)

        result_data = [
            {
                index_col: cid,
                **cell_data[cid],
            }
            for cid in active_cells
        ]

        return pd.DataFrame(result_data).set_index(index_col)


class ISEA4RRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA4R DGGS, an equal area rhombic grid with a refinement ratio of 4 defined in the ISEA projection transformed into a 5x6 Cartesian space resulting in axis-aligned square zones.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.ISEA4R()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class IVEA4RRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the IVEA4R DGGS, an equal area rhombic grid with a refinement ratio of 4 defined in the IVEA projection transformed into a 5x6 Cartesian space resulting in axis-aligned square zones, using the same global indexing and sub-zone ordering as for ISEA4R.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.IVEA4R()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class ISEA9RRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA9R DGGS, an equal area rhombic grid with a refinement ratio of 9 defined in the ISEA projection transformed into a 5x6 Cartesian space resulting in axis-aligned square zones.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.ISEA9R()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class IVEA9RRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA9R DGGS, an equal area rhombic grid with a refinement ratio of 9 defined in the IVEA projection transformed into a 5x6 Cartesian space resulting in axis-aligned square zones.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.IVEA9R()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class ISEA3HRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA3H DGGS, an equal area hexagonal grid with a refinement ratio of 3 defined in the ISEA projection.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.ISEA3H()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class IVEA3HRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the IVEA3H DGGS, an equal area hexagonal grid with a refinement ratio of 3 defined in the IVEA projection, using the same global indexing and sub-zone ordering as for ISEA3H.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.IVEA3H()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class ISEA7HRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA7H DGGS, an equal area hexagonal grid with a refinement ratio of 7 defined in the ISEA projection.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.ISEA7H()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class IVEA7HRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the IVEA7H DGGS, an equal area hexagonal grid with a refinement ratio of 7 defined in the ISEA projection.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.IVEA7H()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class ISEA7HZ7RasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA7H DGGS, which has the same Discrete Global Grid Hierarchy (DGGH) and sub-zone order as ISEA7H, but using the Z7 indexing for interoperability with DGGRID and IGEO7.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.ISEA7H_Z7()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class IVEA7HZ7RasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the ISEA7H DGGS, which has the same Discrete Global Grid Hierarchy (DGGH) and sub-zone order as ISEA7H, but using the Z7 indexing for interoperability with DGGRID and IGEO7.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.IVEA7H_Z7()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class RTEA4RRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the RTEA4R DGGS, an equal-area rhombic grid with a refinement ratio of 4 defined in the RTEA projection transformed into a 5x6 Cartesian space resulting in axis-aligned square zones, using the same global indexing and sub-zone ordering as for ISEA4R.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.RTEA4R()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class RTEA9RRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the RTEA9R DGGS, an equal-area rhombic grid with a refinement ratio of 9 defined in the RTEA projection transformed into a 5x6 Cartesian space resulting in axis-aligned square zones, using the same global indexing and sub-zone ordering as for ISEA9R.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.RTEA9R()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class RTEA3HRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the RTEA3H DGGS, an equal area hexagonal grid with a refinement ratio of 3 defined in the RTEA projection using the same global indexing and sub-zone ordering as for ISEA3H.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.RTEA3H()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class RTEA7HRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the RTEA7H DGGS, an equal-area hexagonal grid with a refinement ratio of 7 defined in the RTEA projection transformed using the same global indexing and sub-zone ordering as for ISEA7H.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.RTEA7H()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class RTEA7HZ7RasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the RTEA7H DGGS, an equal-area hexagonal grid with a refinement ratio of 7 defined in the RTEA projection transformed using the same global indexing and sub-zone ordering as for ISEA7H.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.RTEA7H_Z7()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class HEALPixRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the HEALPix DGGS, an equal area and axis-aligned grid with square zones topology and a refinement ratio of 4 defined in the HEALPix projection, using configuration Nφ/H = 4, Nθ/K = 3 (same as default PROJ implementation), the new indexing described in OGC API - DGGS Annex B, and scanline-based sub-zone ordering.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.HEALPix()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs


class RHEALPixRasterIndexer(DGGALRasterIndexer):
    """
    A raster indexer for the HEALPix DGGS, an equal area and axis-aligned grid with square zones topology and a refinement ratio of 9 defined in the rHEALPix projection using 50° E prime meridian (equivalent to PROJ implementation with parameters +proj=rhealpix +lon_0=50 +ellps=WGS84), the original hierarchical indexing, and scanline-based sub-zone ordering.
    """

    def __init__(self, dggs: str):
        super().__init__(dggs)
        self._dggrs = dggal.rHEALPix()

    @property
    def dggrs(self) -> dggal.DGGRS:
        return self._dggrs
