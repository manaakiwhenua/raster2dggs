import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from click.testing import CliRunner

from classes.base import TestRunthrough
from data.datapaths import TEST_OUTPUT_PATH
from raster2dggs.cli import cli

# 10×10 pixel Float32 raster in EPSG:4326 near Auckland
_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 10
_PIXEL_VALUE = 42.0
_NODATA = -9999.0

# H3 res 5 cells are ~252 km²; the raster covers ~80 km², so all pixels land
# in one or two cells — guaranteeing lists of length > 1.
_COARSE_RES = 5
# H3 res 7 cells are ~5 km²; each 0.01°×0.01° pixel (~0.8 km²) maps to its
# own cell — lists of exactly length 1.
_FINE_RES = 7


def _make_raster(path: str, nodata: float = None) -> None:
    data = np.full((1, _SIZE, _SIZE), _PIXEL_VALUE, dtype=np.float32)
    if nodata is not None:
        data[0, 0, 0] = nodata
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*_BOUNDS, _SIZE, _SIZE),
        nodata=nodata,
    ) as dst:
        dst.write(data)


def _read_output(output_dir: Path) -> pa.Table:
    return pq.read_table(str(output_dir))


class TestOutList(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._raster.name)

    def tearDown(self):
        super().tearDown()
        Path(self._raster.name).unlink(missing_ok=True)

    def _run(self, res, *extra_args):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._raster.name, str(TEST_OUTPUT_PATH), "-r", str(res)]
            + list(extra_args),
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        return _read_output(TEST_OUTPUT_PATH)

    def test_band_columns_are_list_typed(self):
        table = self._run(_COARSE_RES, "--out", "list")
        band_field = table.schema.field("band_1")
        self.assertIsInstance(
            band_field.type,
            pa.ListType,
            f"Expected list type, got {band_field.type}",
        )

    def test_list_element_type_is_float64_when_rounded(self):
        # float32 is promoted to float64 before rounding to avoid precision artefacts
        table = self._run(_COARSE_RES, "--out", "list", "-d", "1")
        element_type = table.schema.field("band_1").type.value_type
        self.assertEqual(element_type, pa.float64())

    def test_list_element_type_preserves_source_dtype_when_unrounded(self):
        table = self._run(_COARSE_RES, "--out", "list", "-d", "none")
        element_type = table.schema.field("band_1").type.value_type
        self.assertEqual(element_type, pa.float32())

    def test_coarser_cells_collect_multiple_values(self):
        table = self._run(_COARSE_RES, "--out", "list")
        df = table.to_pandas()
        lengths = df["band_1"].map(len)
        self.assertTrue(
            (lengths > 1).any(),
            "At coarse resolution, at least some cells should aggregate multiple pixels",
        )

    def test_list_values_are_pixel_values(self):
        table = self._run(_COARSE_RES, "--out", "list")
        df = table.to_pandas()
        all_values = [v for lst in df["band_1"] for v in lst]
        self.assertTrue(
            all(np.isclose(v, _PIXEL_VALUE, atol=1e-3) for v in all_values),
            "All list elements should equal the source pixel value (no aggregation)",
        )

    def test_finer_resolution_produces_shorter_lists(self):
        # At coarser resolution more pixels share a cell, so lists are longer on average.
        coarse = self._run(_COARSE_RES, "--out", "list").to_pandas()
        fine = self._run(_FINE_RES, "--out", "list").to_pandas()
        avg_coarse = coarse["band_1"].map(len).mean()
        avg_fine = fine["band_1"].map(len).mean()
        self.assertGreater(
            avg_coarse,
            avg_fine,
            "Average list length should be greater at coarser DGGS resolution",
        )

    def test_lists_are_sorted(self):
        table = self._run(_COARSE_RES, "--out", "list")
        df = table.to_pandas()
        for lst in df["band_1"]:
            lst = list(lst)
            self.assertEqual(
                lst, sorted(lst), "Each cell's value list must be in ascending order"
            )

    def test_exit_code_zero(self):
        # Regression: --out list produces output and exits cleanly
        table = self._run(_COARSE_RES, "--out", "list")
        self.assertGreater(len(table), 0)

    def test_agg_ignored_warning_emitted(self):
        runner = CliRunner()
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster.name,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--out",
                "list",
                "--agg",
                "sum",
            ],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("--agg", result.output)


class TestOutListNodataExclusion(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._raster.name, nodata=_NODATA)

    def tearDown(self):
        super().tearDown()
        Path(self._raster.name).unlink(missing_ok=True)

    def _run(self, res, *extra_args):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._raster.name, str(TEST_OUTPUT_PATH), "-r", str(res)]
            + list(extra_args),
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        return _read_output(TEST_OUTPUT_PATH).to_pandas()

    def test_nodata_sentinel_excluded_from_lists(self):
        df = self._run(_COARSE_RES, "--out", "list")
        all_values = [v for lst in df["band_1"] for v in lst]
        self.assertFalse(
            any(np.isclose(v, _NODATA) for v in all_values),
            "Nodata sentinel value should not appear in any list (omit policy)",
        )

    def test_nodata_nan_excluded_from_lists(self):
        df = self._run(_COARSE_RES, "--out", "list")
        all_values = [v for lst in df["band_1"] for v in lst]
        self.assertFalse(
            any(np.isnan(v) for v in all_values),
            "NaN should not appear in any list (omit policy)",
        )


class TestOutHistogram(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._raster.name)

    def tearDown(self):
        super().tearDown()
        Path(self._raster.name).unlink(missing_ok=True)

    def _run(self, res, *extra_args):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._raster.name, str(TEST_OUTPUT_PATH), "-r", str(res)]
            + list(extra_args),
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        return _read_output(TEST_OUTPUT_PATH)

    def test_band_columns_are_struct_typed(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        band_field = table.schema.field("band_1")
        self.assertIsInstance(
            band_field.type,
            pa.StructType,
            f"Expected struct type, got {band_field.type}",
        )

    def test_struct_has_values_and_counts_fields(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        struct_type = table.schema.field("band_1").type
        field_names = {struct_type.field(i).name for i in range(struct_type.num_fields)}
        self.assertEqual(field_names, {"values", "counts"})

    def test_values_field_is_list_typed(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        struct_type = table.schema.field("band_1").type
        values_type = struct_type.field("values").type
        self.assertIsInstance(values_type, pa.ListType)

    def test_counts_field_is_list_of_int64(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        struct_type = table.schema.field("band_1").type
        counts_type = struct_type.field("counts").type
        self.assertIsInstance(counts_type, pa.ListType)
        self.assertEqual(counts_type.value_type, pa.int64())

    def test_values_element_type_is_float64_when_rounded(self):
        table = self._run(_COARSE_RES, "--out", "histogram", "-d", "1")
        struct_type = table.schema.field("band_1").type
        values_element_type = struct_type.field("values").type.value_type
        self.assertEqual(values_element_type, pa.float64())

    def test_values_element_type_preserves_source_dtype_when_unrounded(self):
        table = self._run(_COARSE_RES, "--out", "histogram", "-d", "none")
        struct_type = table.schema.field("band_1").type
        values_element_type = struct_type.field("values").type.value_type
        self.assertEqual(values_element_type, pa.float32())

    def test_counts_sum_equals_total_pixels(self):
        # Total counts across all cells must equal total number of non-nodata pixels.
        table = self._run(_COARSE_RES, "--out", "histogram")
        df = table.to_pandas()
        total_counts = sum(sum(row["band_1"]["counts"]) for _, row in df.iterrows())
        self.assertEqual(total_counts, _SIZE * _SIZE)

    def test_values_are_pixel_values(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        df = table.to_pandas()
        all_values = [v for row in df["band_1"] for v in row["values"]]
        self.assertTrue(
            all(np.isclose(v, _PIXEL_VALUE, atol=1e-3) for v in all_values),
            "All histogram values should equal the source pixel value",
        )

    def test_values_are_sorted(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        df = table.to_pandas()
        for row in df["band_1"]:
            vals = list(row["values"])
            self.assertEqual(
                vals, sorted(vals), "Histogram values must be in ascending order"
            )

    def test_values_and_counts_same_length(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        df = table.to_pandas()
        for row in df["band_1"]:
            self.assertEqual(
                len(row["values"]),
                len(row["counts"]),
                "values and counts lists must be the same length",
            )

    def test_exit_code_zero(self):
        table = self._run(_COARSE_RES, "--out", "histogram")
        self.assertGreater(len(table), 0)

    def test_agg_ignored_warning_emitted(self):
        runner = CliRunner()
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster.name,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--out",
                "histogram",
                "--agg",
                "sum",
            ],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn("--agg", result.output)


class TestMultiAgg(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._raster.name)

    def tearDown(self):
        super().tearDown()
        Path(self._raster.name).unlink(missing_ok=True)

    def _run(self, res, *extra_args):
        if TEST_OUTPUT_PATH.exists():
            self.clearOutFolder(TEST_OUTPUT_PATH)
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["h3", self._raster.name, str(TEST_OUTPUT_PATH), "-r", str(res)]
            + list(extra_args),
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0, result.output)
        return _read_output(TEST_OUTPUT_PATH)

    def test_single_agg_produces_scalar_column(self):
        table = self._run(_COARSE_RES, "--agg", "mean")
        self.assertNotIsInstance(table.schema.field("band_1").type, pa.StructType)

    def test_multi_agg_produces_struct_column(self):
        table = self._run(_COARSE_RES, "--agg", "min,max")
        self.assertIsInstance(
            table.schema.field("band_1").type,
            pa.StructType,
            f"Expected struct type for multi-agg, got {table.schema.field('band_1').type}",
        )

    def test_struct_field_names_match_requested_aggs(self):
        table = self._run(_COARSE_RES, "--agg", "min,max,mean")
        struct_type = table.schema.field("band_1").type
        field_names = {struct_type.field(i).name for i in range(struct_type.num_fields)}
        self.assertEqual(field_names, {"min", "max", "mean"})

    def test_min_lte_max_in_struct(self):
        table = self._run(_COARSE_RES, "--agg", "min,max")
        df = table.to_pandas()
        for row in df["band_1"]:
            self.assertLessEqual(row["min"], row["max"])

    def test_uniform_raster_min_equals_max(self):
        table = self._run(_COARSE_RES, "--agg", "min,max")
        df = table.to_pandas()
        for row in df["band_1"]:
            self.assertAlmostEqual(row["min"], row["max"], places=3)

    def test_struct_field_types_match_source_dtype(self):
        table = self._run(_COARSE_RES, "--agg", "min,max", "-d", "none")
        struct_type = table.schema.field("band_1").type
        self.assertEqual(struct_type.field("min").type, pa.float32())
        self.assertEqual(struct_type.field("max").type, pa.float32())

    def test_struct_field_types_promoted_when_rounded(self):
        table = self._run(_COARSE_RES, "--agg", "min,max", "-d", "1")
        struct_type = table.schema.field("band_1").type
        self.assertEqual(struct_type.field("min").type, pa.float64())
        self.assertEqual(struct_type.field("max").type, pa.float64())

    def test_mean_value_equals_pixel_value_on_uniform_raster(self):
        table = self._run(_COARSE_RES, "--agg", "min,mean,max")
        df = table.to_pandas()
        for row in df["band_1"]:
            self.assertTrue(np.isclose(row["mean"], _PIXEL_VALUE, atol=1e-3))

    def test_exit_code_zero(self):
        table = self._run(_COARSE_RES, "--agg", "min,max")
        self.assertGreater(len(table), 0)

    def test_multi_agg_with_compaction(self):
        # Regression: compaction called nunique() on dict columns, which raised TypeError
        table = self._run(_COARSE_RES, "--agg", "min,max,mean", "-co")
        self.assertGreater(len(table), 0)
        self.assertIsInstance(table.schema.field("band_1").type, pa.StructType)

    def test_invalid_agg_name_rejected(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster.name,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--agg",
                "min,bogus",
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_no_spurious_warning_without_explicit_agg(self):
        runner = CliRunner()
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster.name,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--out",
                "list",
            ],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)
        self.assertNotIn("--agg", result.output)


class TestOutListValidation(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = tempfile.NamedTemporaryFile(suffix=".tiff", delete=False)
        _make_raster(self._raster.name)

    def tearDown(self):
        super().tearDown()
        Path(self._raster.name).unlink(missing_ok=True)

    def test_inappropriate_combination_rejected(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster.name,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--semantics",
                "cell_average",
                "--transfer",
                "assign_centers",
                "--out",
                "list",
            ],
        )
        self.assertNotEqual(
            result.exit_code,
            0,
            "cell_average + assign_centers is inappropriate and should be rejected",
        )

    def test_unimplemented_valid_combination_rejected(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster.name,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--semantics",
                "point_sample_field",
                "--transfer",
                "sample_nn",
                "--out",
                "value",
            ],
        )
        self.assertNotEqual(
            result.exit_code,
            0,
            "point_sample_field + sample_nn is valid but not yet implemented",
        )
