import numpy as np
import pyarrow as pa
import rasterio
from click.testing import CliRunner
from osgeo import gdal
from rasterio.crs import CRS
from rasterio.transform import from_bounds

from classes.base import TestRunthrough, read_output
from classes.helpers import make_raster
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
    make_raster(path, _BOUNDS, _SIZE, pixel_value=_PIXEL_VALUE, nodata=nodata)


def _make_mostly_nodata_raster(path: str) -> None:
    # All pixels are nodata except the single centre pixel.
    # At res 5 an H3 cell covers the entire raster extent, so
    # valid_frac ≈ 1/100 = 0.01 — below any threshold > 0.01.
    data = np.full((1, _SIZE, _SIZE), _NODATA, dtype=np.float32)
    data[0, _SIZE // 2, _SIZE // 2] = _PIXEL_VALUE
    transform = from_bounds(*_BOUNDS, _SIZE, _SIZE)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=transform,
        nodata=_NODATA,
    ) as dst:
        dst.write(data)


class TestOutList(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def _run(self, res, *extra_args):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, res, *extra_args)
        return read_output(TEST_OUTPUT_PATH)

    def test_band_columns_are_list_typed(self):
        table = self._run(_COARSE_RES, "--point", "list")
        band_field = table.schema.field("band_1")
        self.assertIsInstance(
            band_field.type,
            pa.ListType,
            f"Expected list type, got {band_field.type}",
        )

    def test_list_element_type_is_float64_when_rounded(self):
        # float32 is promoted to float64 before rounding to avoid precision artefacts
        table = self._run(_COARSE_RES, "--point", "list", "-d", "1")
        element_type = table.schema.field("band_1").type.value_type
        self.assertEqual(element_type, pa.float64())

    def test_list_element_type_preserves_source_dtype_when_unrounded(self):
        table = self._run(_COARSE_RES, "--point", "list", "-d", "none")
        element_type = table.schema.field("band_1").type.value_type
        self.assertEqual(element_type, pa.float32())

    def test_coarser_cells_collect_multiple_values(self):
        table = self._run(_COARSE_RES, "--point", "list")
        df = table.to_pandas()
        lengths = df["band_1"].map(len)
        self.assertTrue(
            (lengths > 1).any(),
            "At coarse resolution, at least some cells should aggregate multiple pixels",
        )

    def test_list_values_are_pixel_values(self):
        table = self._run(_COARSE_RES, "--point", "list")
        df = table.to_pandas()
        all_values = [v for lst in df["band_1"] for v in lst]
        self.assertTrue(
            all(np.isclose(v, _PIXEL_VALUE, atol=1e-3) for v in all_values),
            "All list elements should equal the source pixel value (no aggregation)",
        )

    def test_finer_resolution_produces_shorter_lists(self):
        # At coarser resolution more pixels share a cell, so lists are longer on average.
        coarse = self._run(_COARSE_RES, "--point", "list").to_pandas()
        fine = self._run(_FINE_RES, "--point", "list").to_pandas()
        avg_coarse = coarse["band_1"].map(len).mean()
        avg_fine = fine["band_1"].map(len).mean()
        self.assertGreater(
            avg_coarse,
            avg_fine,
            "Average list length should be greater at coarser DGGS resolution",
        )

    def test_lists_are_sorted(self):
        table = self._run(_COARSE_RES, "--point", "list")
        df = table.to_pandas()
        for lst in df["band_1"]:
            lst = list(lst)
            self.assertEqual(
                lst, sorted(lst), "Each cell's value list must be in ascending order"
            )

    def test_exit_code_zero(self):
        # Regression: --out list produces output and exits cleanly
        table = self._run(_COARSE_RES, "--point", "list")
        self.assertGreater(len(table), 0)

    def test_agg_ignored_warning_emitted(self):
        runner = CliRunner()
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--point",
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
        self._raster = self.make_temp_raster(lambda p: _make_raster(p, nodata=_NODATA))

    def _run(self, res, *extra_args):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, res, *extra_args)
        return read_output(TEST_OUTPUT_PATH).to_pandas()

    def test_nodata_sentinel_excluded_from_lists(self):
        df = self._run(_COARSE_RES, "--point", "list")
        all_values = [v for lst in df["band_1"] for v in lst]
        self.assertFalse(
            any(np.isclose(v, _NODATA) for v in all_values),
            "Nodata sentinel value should not appear in any list (omit policy)",
        )

    def test_nodata_nan_excluded_from_lists(self):
        df = self._run(_COARSE_RES, "--point", "list")
        all_values = [v for lst in df["band_1"] for v in lst]
        self.assertFalse(
            any(np.isnan(v) for v in all_values),
            "NaN should not appear in any list (omit policy)",
        )


class TestOutHistogram(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def _run(self, res, *extra_args):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, res, *extra_args)
        return read_output(TEST_OUTPUT_PATH)

    def test_band_columns_are_struct_typed(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        band_field = table.schema.field("band_1")
        self.assertIsInstance(
            band_field.type,
            pa.StructType,
            f"Expected struct type, got {band_field.type}",
        )

    def test_struct_has_values_and_counts_fields(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        struct_type = table.schema.field("band_1").type
        field_names = {struct_type.field(i).name for i in range(struct_type.num_fields)}
        self.assertEqual(field_names, {"values", "counts"})

    def test_values_field_is_list_typed(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        struct_type = table.schema.field("band_1").type
        values_type = struct_type.field("values").type
        self.assertIsInstance(values_type, pa.ListType)

    def test_counts_field_is_list_of_int64(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        struct_type = table.schema.field("band_1").type
        counts_type = struct_type.field("counts").type
        self.assertIsInstance(counts_type, pa.ListType)
        self.assertEqual(counts_type.value_type, pa.int64())

    def test_values_element_type_is_float64_when_rounded(self):
        table = self._run(_COARSE_RES, "--point", "histogram", "-d", "1")
        struct_type = table.schema.field("band_1").type
        values_element_type = struct_type.field("values").type.value_type
        self.assertEqual(values_element_type, pa.float64())

    def test_values_element_type_preserves_source_dtype_when_unrounded(self):
        table = self._run(_COARSE_RES, "--point", "histogram", "-d", "none")
        struct_type = table.schema.field("band_1").type
        values_element_type = struct_type.field("values").type.value_type
        self.assertEqual(values_element_type, pa.float32())

    def test_counts_sum_equals_total_pixels(self):
        # Total counts across all cells must equal total number of non-nodata pixels.
        table = self._run(_COARSE_RES, "--point", "histogram")
        df = table.to_pandas()
        total_counts = sum(sum(row["band_1"]["counts"]) for _, row in df.iterrows())
        self.assertEqual(total_counts, _SIZE * _SIZE)

    def test_values_are_pixel_values(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        df = table.to_pandas()
        all_values = [v for row in df["band_1"] for v in row["values"]]
        self.assertTrue(
            all(np.isclose(v, _PIXEL_VALUE, atol=1e-3) for v in all_values),
            "All histogram values should equal the source pixel value",
        )

    def test_values_are_sorted(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        df = table.to_pandas()
        for row in df["band_1"]:
            vals = list(row["values"])
            self.assertEqual(
                vals, sorted(vals), "Histogram values must be in ascending order"
            )

    def test_values_and_counts_same_length(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        df = table.to_pandas()
        for row in df["band_1"]:
            self.assertEqual(
                len(row["values"]),
                len(row["counts"]),
                "values and counts lists must be the same length",
            )

    def test_exit_code_zero(self):
        table = self._run(_COARSE_RES, "--point", "histogram")
        self.assertGreater(len(table), 0)

    def test_agg_ignored_warning_emitted(self):
        runner = CliRunner()
        TEST_OUTPUT_PATH.mkdir(exist_ok=True)
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--point",
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
        self._raster = self.make_temp_raster(_make_raster)

    def _run(self, res, *extra_args):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, res, *extra_args)
        return read_output(TEST_OUTPUT_PATH)

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
                self._raster,
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
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--point",
                "list",
            ],
            catch_exceptions=False,
        )
        self.assertEqual(result.exit_code, 0)
        self.assertNotIn("--agg", result.output)


class TestOutListValidation(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def test_sample_list_rejected(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--sample",
                "--point",
                "list",
            ],
        )
        self.assertNotEqual(result.exit_code, 0, "--sample --list should be rejected")

    def test_overlay_weighted_list_rejected(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--overlay",
                "weighted",
                "--point",
                "list",
            ],
        )
        self.assertNotEqual(
            result.exit_code, 0, "--overlay --weighted --list should be rejected"
        )

    def test_sample_overlay_rejected(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--sample",
                "--overlay",
                "weighted",
            ],
        )
        self.assertNotEqual(
            result.exit_code, 0, "--sample and --overlay should be mutually exclusive"
        )

    def test_sample_runs(self):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, _COARSE_RES, "--sample")

    def test_sample_bilinear_runs(self):
        self.invoke_cli(
            "h3", self._raster, TEST_OUTPUT_PATH, _COARSE_RES, "--sample", "bilinear"
        )

    def test_sample_bicubic_runs(self):
        self.invoke_cli(
            "h3", self._raster, TEST_OUTPUT_PATH, _COARSE_RES, "--sample", "bicubic"
        )

    def test_sample_lanczos_runs(self):
        self.invoke_cli(
            "h3", self._raster, TEST_OUTPUT_PATH, _COARSE_RES, "--sample", "lanczos"
        )


_CATEGORICAL_CLASSES = [1, 2, 3]


def _make_categorical_raster(path: str) -> None:
    # 10×10 uint8 raster with three equal classes (classes 1, 2, 3 in stripes).
    # Each class occupies ~1/3 of the pixels so frac ≈ {1: 0.33, 2: 0.33, 3: 0.33}.
    row = np.array([[1] * 4 + [2] * 3 + [3] * 3], dtype=np.uint8)
    data = np.tile(row, (_SIZE, 1)).reshape(1, _SIZE, _SIZE)
    transform = from_bounds(*_BOUNDS, _SIZE, _SIZE)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=1,
        dtype="uint8",
        crs=CRS.from_epsg(4326),
        transform=transform,
    ) as dst:
        dst.write(data)


def _set_band_name(path: str, band_name: str, band_index: int = 1) -> None:
    ds = gdal.Open(path, gdal.GA_Update)
    ds.GetRasterBand(band_index).SetDescription(band_name)
    ds.FlushCache()
    ds.Close()


def _make_named_raster(path: str, band_name: str) -> None:
    _make_raster(path)
    _set_band_name(path, band_name)


def _make_named_categorical_raster(path: str, band_name: str) -> None:
    _make_categorical_raster(path)
    _set_band_name(path, band_name)


class TestOverlay(TestRunthrough):
    """Smoke tests for --transfer overlay_weighted, overlay_mode, and mass_preserve."""

    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def _run_overlay(self, *overlay_flags, **kw):
        extra = kw.get("extra_args", ())
        self.invoke_cli(
            "h3",
            self._raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            *overlay_flags,
            *extra,
        )
        return read_output(TEST_OUTPUT_PATH)

    def test_overlay_weighted_runs(self):
        table = self._run_overlay("weighted")
        df = table.to_pandas()
        self.assertFalse(df.empty, "--overlay --weighted should produce output rows")
        self.assertIn("band_1", df.columns)

    def test_overlay_weighted_values_close_to_pixel(self):
        # Uniform raster → all cell values should equal _PIXEL_VALUE
        table = self._run_overlay("weighted")
        df = table.to_pandas()
        self.assertTrue(
            np.allclose(df["band_1"].dropna(), _PIXEL_VALUE, atol=1e-3),
            f"Expected ~{_PIXEL_VALUE}, got: {df['band_1'].unique()}",
        )

    def test_overlay_mode_runs(self):
        table = self._run_overlay("mode")
        df = table.to_pandas()
        self.assertFalse(df.empty, "--overlay --mode should produce output rows")

    def test_mass_preserve_runs(self):
        table = self._run_overlay("mass-preserve")
        df = table.to_pandas()
        self.assertFalse(
            df.empty, "--overlay --mass-preserve should produce output rows"
        )

    def test_mass_preserve_conserves_total(self):
        # sum(cell values) must equal sum(pixel_value * pixel_area) from the raster.
        # The raster is uniform (_PIXEL_VALUE) with no nodata, so the expected total
        # is simply pixel_value * pixel_area * n_pixels.
        import rasterio as _rio

        table = self._run_overlay("mass-preserve", extra_args=("-d", "none"))
        cell_total = table.to_pandas()["band_1"].sum()
        with _rio.open(self._raster) as src:
            raster_total = _PIXEL_VALUE * src.width * src.height
        self.assertAlmostEqual(
            cell_total,
            raster_total,
            delta=raster_total * 1e-4,
            msg=f"mass_preserve total {cell_total:.6f} differs from raster total {raster_total:.6f}",
        )

    def test_valid_coverage_threshold_zero_keeps_all(self):
        # threshold=0.0 is the no-op default; results should match unthresholded run
        table_default = self._run_overlay("weighted")
        table_zero = self._run_overlay(
            "weighted", extra_args=("--valid-coverage-threshold", "0.0")
        )
        self.assertEqual(
            len(table_default),
            len(table_zero),
            "threshold=0.0 should not filter any cells",
        )

    def test_valid_coverage_threshold_filters_low_coverage_cells(self):
        # Raster with 1/100 valid pixels → valid_frac ≈ 0.01 per cell.
        # threshold=0.5 → those cells are nulled; emit policy keeps rows but as NaN.
        raster = self.make_temp_raster(_make_mostly_nodata_raster)
        self.invoke_cli(
            "h3",
            raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "weighted",
            "--nodata",
            "emit",
            "-vct",
            "0.0",
        )
        n_valid_no_threshold = int(
            read_output(TEST_OUTPUT_PATH).to_pandas()["band_1"].notna().sum()
        )
        self.invoke_cli(
            "h3",
            raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "weighted",
            "--nodata",
            "emit",
            "-vct",
            "0.5",
        )
        n_valid_with_threshold = int(
            read_output(TEST_OUTPUT_PATH).to_pandas()["band_1"].notna().sum()
        )
        self.assertLess(
            n_valid_with_threshold,
            n_valid_no_threshold,
            "Higher threshold should produce fewer non-NaN cell values",
        )

    def test_valid_coverage_threshold_ignored_for_mass_preserve(self):
        # mass_preserve must not filter by coverage — partial sums are correct values.
        # threshold=0.9 should be ignored → same row count as threshold=0.0.
        raster = self.make_temp_raster(_make_mostly_nodata_raster)
        self.invoke_cli(
            "h3",
            raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "mass-preserve",
            "-vct",
            "0.0",
        )
        n_without = len(read_output(TEST_OUTPUT_PATH).to_pandas())
        self.invoke_cli(
            "h3",
            raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "mass-preserve",
            "-vct",
            "0.9",
        )
        n_with = len(read_output(TEST_OUTPUT_PATH).to_pandas())
        self.assertEqual(
            n_without, n_with, "mass_preserve should ignore coverage threshold"
        )

    def test_named_band_column_is_preserved(self):
        band_name = "temperature"
        raster = self.make_temp_raster(lambda p: _make_named_raster(p, band_name))
        self.invoke_cli(
            "h3",
            raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "weighted",
        )
        table = read_output(TEST_OUTPUT_PATH)
        self.assertIn(
            band_name, table.schema.names, "Named band should appear as output column"
        )
        self.assertNotIn(
            "band_1",
            table.schema.names,
            "Generic 'band_1' should not appear when band has a name",
        )

    def test_overlay_without_method_rejected(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--overlay",
            ],
        )
        self.assertNotEqual(
            result.exit_code, 0, "--overlay without a method flag should be rejected"
        )

    def test_point_and_overlay_mutually_exclusive(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "h3",
                self._raster,
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_COARSE_RES),
                "--point",
                "list",
                "--overlay",
                "weighted",
            ],
        )
        self.assertNotEqual(
            result.exit_code, 0, "--point and --overlay should be mutually exclusive"
        )


class TestFractionCover(TestRunthrough):
    """Tests for --overlay --fractions."""

    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_categorical_raster)

    def _run(self, *extra_args):
        self.invoke_cli(
            "h3",
            self._raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "fractions",
            *extra_args,
        )
        return read_output(TEST_OUTPUT_PATH)

    def test_runs_and_produces_rows(self):
        table = self._run()
        self.assertGreater(len(table), 0)

    def test_band_column_is_struct_typed(self):
        table = self._run()
        field = table.schema.field("band_1")
        self.assertIsInstance(
            field.type, pa.StructType, f"Expected struct type, got {field.type}"
        )

    def test_classes_field_is_list_of_int64(self):
        table = self._run()
        struct_type = table.schema.field("band_1").type
        classes_type = struct_type.field("classes").type
        self.assertIsInstance(classes_type, pa.ListType)
        self.assertEqual(classes_type.value_type, pa.int64())

    def test_fractions_field_is_list_of_float64(self):
        table = self._run()
        struct_type = table.schema.field("band_1").type
        fractions_type = struct_type.field("fractions").type
        self.assertIsInstance(fractions_type, pa.ListType)
        self.assertEqual(fractions_type.value_type, pa.float64())

    def test_fractions_sum_at_most_one(self):
        # Fractions represent fraction of total cell area, so partial-coverage
        # cells (outside raster or over nodata) may sum to less than 1.0.
        # Run without rounding so no precision-loss obscures the invariant.
        table = self._run("-d", "none")
        df = table.to_pandas()
        for row in df["band_1"]:
            if row is not None:
                total = sum(row["fractions"])
                self.assertLessEqual(
                    total, 1.0 + 1e-5, "Class fractions must not exceed 1.0"
                )
                self.assertGreater(total, 0.0, "Non-null fractions must be positive")

    def test_partial_coverage_sums_less_than_one(self):
        # At res 5, H3 cells (~252 km²) are much larger than the ~80 km² test raster,
        # so valid_frac ≈ 0.33 and fractions should sum well below 1.0.
        table = self._run()
        df = table.to_pandas()
        sums = [sum(row["fractions"]) for row in df["band_1"] if row is not None]
        self.assertTrue(
            any(s < 0.9 for s in sums),
            "Expected at least one partially-covered cell with fraction sum < 0.9",
        )

    def test_decimals_applied_to_fractions(self):
        table = self._run("-d", "3")
        df = table.to_pandas()
        for row in df["band_1"]:
            if row is not None:
                for f in row["fractions"]:
                    self.assertAlmostEqual(
                        f,
                        round(f, 3),
                        places=10,
                        msg=f"Fraction {f} not rounded to 3 dp",
                    )

    def test_all_keys_are_valid_classes(self):
        table = self._run()
        df = table.to_pandas()
        for row in df["band_1"]:
            if row is not None:
                for k in row["classes"]:
                    self.assertIn(k, _CATEGORICAL_CLASSES, f"Unexpected class key {k}")

    def test_named_band_column_is_preserved(self):
        band_name = "landcover"
        raster = self.make_temp_raster(
            lambda p: _make_named_categorical_raster(p, band_name)
        )
        self.invoke_cli(
            "h3",
            raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "fractions",
        )
        table = read_output(TEST_OUTPUT_PATH)
        self.assertIn(
            band_name,
            table.schema.names,
            "Named band should appear as output column for fractions",
        )
        self.assertNotIn(
            "band_1",
            table.schema.names,
            "Generic 'band_1' should not appear when band has a name",
        )
        field = table.schema.field(band_name)
        self.assertIsInstance(
            field.type,
            pa.StructType,
            f"Named band column should be struct type, got {field.type}",
        )


class TestOverlayCollect(TestRunthrough):
    """Tests for --overlay --list and --overlay --histogram."""

    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def _run_list(self, *extra_args):
        self.invoke_cli(
            "h3",
            self._raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "list",
            *extra_args,
        )
        return read_output(TEST_OUTPUT_PATH)

    def _run_histogram(self, *extra_args):
        self.invoke_cli(
            "h3",
            self._raster,
            TEST_OUTPUT_PATH,
            _COARSE_RES,
            "--overlay",
            "histogram",
            *extra_args,
        )
        return read_output(TEST_OUTPUT_PATH)

    def test_overlay_list_produces_list_column(self):
        table = self._run_list()
        self.assertGreater(len(table), 0, "--overlay --list produced no rows")
        field = table.schema.field("band_1")
        self.assertIsInstance(
            field.type, pa.ListType, f"Expected list type, got {field.type}"
        )

    def test_overlay_list_values_are_numeric(self):
        table = self._run_list()
        df = table.to_pandas()
        for val in df["band_1"].dropna():
            self.assertTrue(
                hasattr(val, "__len__"), f"Expected sequence, got {type(val)}"
            )
            self.assertGreater(
                len(val), 0, "List column should not contain empty lists"
            )

    def test_overlay_histogram_produces_struct_column(self):
        table = self._run_histogram()
        self.assertGreater(len(table), 0, "--overlay --histogram produced no rows")
        field = table.schema.field("band_1")
        self.assertIsInstance(
            field.type, pa.StructType, f"Expected struct type, got {field.type}"
        )

    def test_overlay_histogram_has_values_and_counts_fields(self):
        table = self._run_histogram()
        struct_type = table.schema.field("band_1").type
        field_names = {struct_type.field(i).name for i in range(struct_type.num_fields)}
        self.assertIn("values", field_names)
        self.assertIn("counts", field_names)

    def test_overlay_histogram_counts_are_positive_integers(self):
        table = self._run_histogram()
        df = table.to_pandas()
        for row in df["band_1"]:
            if row is not None:
                for c in row["counts"]:
                    self.assertGreater(c, 0, "Histogram count must be positive")

    def test_overlay_list_no_decimals_does_not_crash(self):
        # Regression test: _build_write_schema/_element_type used to resolve
        # the "preserve source dtype" (-d none) branch against the Stage-2
        # (post-aggregation) column dtype, which is `object` for collected
        # list/histogram columns rather than the source raster's pixel
        # dtype -- crashing pa.from_numpy_dtype. Fixed by capturing the real
        # per-band dtypes at Stage 1 (kwargs["source_pixel_dtypes"]).
        table = self._run_list("-d", "none")
        field = table.schema.field("band_1")
        self.assertEqual(field.type.value_type, pa.float32())

    def test_overlay_histogram_no_decimals_does_not_crash(self):
        table = self._run_histogram("-d", "none")
        struct_type = table.schema.field("band_1").type
        self.assertEqual(struct_type.field("values").type, pa.list_(pa.float32()))

    def test_overlay_list_and_histogram_same_total_values(self):
        list_table = self._run_list()
        hist_table = self._run_histogram()
        list_df = list_table.to_pandas()
        hist_df = hist_table.to_pandas()
        self.assertEqual(len(list_df), len(hist_df))
        for lst, hist in zip(list_df["band_1"], hist_df["band_1"]):
            if lst is None and hist is None:
                continue
            self.assertEqual(
                len(lst),
                sum(hist["counts"]),
                "Total value count in list should equal sum of histogram counts",
            )


# --------------------------------------------------------------------------- #
# Geodesic pixel-area weighting for geographic CRS rasters
# --------------------------------------------------------------------------- #

# 1-column, 2-row WGS84 raster.  All corners fall within H3 res-1 cell
# 81323ffffffffff (verified: all four corners + centre map to that cell).
# Top pixel (lat 34.5–36.5, row 0) = 0.0; bottom pixel (lat 32.5–34.5, row 1) = 10.0.
# Row 1 is equatorward → larger geodesic area → weighted mean > naive mean.
#   row0 area ≈ 70 461 km²,  row1 area ≈ 72 143 km²  (pyproj WGS84)
#   geodesic mean = 10 × 72 143 / (70 461 + 72 143) ≈ 5.0590
_GEO_BOUNDS = (170.5, 32.5, 174.0, 36.5)
_GEO_RES = 1
_GEO_NAIVE_MEAN = 5.0
_GEO_EXPECTED_MEAN = 5.0590


def _make_geodesic_test_raster_uniform(path: str) -> None:
    data = np.array([[[1.0], [1.0]]], dtype=np.float32)  # (bands=1, height=2, width=1)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=1,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*_GEO_BOUNDS, 1, 2),
    ) as dst:
        dst.write(data)


def _make_geodesic_test_raster(path: str) -> None:
    data = np.array([[[0.0], [10.0]]], dtype=np.float32)  # (bands=1, height=2, width=1)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=1,
        count=1,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=from_bounds(*_GEO_BOUNDS, 1, 2),
    ) as dst:
        dst.write(data)


class TestGeodesicAreaWeighting(TestRunthrough):
    """--overlay weighted uses geodesic pixel areas for geographic CRS rasters."""

    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_geodesic_test_raster)

    def test_geodesic_mean_differs_from_naive_mean(self):
        self.invoke_cli(
            "h3", self._raster, TEST_OUTPUT_PATH, _GEO_RES, "--overlay", "weighted"
        )
        df = read_output(TEST_OUTPUT_PATH).to_pandas()
        self.assertEqual(len(df), 1, f"Expected 1 H3 cell, got {len(df)}")
        value = float(df["band_1"].iloc[0])
        self.assertGreater(
            value,
            _GEO_NAIVE_MEAN + 0.01,
            f"Geodesic mean ({value:.4f}) should exceed naive mean ({_GEO_NAIVE_MEAN})",
        )
        self.assertAlmostEqual(
            value,
            _GEO_EXPECTED_MEAN,
            delta=0.05,
            msg=f"Expected ~{_GEO_EXPECTED_MEAN:.4f}, got {value:.4f}",
        )


# Expected result for uniform density=1 raster over _GEO_BOUNDS at _GEO_RES:
# Σ(geodesic_pixel_area) for both rows ≈ 142 603 821 767 m²
_GEO_DENSITY_EXPECTED = 142603821767.0


class TestDensityPreserve(TestRunthrough):
    """--overlay density-preserve integrates density × geodesic pixel area."""

    def setUp(self):
        super().setUp()
        # Reuse the geodesic test raster factory — uniform value 1.0 in both rows.
        self._raster = self.make_temp_raster(_make_geodesic_test_raster_uniform)

    def test_density_preserve_sum_equals_total_geodesic_area(self):
        # density=1 everywhere → sum across all DGGS cells = total geodesic pixel area.
        # Mass conservation: each pixel's contribution is distributed across however many
        # cells it overlaps, so the grand sum must equal Σ(pixel_area).
        self.invoke_cli(
            "h3",
            self._raster,
            TEST_OUTPUT_PATH,
            _GEO_RES,
            "--overlay",
            "density-preserve",
            "-d",
            "none",
        )
        df = read_output(TEST_OUTPUT_PATH).to_pandas()
        self.assertGreater(len(df), 0, "--overlay density-preserve produced no rows")
        total = float(df["band_1"].dropna().sum())
        self.assertAlmostEqual(
            total,
            _GEO_DENSITY_EXPECTED,
            delta=_GEO_DENSITY_EXPECTED * 0.01,
            msg=f"Expected sum ~{_GEO_DENSITY_EXPECTED:.0f} m², got {total:.0f}",
        )


class TestNegativeDecimals(TestRunthrough):
    """Negative -d values round to nearest 10 (-1), 100 (-2), etc.
    Output schema must be Int64, matching the behaviour of -d 0."""

    def setUp(self):
        super().setUp()
        self._raster = self.make_temp_raster(_make_raster)

    def _run(self, *extra_args):
        self.invoke_cli("h3", self._raster, TEST_OUTPUT_PATH, _COARSE_RES, *extra_args)
        return read_output(TEST_OUTPUT_PATH)

    def test_d_neg1_schema_is_int64(self):
        # -d -1 rounds to nearest 10 → whole number → Int64 schema
        table = self._run("-d", "-1")
        self.assertEqual(table.schema.field("band_1").type, pa.int64())

    def test_d_neg1_values_rounded_to_tens(self):
        # pixel value 42.0 → round(-1) = 40
        table = self._run("-d", "-1")
        df = table.to_pandas()
        self.assertTrue(
            (df["band_1"] == 40).all(),
            f"Expected 40 (42 rounded to nearest 10), got: {df['band_1'].unique()}",
        )

    def test_d_neg2_schema_is_int64(self):
        table = self._run("-d", "-2")
        self.assertEqual(table.schema.field("band_1").type, pa.int64())

    def test_d_neg2_values_rounded_to_hundreds(self):
        # 42 rounded to nearest 100 = 0
        table = self._run("-d", "-2")
        df = table.to_pandas()
        self.assertTrue(
            (df["band_1"] == 0).all(),
            f"Expected 0 (42 rounded to nearest 100), got: {df['band_1'].unique()}",
        )

    def test_d_neg1_list_element_type_is_int64(self):
        table = self._run("--point", "list", "-d", "-1")
        element_type = table.schema.field("band_1").type.value_type
        self.assertEqual(element_type, pa.int64())

    def test_d_neg1_list_values_rounded_to_tens(self):
        table = self._run("--point", "list", "-d", "-1")
        df = table.to_pandas()
        all_values = [v for lst in df["band_1"] for v in lst]
        self.assertTrue(
            all(v == 40 for v in all_values),
            f"Expected all list elements to be 40, got: {set(all_values)}",
        )

    def test_d_neg1_histogram_values_type_is_int64(self):
        table = self._run("--point", "histogram", "-d", "-1")
        hist_type = table.schema.field("band_1").type
        self.assertEqual(hist_type.field("values").type.value_type, pa.int64())
