"""
Tests for numeric (binned) histograms: https://github.com/manaakiwhenua/raster2dggs/issues/68.

Categorical (unbinned) histogram behaviour is covered by test_output_schema.py;
these tests focus on --hist-bins/--hist-width/--hist-weight/--hist-normalize.
"""

import pyarrow as pa
from click.testing import CliRunner

from classes.base import TestRunthrough, read_output
from classes.helpers import make_gradient_raster, make_raster
from data.datapaths import TEST_OUTPUT_PATH
from raster2dggs.cli import cli

# 10x10 pixel raster, values 0..99 (gradient) or a fixed value (uniform).
# H3 res 5 cells are ~252 km^2, well over the ~93 km^2 raster extent, so the
# whole raster is covered by a small number of cells -- large enough lists to
# exercise binning meaningfully.
_BOUNDS = (174.0, -41.1, 174.1, -41.0)
_SIZE = 10
_RES = 5


def _make_gradient(path: str) -> None:
    make_gradient_raster(path, _BOUNDS, _SIZE)


def _make_uniform(path: str, value: float = 7.0) -> None:
    make_raster(path, _BOUNDS, _SIZE, pixel_value=value)


class _HistTestBase(TestRunthrough):
    def setUp(self):
        super().setUp()
        self._gradient = self.make_temp_raster(_make_gradient)
        self._uniform = self.make_temp_raster(_make_uniform)

    def _run(self, raster, *extra_args):
        self.invoke_cli("h3", raster, TEST_OUTPUT_PATH, _RES, *extra_args)
        return read_output(TEST_OUTPUT_PATH)

    def _non_null_histograms(self, table, col="band_1"):
        return [v for v in table.to_pandas()[col] if v is not None]


class TestPointHistBinsExplicit(_HistTestBase):
    def test_schema_types(self):
        table = self._run(
            self._gradient, "--point", "histogram", "--hist-bins", "0,25,50,75,100"
        )
        struct_type = table.schema.field("band_1").type
        self.assertEqual(struct_type.field("values").type, pa.list_(pa.float64()))
        self.assertEqual(struct_type.field("counts").type, pa.list_(pa.int64()))

    def test_bin_edges_are_lower_edges_of_populated_bins(self):
        table = self._run(
            self._gradient, "--point", "histogram", "--hist-bins", "0,25,50,75,100"
        )
        for hist in self._non_null_histograms(table):
            for v in hist["values"]:
                self.assertIn(v, (0.0, 25.0, 50.0, 75.0))

    def test_nothing_dropped_when_bins_cover_full_range(self):
        table = self._run(
            self._gradient, "--point", "histogram", "--hist-bins", "0,25,50,75,100"
        )
        total = sum(sum(hist["counts"]) for hist in self._non_null_histograms(table))
        self.assertEqual(total, _SIZE * _SIZE)

    def test_out_of_range_values_dropped(self):
        table = self._run(
            self._gradient, "--point", "histogram", "--hist-bins", "10,90"
        )
        total = sum(sum(hist["counts"]) for hist in self._non_null_histograms(table))
        self.assertLess(total, _SIZE * _SIZE)

    def test_open_ended_edges_drop_nothing(self):
        table = self._run(
            self._gradient, "--point", "histogram", "--hist-bins", "-inf,50,inf"
        )
        total = sum(sum(hist["counts"]) for hist in self._non_null_histograms(table))
        self.assertEqual(total, _SIZE * _SIZE)

    def test_last_bin_is_closed(self):
        # A value exactly at the final edge (99 is the max value; use an edge
        # that lands exactly on a present value) must be included, not dropped.
        table = self._run(self._gradient, "--point", "histogram", "--hist-bins", "0,99")
        total = sum(sum(hist["counts"]) for hist in self._non_null_histograms(table))
        # Values 0..98 fall in [0, 99); value 99 must also be counted (closed
        # last bin), so nothing in range [0, 99] is dropped.
        self.assertEqual(total, _SIZE * _SIZE)


class TestPointHistWidth(_HistTestBase):
    def test_edges_match_origin_plus_k_width(self):
        table = self._run(self._gradient, "--point", "histogram", "--hist-width", "25")
        for hist in self._non_null_histograms(table):
            for v in hist["values"]:
                self.assertEqual(v % 25, 0.0)

    def test_width_mode_drops_nothing(self):
        table = self._run(self._gradient, "--point", "histogram", "--hist-width", "25")
        total = sum(sum(hist["counts"]) for hist in self._non_null_histograms(table))
        self.assertEqual(total, _SIZE * _SIZE)

    def test_origin_shifts_bin_edges(self):
        default_table = self._run(
            self._gradient, "--point", "histogram", "--hist-width", "25"
        )
        shifted_table = self._run(
            self._gradient,
            "--point",
            "histogram",
            "--hist-width",
            "25",
            "--hist-origin",
            "10",
        )
        default_edges = {
            v for h in self._non_null_histograms(default_table) for v in h["values"]
        }
        shifted_edges = {
            v for h in self._non_null_histograms(shifted_table) for v in h["values"]
        }
        self.assertNotEqual(default_edges, shifted_edges)
        for edge in shifted_edges:
            self.assertEqual((edge - 10) % 25, 0.0)

    def test_bins_and_width_mutually_exclusive(self):
        result = CliRunner().invoke(
            cli,
            [
                "h3",
                str(self._gradient),
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_RES),
                "--point",
                "histogram",
                "--hist-bins",
                "0,50,100",
                "--hist-width",
                "25",
            ],
        )
        self.assertNotEqual(result.exit_code, 0)


class TestPointHistNormalize(_HistTestBase):
    def test_valid_overlap_sums_to_one_per_cell(self):
        table = self._run(
            self._gradient,
            "--point",
            "histogram",
            "--hist-normalize",
            "valid-overlap",
            "-d",
            "none",
        )
        struct_type = table.schema.field("band_1").type
        self.assertEqual(struct_type.field("counts").type, pa.list_(pa.float64()))
        for hist in self._non_null_histograms(table):
            self.assertAlmostEqual(sum(hist["counts"]), 1.0, places=6)

    def test_cell_area_normalize_gives_positive_floats(self):
        table = self._run(
            self._gradient,
            "--point",
            "histogram",
            "--hist-normalize",
            "cell-area",
            "-d",
            "none",
        )
        for hist in self._non_null_histograms(table):
            for c in hist["counts"]:
                self.assertGreater(c, 0.0)


class TestOverlayHistBinned(_HistTestBase):
    def test_negative_decimals_does_not_crash(self):
        # Regression test: parent_groupby_nn's Int64 cast used to run on the
        # whole (object-dtype) frame for decimals <= 0, raising for the
        # dict-valued histogram column produced by --overlay histogram.
        table = self._run(
            self._gradient,
            "--overlay",
            "histogram",
            "--hist-bins",
            "0,25,50,75,100",
            "-d",
            "0",
        )
        struct_type = table.schema.field("band_1").type
        self.assertEqual(struct_type.field("counts").type, pa.list_(pa.int64()))
        self.assertGreater(len(self._non_null_histograms(table)), 0)

    def test_categorical_mode_unaffected_by_new_flags(self):
        # No --hist-* flags: schema and values must match plain --overlay histogram.
        with_flags = self._run(self._uniform, "--overlay", "histogram")
        without_flags_schema = with_flags.schema.field("band_1").type
        self.assertEqual(
            without_flags_schema.field("values").type, pa.list_(pa.float64())
        )
        self.assertEqual(
            without_flags_schema.field("counts").type, pa.list_(pa.int64())
        )
        for hist in self._non_null_histograms(with_flags):
            self.assertEqual(hist["values"], [7.0])


class TestOverlayHistAreaWeight(_HistTestBase):
    def test_area_weight_requires_overlay_histogram(self):
        result = CliRunner().invoke(
            cli,
            [
                "h3",
                str(self._uniform),
                str(TEST_OUTPUT_PATH),
                "-r",
                str(_RES),
                "--point",
                "histogram",
                "--hist-weight",
                "area",
            ],
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_area_weighted_totals_conserve_raster_area(self):
        # Summed across every cell the raster overlaps, area-weighted counts
        # (coverage x geodesic pixel area) must reconstruct the raster's own
        # total geodesic area -- nothing gained or lost by partitioning across
        # H3 cells.
        table = self._run(
            self._uniform,
            "--overlay",
            "histogram",
            "--hist-weight",
            "area",
            "-d",
            "none",
        )
        struct_type = table.schema.field("band_1").type
        self.assertEqual(struct_type.field("counts").type, pa.list_(pa.float64()))
        total_area = sum(
            sum(hist["counts"]) for hist in self._non_null_histograms(table)
        )
        # Independently computed geodesic area of the (uniform, single-valued)
        # raster: bounds (174.0,-41.1)-(174.1,-41.0) is about 93.4 km^2.
        self.assertAlmostEqual(total_area, 93_365_567.0, delta=50_000.0)

    def test_area_weight_and_cell_area_normalize_matches_fractions(self):
        # struct<values,counts> from an area-weighted, cell-area-normalized
        # histogram on a single-valued raster should give ~ the same
        # class-fraction as --overlay fractions for that (only) class.
        hist_table = self._run(
            self._uniform,
            "--overlay",
            "histogram",
            "--hist-weight",
            "area",
            "--hist-normalize",
            "cell-area",
            "-d",
            "none",
        )
        frac_table = self._run(self._uniform, "--overlay", "fractions", "-d", "none")

        hist_by_cell = dict(
            zip(hist_table.to_pandas().index, hist_table.to_pandas()["band_1"])
        )
        frac_by_cell = dict(
            zip(frac_table.to_pandas().index, frac_table.to_pandas()["band_1"])
        )
        compared = 0
        for cell_id, hist in hist_by_cell.items():
            if hist is None:
                continue
            frac = frac_by_cell.get(cell_id)
            if frac is None:
                continue
            self.assertAlmostEqual(hist["counts"][0], frac["fractions"][0], delta=0.02)
            compared += 1
        self.assertGreater(compared, 0)
