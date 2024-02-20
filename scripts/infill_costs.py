# -*- coding: utf-8 -*-
"""Script to infill cost matrices produced by OTP4GB-py."""

##### IMPORTS #####
from __future__ import annotations

import argparse
import datetime
import enum
import functools
import logging
import pathlib
import re
import sys
import textwrap
from typing import Any, Callable, Mapping, NamedTuple, Optional, Union

import caf.toolkit
import numpy as np
import pandas as pd
import pydantic
import tqdm
from matplotlib import figure, offsetbox, patches
from matplotlib import pyplot as plt
from matplotlib.backends import backend_pdf
from pydantic import dataclasses, types
from scipy import optimize

sys.path.extend((".", ".."))
import otp4gb
from otp4gb import centroids, config, cost, parameters

##### CONSTANTS #####
LOG = logging.getLogger(otp4gb.__package__ + ".infill_costs")
MARKER_SIZE = 10


##### CLASSES #####
@dataclasses.dataclass
class InclusiveRange:
    """Inclusive range iterator."""

    start: int
    end: int

    _current: int = 0

    def __iter__(self):
        self._current = self.start - 1
        return self

    def __next__(self) -> int:
        self._current += 1
        if self._current <= self.end:
            return self._current

        raise StopIteration()


class InfillMethod(enum.StrEnum):
    """Cost infilling methods."""

    MEAN_RATIO = enum.auto()
    LINEAR = enum.auto()
    POLYNOMIAL_2 = enum.auto()
    POLYNOMIAL_3 = enum.auto()
    POLYNOMIAL_4 = enum.auto()
    EXPONENTIAL = enum.auto()
    LOGARITHMIC = enum.auto()
    X_PLUS_LOGX = enum.auto()


class OutlierMethod(enum.StrEnum):
    """Method for calculating outliers in `CurveFit`."""

    STDDEV = enum.auto()
    GEH = enum.auto()


class InfillParameters(caf.toolkit.BaseConfig):
    """Config for `infill_costs` module."""

    folders: list[types.DirectoryPath]
    infill_columns: dict[str, float]
    zero_cost_zones: InclusiveRange
    infill_methods: list[InfillMethod]
    outlier_method: Optional[OutlierMethod] = None
    outlier_cutoff: Optional[float] = None


@dataclasses.dataclass
class InfillArgs:
    """Arguments for `infill_costs` module."""

    config: types.FilePath

    @classmethod
    def parse(cls) -> InfillArgs:
        """Parse command line arguments."""
        parser = argparse.ArgumentParser(
            description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument(
            "config", type=pathlib.Path, help="path to infill config file"
        )

        args = parser.parse_args()
        return InfillArgs(config=args.config)


class PlotType(enum.Enum):
    """Type of plot to create."""

    HEXBIN = enum.auto()
    SCATTER = enum.auto()
    HISTOGRAM = enum.auto()


class _Config:
    arbitrary_types_allowed = True


@dataclasses.dataclass(config=_Config)
class PlotData:
    """Data for plotting to a graph."""

    x: pd.Series
    y: pd.Series
    title: str | None = None
    outlier_indices: pd.Index | None = None
    outlier_label: str | None = None

    @pydantic.validator("y")
    def _check_index(cls, value: pd.Series, values) -> pd.Series:
        if not value.index.equals(values["x"].index):
            raise ValueError("x and y indices are different")

        return value

    def filter(
        self, min_x: float, max_x: float, min_y: float, max_y: float
    ) -> PlotData:
        """Filter data to within bounds given."""
        x_filter = (self.x > min_x) & (self.x < max_x)
        y_filter = (self.y > min_y) & (self.y < max_y)

        index = self.x.index[x_filter & y_filter]

        return PlotData(x=self.x.loc[index], y=self.y.loc[index], title=self.title)


class AxisLimit(NamedTuple):
    """Limits for plot axis."""

    min_x: int | float | None = None
    max_x: int | float | None = None
    min_y: int | float | None = None
    max_y: int | float | None = None

    def infill(self, data: PlotData) -> AxisLimit:
        """Update any missing values to min / max `data` values."""
        filtered = data.filter(
            min_x=-np.inf if self.min_x is None else self.min_x,
            max_x=np.inf if self.max_x is None else self.max_x,
            min_y=-np.inf if self.min_y is None else self.min_y,
            max_y=np.inf if self.max_y is None else self.max_y,
        )

        values = []

        for name, value in self._asdict().items():
            if value is not None:
                values.append(value)
                continue

            func = np.min if name.startswith("min") else np.max
            data = filtered.x if name.endswith("x") else filtered.y

            values.append(func(data))

        return AxisLimit(*values)


CurveFunction = Union[
    Callable[[np.ndarray, float], np.ndarray],
    Callable[[np.ndarray, float, float], np.ndarray],
    Callable[[np.ndarray, float, float, float], np.ndarray],
    Callable[[np.ndarray, float, float, float, float], np.ndarray],
    Callable[[np.ndarray, float, float, float, float, float], np.ndarray],
]


@dataclasses.dataclass(config=_Config)
class CurveFit:
    """Fits data to curves using `scipy.optimize.curve_fit` and calculates outliers.

    After calculating outliers the first time the fitting
    calculation will be redone without including the outliers.
    """

    function: CurveFunction
    popt: np.ndarray
    residuals: np.ndarray
    stddev: float
    geh: np.ndarray
    outlier_mask: np.ndarray
    outlier_method: Optional[OutlierMethod] = None
    outlier_cutoff: Optional[float] = None

    @classmethod
    def fit(
        cls,
        func: CurveFunction,
        x_data: np.ndarray,
        y_data: np.ndarray,
        outlier_method: OutlierMethod | None = None,
        outlier_cutoff: float | None = 5,
        **kwargs,
    ) -> CurveFit:
        """Fit given `func` to data using `scipy.optimize.curve_fit`.

        If `outlier_method` is given then outliers are calculated,
        after fitting, and the fitting is performed a second time
        without them (no new outliers are calculated on the second
        fit).

        Parameters
        ----------
        func : CurveFunction
            Function to fit data to.
        x_data, y_data : np.ndarray
            Data to fit to.
        outlier_method: OutlierMethod, optional
            Method used to calculate outliers, if None no
            outliers are calculated.
        outlier_cutoff : float | None, default 5
            Cutoff value used to determine outliers, used differently
            depending on `outlier_method`:
            - STDDEV: Factor applied to standard deviation when calculating
              outliers, an outlier is defined as the points with a residual
              greater than `stddev_cutoff` multiple of standard deviations.
            - GEH: Any values with a GEH greater than this value are
              considered outliers.
        kwargs :
            Keyword arguments to pass to `optimize.curve_fit`.

        Returns
        -------
        CurveFit
            Optimum parameters and outliers from fitting.
        """
        popt, pcov, info, msg, ier = optimize.curve_fit(
            func, x_data, y_data, full_output=True, **kwargs
        )
        residuals = info["fvec"]
        stddev = np.std(np.abs(residuals))
        geh = np.array(calculate_geh(y_data, func(x_data, *popt)))

        if outlier_method is None:
            return CurveFit(
                function=func,
                popt=popt,
                residuals=residuals,
                stddev=stddev,
                geh=geh,
                outlier_mask=np.full(x_data.shape, False),
            )

        if outlier_method == OutlierMethod.STDDEV:
            outlier_mask = np.abs(residuals) > (outlier_cutoff * stddev)
        elif outlier_method == OutlierMethod.GEH:
            outlier_mask = geh > outlier_cutoff
        else:
            raise ValueError(f"unknown outlier method: {outlier_method}")

        excluded_fit = cls.fit(
            func, x_data[~outlier_mask], y_data[~outlier_mask], None, **kwargs
        )

        return CurveFit(
            function=func,
            popt=excluded_fit.popt,
            residuals=excluded_fit.residuals,
            stddev=excluded_fit.stddev,
            geh=geh,
            outlier_mask=outlier_mask,
            outlier_method=outlier_method,
            outlier_cutoff=outlier_cutoff,
        )

    def __call__(self, array: np.ndarray) -> np.ndarray:
        """Call curve function with optimum parameters."""
        return self.function(array, *self.popt)


class InfillFunction:
    """Functions for infilling costs using crow-fly distances."""

    _MAX_LABEL_WIDTH = 30
    _FMT = ".2f"
    _curve: Optional[CurveFit]
    _function: Callable[[np.ndarray], np.ndarray]
    _equation: str
    _name: str
    outlier_indices: pd.MultiIndex
    outlier_label: str = ""

    def __init__(
        self,
        method: InfillMethod,
        x: pd.Series,
        y: pd.Series,
        outlier_method: OutlierMethod | None = None,
        outlier_cutoff: float | None = None,
    ) -> None:
        """Fits infilling function to given data.

        Parameters
        ----------
        method : InfillMethod
            Method of infilling to use.
        x, y : pd.Series
            X and y values to fit infilling function to.
        outlier_method: OutlierMethod, optional
            Method for calculating outliers when fitting data.
        outlier_cutoff: float, optional
            Cutoff value to use for outliers, depends
            on `outlier_method`.
        """
        setup_function = self._get_function(method)
        self.outlier_indices = pd.MultiIndex.from_tuples(
            [], names=["origin", "destination"]
        )
        self._outlier_method = outlier_method
        self._outlier_cutoff = outlier_cutoff

        try:
            setup_function(x, y)
        except RuntimeError as error:
            LOG.error("Infilling fit error: %s", error)

            self._equation = "y = NaN"
            self._function = lambda arr: arr * np.nan
            self._name = f"{method.name.replace('_', ' ').title()} Fit Error"

    def __call__(self, distances: np.ndarray) -> np.ndarray:
        """Calculate infill values based on `distances`."""
        return self._function(distances)

    @property
    def label(self) -> str:
        """Plot label for infill function with equation."""
        if len(self._name) > self._MAX_LABEL_WIDTH:
            name = textwrap.fill(self._name, self._MAX_LABEL_WIDTH)
        else:
            name = self._name

        equation = re.sub(r"y\s*=\s*", "", self._equation).strip()

        if len(equation) <= self._MAX_LABEL_WIDTH:
            return f"{name}\n$y={equation}$"

        equation = textwrap.wrap(equation, self._MAX_LABEL_WIDTH)
        equation = "".join(f"\n   ${i}$" for i in equation)

        return f"{name}\n$y=${equation}"

    def _get_function(
        self, method: InfillMethod
    ) -> Callable[[pd.Series, pd.Series], None]:
        lookup: dict[InfillMethod, Callable] = {
            InfillMethod.MEAN_RATIO: self._ratio,
            InfillMethod.LINEAR: self._linear,
            InfillMethod.POLYNOMIAL_2: functools.partial(self._polynomial, degree=2),
            InfillMethod.POLYNOMIAL_3: functools.partial(self._polynomial, degree=3),
            InfillMethod.POLYNOMIAL_4: functools.partial(self._polynomial, degree=4),
            InfillMethod.EXPONENTIAL: self._exponential,
            InfillMethod.LOGARITHMIC: self._logarithmic,
            InfillMethod.X_PLUS_LOGX: self._x_plus_logx,
        }

        if method not in lookup:
            raise ValueError(f"no function defined for method: '{method}'")

        return lookup[method]

    def _fit(
        self, func: CurveFunction, x: pd.Series, y: pd.Series, **kwargs
    ) -> CurveFit:
        self._curve = CurveFit.fit(
            func, x, y, self._outlier_method, self._outlier_cutoff, **kwargs
        )
        assert isinstance(self._curve, CurveFit)

        self._function = lambda x: func(x, *list(self._curve.popt))

        if self._curve is not None and self._curve.outlier_method is not None:
            self.outlier_indices = x.index[self._curve.outlier_mask]
            self.outlier_label = (
                f"{self._curve.outlier_method.name} - {self._curve.outlier_cutoff}"
            )

        return self._curve

    def _ratio(self, x: pd.Series, y: pd.Series) -> None:
        ratio = np.mean(y / x)

        self._function = lambda arr: arr * ratio
        self._name = "Mean Ratio"
        self._equation = f"y = {ratio:.2f}x"
        self._curve = None

    def _linear(self, x: pd.Series, y: pd.Series) -> None:
        def line(arr, gradient, intercept):
            return (arr * gradient) + intercept

        self._curve = self._fit(line, x, y)
        gradient, intercept = self._curve.popt

        self._name = "Linear Regression"
        self._equation = f"y = {gradient:.2f}x {intercept:+.2f}"

    def _polynomial(self, x: pd.Series, y: pd.Series, degree: int) -> None:
        def poly5(x, a, b, c, d, e, f):
            return a * x**5 + b * x**4 + c * x**3 + d * x**2 + e * x + f

        def poly4(x, b, c, d, e, f):
            return b * x**4 + c * x**3 + d * x**2 + e * x + f

        def poly3(x, c, d, e, f):
            return c * x**3 + d * x**2 + e * x + f

        def poly2(x, d, e, f):
            return d * x**2 + e * x + f

        degree_lookup: dict[int, Callable] = {5: poly5, 4: poly4, 3: poly3, 2: poly2}

        if degree not in degree_lookup:
            raise ValueError(
                f"function not defined for polynomial with degree {degree}, "
                f"only defined for {', '.join(str(i) for i in degree_lookup)}"
            )

        self._curve = self._fit(degree_lookup[degree], x, y)
        self._name = f"Polynomial degree {degree}"

        self._equation = "y ="
        for i, value in enumerate(self._curve.popt):
            power = degree - i

            if i == 0:
                self._equation += f"{value:.2f}"
            else:
                self._equation += f"{value:+.2f}"

            if power > 1:
                self._equation += f"x^{power}"
            elif power == 1:
                self._equation += "x"

    def _exponential(self, x: pd.Series, y: pd.Series) -> None:
        def exp(arr, a, b, c):
            return a * np.exp(b * arr) + c

        self._curve = self._fit(exp, x, y)
        a, b, c = self._curve.popt

        self._name = "Exponential"
        self._equation = rf"y = {a:.2f} e^{{{b:.2f}x}} {c:+.2f}"

    def _logarithmic(self, x: pd.Series, y: pd.Series) -> None:
        def log(arr, a, b, c):
            return a * np.log(b * arr) + c

        self._curve = self._fit(log, x, y)
        a, b, c = self._curve.popt

        self._name = "Natural Log"
        self._equation = rf"y = {a:.2f} \ln({b:.2f}x) {c:+.2f}"

    def _x_plus_logx(self, x: pd.Series, y: pd.Series) -> None:
        def log(arr, a, b):
            return (a * arr) + b * np.log(arr + 1)

        self._curve = self._fit(log, x, y, p0=(1, 10))
        a, b = self._curve.popt

        self._name = "Linear & Logarithmic"
        self._equation = rf"y = {a:.2f}x {b:+.2f} \cdot \ln(x + 1)"


##### FUNCTIONS #####
def calculate_geh(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Calculate the GEH statistic between `x` and `y`."""
    return np.sqrt((2 * (y - x) ** 2) / (x + y))


def calculate_crow_fly(
    origins_path: pathlib.Path,
    destinations_path: pathlib.Path | None,
    extents: centroids.Bounds | None = None,
) -> pd.Series:
    """Calculate crow-fly distances between origins and destination centroids.

    Parameters
    ----------
    origins_path : pathlib.Path
        CSV containing origin centroids with lat/lon coordinates.
    destinations_path : pathlib.Path, optional
        Optional destination centroids (origins used if not given).
    extents : centroids.Bounds, optional
        Extents to filter the centroids for.

    Returns
    -------
    pd.Series
        Crow-fly distances with origin and destination indices.
    """
    if destinations_path is None:
        LOG.info("Calculating crow-fly distances for centroids '%s'", origins_path.name)
    else:
        LOG.info(
            "Calculating crow-fly distance for origins '%s' and destinations '%s'",
            origins_path.name,
            destinations_path.name,
        )

    centroid_data = centroids.load_centroids(
        origins_path,
        destinations_path,
        zone_columns=centroids.ZoneCentroidColumns(),
        extents=extents,
    )
    if centroid_data.destinations is None:
        destinations = centroid_data.origins
    else:
        destinations = centroid_data.destinations

    return parameters.calculate_distance_matrix(
        centroid_data.origins.set_index(centroid_data.columns.id),
        destinations.set_index(centroid_data.columns.id),
        parameters.CROWFLY_DISTANCE_CRS,
    )


def filter_responses(responses_path: pathlib.Path, max_count: int = 10) -> pathlib.Path:
    """Filter responses with itineraries.

    Parameters
    ----------
    responses_path : pathlib.Path
        Path to JSON lines file containing OTP4GB responses.
    max_count : int, default 10
        Maximum number of responses to include in filter.

    Returns
    -------
    pathlib.Path
        Path to file containing filtered responses.
    """
    output_path = responses_path.with_name(responses_path.stem + "-filtered.jsonl")
    count = 0

    with open(responses_path, "rt", encoding="utf-8") as file:
        with open(output_path, "wt", encoding="utf-8") as out_file:
            for result in tqdm.tqdm(
                cost.iterate_responses(file), desc="Iterating responses"
            ):
                if result.plan is None or len(result.plan.itineraries) == 0:
                    continue

                out_file.write(result.json() + "\n")
                count += 1
                if count > max_count:
                    break

    LOG.info("Written filtered responses: %s", output_path.name)
    return output_path


def fixed_annotation(ax: plt.Axes, text: str, loc: str = "upper right") -> None:
    """Add an `AnchoredText` box to `ax` at `loc`."""
    at = offsetbox.AnchoredText(
        text,
        prop={"fontsize": "small"},
        frameon=True,
        loc=loc,
    )
    at.patch.set_boxstyle(patches.BoxStyle("round"))
    ax.add_artist(at)


def histogram(ax: plt.Axes, data: pd.Series) -> None:
    """Plot histogram of `data` with percentage labels."""
    finite = data.values[np.isfinite(data.values)]

    max_ = np.max(finite)
    annotation = [
        f"Min: {np.min(finite):,.0f},",
        f"median: {np.median(finite):,.0f},",
        f"max: {max_:,.0f},",
        f"max OD: {data.index[data == max_][0]}.",
    ]

    if len(finite) != len(data):
        annotation.append(
            f"{len(finite)} ({len(finite) / len(data):.1%})"
            "non-finite values excluded from plot."
        )

    fixed_annotation(ax, textwrap.fill(" ".join(annotation), width=30))

    counts, _, rectangles = ax.hist(finite, bins=20)

    total = np.sum(counts)
    for count, rect in zip(counts, rectangles):
        if count == 0:
            continue

        percentage = count / total
        if percentage > 0.01:  # Greater than 1%
            label = f"{percentage:.0%}"
        else:
            label = f"{percentage:.1%}"

        ax.text(
            rect.get_x() + rect.get_width() / 2,
            rect.get_height() + 5,
            label,
            ha="center",
            va="bottom",
            fontsize="small",
            rotation=45,
        )

    ax.set_ylabel("Count")
    ax.set_xlabel(data.name)
    ax.set_title(f"Distribution of {data.name}")


def plot_axes(
    ax: plt.Axes,
    data: PlotData,
    plot_type: PlotType,
    axis_limit: AxisLimit,
) -> None:
    """Create plot on given axes."""
    total_loc = "upper left"

    if plot_type == PlotType.HEXBIN:
        hb = ax.hexbin(data.x, data.y, extent=axis_limit, mincnt=1)
        plt.colorbar(hb, ax=ax, label="Count", aspect=40)

    elif plot_type == PlotType.SCATTER:
        ax.scatter(
            data.x, data.y, rasterized=len(data.x) > 1000, marker="+", s=MARKER_SIZE
        )
        ax.set_ylim(axis_limit.min_y, axis_limit.max_y)
        ax.set_xlim(axis_limit.min_x, axis_limit.max_x)

    elif plot_type == PlotType.HISTOGRAM:
        histogram(ax, data.y)
        total_loc = "center left"

    else:
        raise ValueError(f"unknown plot type: {plot_type.name}")

    fixed_annotation(ax, f"Total Count\n{len(data.x):,}", loc=total_loc)

    if plot_type in (PlotType.HEXBIN, PlotType.SCATTER):
        ax.set_xlabel(data.x.name)
        ax.set_ylabel(data.y.name)

    if data.title is not None and plot_type != PlotType.HISTOGRAM:
        ax.set_title(data.title)


def plot(
    *plot_data: PlotData,
    title: str,
    plot_type: PlotType,
    axis_limit: AxisLimit,
    fit_function: InfillFunction | None = None,
    subplots_kwargs: dict[str, Any] = dict(),
) -> figure.Figure:
    """Create infill hexbin or scatter plots."""
    default_figure_kwargs = dict(figsize=(15, 10), constrained_layout=True)
    default_figure_kwargs.update(subplots_kwargs)

    ncols = len(plot_data) if len(plot_data) > 0 else None
    fig, axes = plt.subplots(ncols=ncols, **default_figure_kwargs)
    fig.suptitle(title)

    for ax, data in zip(axes, plot_data):
        limit = axis_limit.infill(data)
        plot_axes(ax, data, plot_type, limit)

        if plot_type == PlotType.HISTOGRAM:
            continue

        if fit_function is not None:
            x = np.arange(limit.min_x, limit.max_x, 1)
            ax.plot(x, fit_function(x), "--", c="C1", label=fit_function.label)

            if data.outlier_indices is not None and len(data.outlier_indices) > 0:
                ax.scatter(
                    data.x.loc[data.outlier_indices],
                    data.y.loc[data.outlier_indices],
                    marker="+",
                    s=MARKER_SIZE,
                    c="C2",
                    label=f"{len(data.outlier_indices):,} Outliers {data.outlier_label} "
                    f"({len(data.outlier_indices) / len(data.x):.0%})",
                )

            ax.legend(loc="upper right", fontsize="small")

    return fig


def _set_zones_od(data: pd.Series, zones: np.ndarray, value: float) -> pd.Series:
    """Set all zones with origin or destination in `zones` to `value`."""
    existing_zones = np.unique(
        np.concatenate(
            [data.index.get_level_values(i).values for i in ("origin", "destination")]
        )
    )

    new_origins = np.stack(
        [np.repeat(zones, len(existing_zones)), np.tile(existing_zones, len(zones))]
    )
    new_destinations = np.stack(
        [np.repeat(existing_zones, len(zones)), np.tile(zones, len(existing_zones))]
    )
    index_values = np.concatenate([new_origins, new_destinations], axis=1)

    index = pd.MultiIndex.from_arrays(
        [index_values[0], index_values[1]], names=["origin", "destination"]
    )
    update = pd.Series(value, index=index)

    updated = pd.concat([data, update]).groupby(["origin", "destination"]).last()
    updated.name = data.name
    return updated


def infill_metric(
    metric: pd.Series,
    distances: pd.Series,
    plot_file: pathlib.Path,
    method: InfillMethod,
    zero_zones: np.ndarray | None = None,
    outlier_method: OutlierMethod | None = None,
    outlier_cutoff: float | None = None,
) -> pd.Series:
    """Infill given `metric` using `method` given and plot graphs.

    Parameters
    ----------
    metric : pd.Series
        Series of values to infill, with indices
        origin and destination.
    distances : pd.Series
        Crow-fly distances with the same indices as
        `metric`.
    plot_file : pathlib.Path
        Path to save infill graphs to.
    method : InfillMethod
        Method of infilling to use.
    zero_zones: np.ndarray, optional
        List of zones to be excluded from infilling and
        set to 0 after infilling.
    outlier_method: OutlierMethod, optional
        Method for calculating outliers when fitting data.
    outlier_cutoff: float, optional
        Cutoff value to use for outliers, depends
        on `outlier_method`.

    Returns
    -------
    pd.Series
        Infilled `metric`.
    """
    if zero_zones is not None:
        metric = _set_zones_od(metric, zero_zones, np.nan)

    metric = metric.dropna()
    metric.name = re.sub(r"[\s_]+", " ", metric.name).title()

    data = pd.concat([metric, distances], axis=1)

    for column, values in data.items():
        nan_values = values.isna().sum()
        if nan_values > 0:
            LOG.warning("%s Nan values in %s column", f"{nan_values:,}", column)

    before = len(data)
    data = data.dropna(how="any")
    after = len(data)
    message = (
        f"{after:,} ({after / before:.0%}) rows remaining "
        f"after dropping missing values, {before:,} before"
    )
    LOG.info(message)

    infill_indices = distances.index[~distances.index.isin(data.index)]
    infill_function = InfillFunction(
        method,
        data[distances.name],
        data[metric.name],
        outlier_method=outlier_method,
        outlier_cutoff=outlier_cutoff,
    )

    infill_indices = pd.MultiIndex.from_tuples(
        infill_indices.to_flat_index().tolist()
        + infill_function.outlier_indices.to_flat_index().tolist()
    )

    LOG.info(
        "Infilling %s values with %s method, %s of which are outliers",
        f"{len(infill_indices):,}",
        method,
        f"{len(infill_function.outlier_indices):,}",
    )
    calculated = infill_function(distances.loc[infill_indices])

    if calculated.index.isin(metric.index).sum() > 0:
        LOG.warning(
            "Recalculating %s existing metrics",
            calculated.index.isin(metric.index).sum(),
        )

    plot_data = [
        PlotData(
            x=data[distances.name],
            y=data[metric.name],
            title=f"Before Infilling\n{metric.name} vs {distances.name}",
            outlier_indices=infill_function.outlier_indices,
            outlier_label=infill_function.outlier_label,
        )
    ]

    infilled = pd.concat([metric, calculated], axis=0)
    infilled.name = "Infilled " + metric.name
    infilled = infilled.groupby(infilled.index.names).last()

    if zero_zones is not None:
        infilled = _set_zones_od(infilled, zero_zones, 0)

    infilled_data = pd.concat([distances, infilled], axis=1)

    plot_data.append(
        PlotData(
            x=infilled_data.loc[infill_indices, distances.name],
            y=infilled_data.loc[infill_indices, infilled.name],
            title=f"Only {infilled.name} vs {distances.name}",
        )
    )
    plot_data.append(
        PlotData(
            x=infilled_data[distances.name],
            y=infilled_data[infilled.name],
            title=f"After Infilling\n{infilled.name} vs {distances.name}",
        )
    )

    axis_limit = AxisLimit(min_x=0, max_x=200, min_y=0, max_y=None)
    infilled_limits = (axis_limit.infill(i) for i in plot_data)
    max_y = functools.reduce(max, (i.max_y for i in infilled_limits), 0)
    axis_limit = AxisLimit(axis_limit.min_x, axis_limit.max_x, axis_limit.min_y, max_y)

    with backend_pdf.PdfPages(plot_file) as pdf:
        for pt in PlotType:
            fig = plot(
                *plot_data,
                title=f"Infilling Comparison for {plot_file.stem} - {metric.name}",
                plot_type=pt,
                axis_limit=axis_limit,
                fit_function=infill_function,
                subplots_kwargs=dict(sharey=True, figsize=(20, 8)),
            )

            pdf.savefig(fig)
            plt.close(fig)

    LOG.info("Saved plots to %s", plot_file.name)

    LOG.info(
        "Infilled data contains %s valid values (of %s) for '%s'",
        (~infilled.isna()).sum(),
        len(infilled),
        metric.name,
    )

    return infilled


def infill_costs(
    metrics_path: pathlib.Path,
    columns: Mapping[str, float],
    distances: pd.Series,
    output_folder: pathlib.Path,
    methods: list[InfillMethod] | None = None,
    zero_zones: list[int] | None = None,
    outlier_method: OutlierMethod | None = None,
    outlier_cutoff: float | None = None,
) -> None:
    """Infill cost metrics using given `methods` and output graphs.

    Parameters
    ----------
    metrics_path : pathlib.Path
        Cost metrics file for infilling.
    columns : Mapping[str, float]
        Column names for infilling and factors to apply
        before infilling.
    distances : pd.Series
        Crow-fly distances for all OD pairs.
    output_folder : pathlib.Path
        Base folder to save outputs to, sub-folders are
        created for each infill method.
    methods : list[InfillMethod], optional
        Methods of infilling to use, if not given all
        infill methods will be used.
    zero_zones: list[int], optional
        List of zones to be excluded from infilling and
        set to 0 after infilling.
    outlier_method: OutlierMethod, optional
        Method for calculating outliers when fitting data.
    outlier_cutoff: float, optional
        Cutoff value to use for outliers, depends
        on `outlier_method`.

    Raises
    ------
    ValueError
        If distances has missing values.
    """
    LOG.info("Reading '%s'", metrics_path)
    metrics = pd.read_csv(metrics_path, index_col=["origin", "destination"])

    if distances.isna().sum() > 0:
        raise ValueError(f"distances has {distances.isna().sum()} Nan values")

    if methods is None:
        methods = list(InfillMethod)

    if zero_zones is not None:
        zero_zones = np.array(zero_zones)

    for method in methods:
        LOG.info(
            "Using infilling method '%s' on columns: %s",
            method.name,
            [i for i in columns if i in metrics.columns],
        )

        method_folder = output_folder / f"infill - {method.name.lower()}"
        if outlier_method is not None:
            method_folder = method_folder.with_name(
                method_folder.stem + f" ({outlier_method.name})"
            )

        method_folder.mkdir(exist_ok=True, parents=True)
        infilled_metrics = []

        for column, factor in columns.items():
            if column not in metrics.columns:
                LOG.error(
                    "Metric column '%s' not found in '%s'", column, metrics_path.name
                )
                continue

            LOG.info("Multiplying '%s' column by %s before infilling", column, factor)

            assert isinstance(zero_zones, np.ndarray)
            infilled_metrics.append(
                infill_metric(
                    metrics[column] * factor,
                    distances,
                    method_folder / (metrics_path.stem + f"-{column}.pdf"),
                    method,
                    zero_zones=zero_zones,
                    outlier_method=outlier_method,
                    outlier_cutoff=outlier_cutoff,
                )
            )

        infilled_df = pd.concat(infilled_metrics, axis=1)

        out_path = method_folder / (metrics_path.stem + "-infilled.csv")
        infilled_df.to_csv(out_path)
        LOG.info("Written: %s", out_path)

        produce_matrices(infilled_df, out_path)


def produce_matrices(infilled: pd.DataFrame, output_path: pathlib.Path) -> None:
    """Write columns in `infilled` to separate square CSVs.

    Parameters
    ----------
    infilled : pd.DataFrame
        Infilled cost data.
    output_path : pathlib.Path
        Base path to write outputs to (column name will be appended
        when writing the CSVs.)
    """
    for column in infilled:
        data = infilled[column].unstack()

        out_path = output_path.with_name(output_path.stem + f"-{column}.csv")
        data.to_csv(out_path)
        LOG.info("Written: %s", out_path)


def main(
    folder: pathlib.Path,
    params: config.ProcessConfig,
    infill_params: InfillParameters,
) -> None:
    """Infill costs in given OTP `folder`.

    Parameters
    ----------
    folder : pathlib.Path
        Folder containing OTP4GB config and outputs.
    params : config.ProcessConfig
        OTP4GB config from `folder`.
    infill_params : InfillParameters
        Other parameters for controlling the infilling.

    Raises
    ------
    FileNotFoundError
        If the cost metrics file can't be found.
    """
    log_file = folder / f"logs/infill_costs-{datetime.date.today():%Y%m%d}.log"

    details = caf.toolkit.ToolDetails(otp4gb.__package__, otp4gb.__version__)
    with caf.toolkit.LogHelper(otp4gb.__package__, details, log_file=log_file):
        LOG.info("Infilling %s", folder)

        origin_path = config.ASSET_DIR / params.centroids
        destination_path = None
        if params.destination_centroids is not None:
            destination_path = config.ASSET_DIR / params.destination_centroids

        distances = calculate_crow_fly(origin_path, destination_path, None)
        distances = distances / 1000
        distances.name = "Crow-Fly Distance (km)"

        infill_folder_name = f"infilled - {datetime.date.today():%Y%m%d}"

        for time_period in params.time_periods:
            travel_datetime = datetime.datetime.combine(
                params.date, time_period.travel_time
            )
            # Assume time is in local timezone
            travel_datetime = travel_datetime.astimezone()
            LOG.info(
                "Given date / time is assumed to be in local timezone: %s",
                travel_datetime.tzinfo,
            )

            for modes in params.modes:
                matrix_path = folder / (
                    f"costs/{time_period.name}/"
                    f"{'_'.join(modes)}_costs_{travel_datetime:%Y%m%dT%H%M}.csv"
                )
                metrics_path = matrix_path.with_name(matrix_path.stem + "-metrics.csv")

                if not metrics_path.is_file():
                    raise FileNotFoundError(metrics_path)

                infill_folder = metrics_path.parent / infill_folder_name
                infill_folder.mkdir(exist_ok=True)
                LOG.info("Created output folder: %s", infill_folder)

                crow_fly_path = infill_folder / "crow-fly_distances.csv"
                distances.unstack("destination").to_csv(crow_fly_path)
                LOG.info("Written: %s", crow_fly_path.name)

                recalculated_path = matrix_path.with_name(
                    matrix_path.stem + "-recalculated.csv"
                )
                recalculated_metrics_path = recalculated_path.with_name(
                    recalculated_path.stem + "-metrics.csv"
                )
                if not recalculated_metrics_path.is_file():
                    LOG.info("Recalculating costs: '%s'", metrics_path.name)
                    cost.cost_matrix_from_responses(
                        metrics_path.with_name(
                            matrix_path.name + "-response_data.jsonl"
                        ),
                        recalculated_path,
                        params.iterinary_aggregation_method,
                    )

                infill_costs(
                    recalculated_metrics_path,
                    infill_params.infill_columns,
                    distances,
                    infill_folder,
                    methods=infill_params.infill_methods,
                    zero_zones=list(infill_params.zero_cost_zones),
                    outlier_method=infill_params.outlier_method,
                    outlier_cutoff=infill_params.outlier_cutoff,
                )


def _run() -> None:
    args = InfillArgs.parse()

    params = InfillParameters.load_yaml(args.config)

    for folder in params.folders:
        main(folder, config.load_config(folder), params)


if __name__ == "__main__":
    _run()
