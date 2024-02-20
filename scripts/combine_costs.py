# -*- coding: utf-8 -*-
"""Script to average cost matrices together."""

##### IMPORTS #####
from __future__ import annotations

import argparse
import logging
import pathlib
import sys

import numpy as np
import pandas as pd
import pydantic
from caf import toolkit
from pydantic import dataclasses, types

sys.path.extend((".", ".."))
# pylint: disable=wrong-import-position
import otp4gb
from otp4gb import config
from scripts import infill_costs

# pylint: enable=wrong-import-position


##### CONSTANTS #####
LOG = logging.getLogger(otp4gb.__package__ + ".combine_costs")


##### CLASSES #####
@dataclasses.dataclass
class CombineCostsArgs:
    """Arguments for `infill_costs` module."""

    config: types.FilePath

    @classmethod
    def parse(cls) -> CombineCostsArgs:
        """Parse command line arguments."""
        parser = argparse.ArgumentParser(
            description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument(
            "config", type=pathlib.Path, help="path to combine costs config file"
        )

        args = parser.parse_args()
        return CombineCostsArgs(config=args.config)


def _join_paths(base: pathlib.Path | str, end: pathlib.Path | str) -> pathlib.Path:
    """Join paths together if `end` isn't an absolute path."""
    base = pathlib.Path(base)
    end = pathlib.Path(end)

    if end.is_absolute():
        return end
    return base / end


@dataclasses.dataclass
class CostFiles:
    base: types.FilePath
    other: types.FilePath
    output: pathlib.Path

    @staticmethod
    def build_with_base(
        base_folder: pathlib.Path,
        base: pathlib.Path,
        other: pathlib.Path,
        output: pathlib.Path,
    ) -> CostFiles:
        return CostFiles(
            base=_join_paths(base_folder, base),
            other=_join_paths(base_folder, other),
            output=_join_paths(base_folder, output),
        )


@dataclasses.dataclass
class CrowFlyCutoff:
    base: float
    other: float

    _range: float | None = None

    @pydantic.root_validator
    def _check_order(cls, values: dict[str, float]) -> dict[str, float]:
        if values["base"] >= values["other"]:
            raise ValueError(
                f"base cutoff ({values['base']}) should be smaller "
                f"than other cutoff ({values['other']})"
            )

        return values

    @property
    def range(self) -> float:
        """Difference between `base` and `other`."""
        if self._range is None:
            self._range = self.other - self.base
        return self._range


class CombineCostsParameters(toolkit.BaseConfig):
    # Any paths given are relative to this base folder
    base_folder: types.DirectoryPath = pathlib.Path()
    # OTP4GB-py config to to use for centroids data
    otp4gb_config_path: types.FilePath
    output_folder: types.DirectoryPath
    # Paths for cost files relative to cost folders
    costs_to_combine: list[CostFiles]
    crow_fly_cutoff_km: CrowFlyCutoff

    @pydantic.validator("otp4gb_config_path", "output_folder", pre=True)
    def _build_path(
        cls, value, field: pydantic.fields.ModelField, values: dict
    ) -> pathlib.Path:
        # pylint: disable=no-self-argument
        base = values.get("base_folder")
        assert isinstance(base, pathlib.Path)

        path = _join_paths(base, value)

        if field.name == "output_folder":
            path.mkdir(exist_ok=True, parents=True)

        return path

    @pydantic.validator("costs_to_combine", pre=True, each_item=True)
    def _build_cost_paths(cls, value: dict, values: dict) -> CostFiles:
        # pylint: disable=no-self-argument
        if not isinstance(value, dict):
            raise TypeError(f"expected mapping not {type(value)}")

        missing = [i for i in ("base", "other", "output") if i not in value]
        if missing:
            raise KeyError(f"missing: {missing}")

        base = values.get("base_folder")
        out_folder = values.get("output_folder")
        assert isinstance(base, pathlib.Path)
        assert isinstance(out_folder, pathlib.Path)

        return CostFiles(
            base=_join_paths(base, value["base"]),
            other=_join_paths(base, value["other"]),
            output=_join_paths(out_folder, value["output"]),
        )


##### FUNCTIONS #####
def load_costs(path: pathlib.Path, zones: np.ndarray) -> pd.Series:
    LOG.info("Loading costs from %s", path.name)
    data = pd.read_csv(path, index_col=0, dtype=float)

    data.index = pd.to_numeric(data.index, downcast="unsigned")
    data.columns = pd.to_numeric(data.columns, downcast="unsigned")

    data.index.name = "origin"
    data.columns.name = "destination"

    data = data.sort_values("origin").sort_values("destination", axis=1)

    zones = np.sort(zones)

    message = []
    for name in ("index", "columns"):
        missing = zones[~np.isin(zones, getattr(data, name))]

        if len(missing) > 0:
            message.append(f"{len(missing)} from {name}")

    if len(message) > 0:
        raise ValueError("missing " + " and ".join(message))

    LOG.info("Loaded a cost matrix of shape %s", data.shape)
    return data.stack()


def main(parameters: CombineCostsParameters) -> None:
    log_file = parameters.output_folder / "infill_costs.log"

    details = toolkit.ToolDetails(otp4gb.__package__, otp4gb.__version__)
    with toolkit.LogHelper(otp4gb.__package__, details, log_file=log_file):
        LOG.info("Combining cost matrices")

        output_path = parameters.output_folder / "combine_costs_config.yml"
        parameters.save_yaml(output_path)
        LOG.debug("Saved parameters to %s", output_path)

        otp_params = config.load_config(parameters.otp4gb_config_path.parent)

        origin_path = config.ASSET_DIR / otp_params.centroids
        destination_path = None
        if otp_params.destination_centroids is not None:
            destination_path = config.ASSET_DIR / otp_params.destination_centroids

        distances = infill_costs.calculate_crow_fly(origin_path, destination_path, None)
        distances = distances / 1000
        distances.name = "Crow-Fly Distance (km)"

        zones = distances.index.get_level_values("origin").unique().values
        cutoffs = parameters.crow_fly_cutoff_km

        for i, cost_files in enumerate(parameters.costs_to_combine, 1):
            LOG.info("Combining costs %s", i)
            base_cost = load_costs(cost_files.base, zones)
            base_cost.name = "base_cost"
            other_cost = load_costs(cost_files.other, zones)
            other_cost.name = "other_cost"
            costs = pd.concat([base_cost, other_cost], axis=1)

            combined_cost = pd.Series(
                np.nan, index=distances.index, name="combined_cost"
            )

            # Assign values outside cutoff range to individual cost
            mask = distances <= cutoffs.base
            combined_cost.loc[mask] = base_cost[mask]
            mask = distances >= cutoffs.other
            combined_cost.loc[mask] = other_cost[mask]

            # Calculates weighted average for distances inside cutoff range
            mask = (cutoffs.base < distances) & (distances < cutoffs.other)

            # Weights are based on the difference from the cutoffs to the crow-fly distance
            # i.e. the base weight is the difference from the other cutoff so values
            # closer to other cutoff have a lower base weight
            base_weight = cutoffs.other - distances[mask]
            base_weight.name = "base_weight"
            other_weight = distances[mask] - cutoffs.base
            other_weight.name = "other_weight"
            weights = pd.concat([base_weight, other_weight], axis=1)

            combined_cost.loc[mask] = np.average(costs[mask], axis=1, weights=weights)

            costs.loc[:, "mean_cost"] = costs.mean(axis=1)
            metrics = pd.concat([distances, weights, costs, combined_cost], axis=1)

            cost_files.output.parent.mkdir(exist_ok=True)

            combined_cost = combined_cost.unstack("destination")
            combined_cost.to_csv(cost_files.output)
            LOG.info("Written combined cost: %s", cost_files.output)

            out_path = cost_files.output.with_name(
                cost_files.output.stem + "-calculation.csv"
            )
            metrics.to_csv(out_path)
            LOG.info("Written calculation values to: %s", out_path)
            LOG.info("Finished combining %s", i)


def _run() -> None:
    arguments = CombineCostsArgs.parse()

    parameters = CombineCostsParameters.load_yaml(arguments.config)
    main(parameters)


##### MAIN #####
if __name__ == "__main__":
    _run()
