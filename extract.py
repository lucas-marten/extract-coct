import argparse
import os
from datetime import datetime, timedelta

import pandas as pd
import xarray as xr


def parse_arguments():
    parser = argparse.ArgumentParser(description="Extract climate data.")
    parser.add_argument("--date", type=str, help="Date in ISO format (YYYY-MM-DD)")
    parser.add_argument("--point", type=str, help="Path to CSV file with points")
    parser.add_argument(
        "--output", type=str, help="Directory to save the output CSV files"
    )
    return parser.parse_args()


def main(date, points, output_dir):
    points = pd.read_csv(points)

    os.makedirs(output_dir, exist_ok=True)

    variables = ["total_precipitation", "2m_air_temperature"]
    for variable in variables:
        path_in = date.strftime(
            f"/data/forecast/chimera_as/{variable}/%Y/%j/chimera_as_{variable}_M000_%Y%m%d00.nc"
        )

        lats = points.lat.to_list()
        lons = points.lon.to_list()
        ids = points.name.to_list()

        ds = xr.open_dataarray(path_in)
        ds_times = pd.to_datetime(ds.time.values) - timedelta(hours=3)

        dfs = list()
        for i in range(len(ids)):
            prec_values = ds.sel(
                longitude=lons[i], latitude=lats[i], method="nearest"
            ).values
            df = pd.DataFrame({ids[i]: prec_values}, index=ds_times)
            dfs.append(df)

    path_out = os.path.join(output_dir, f"{variable}.csv")
    os.makedirs(os.path.dirname(path_out), exist_ok=True)
    pd.concat(dfs, axis=1).to_csv(path_out)
    print(f"created: {path_out}")

    return True


if __name__ == "__main__":
    args = parse_arguments()
    date = datetime.fromisoformat(str(args.date))
    points = args.points
    output_dir = args.output_dir
    main(date, points, output_dir)
