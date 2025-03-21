#!/airflow/tools/extract-coct/venv/bin/python3
import argparse
import os
from datetime import datetime, timedelta

import pandas as pd
import xarray as xr

import send_mail


def parse_arguments():
    parser = argparse.ArgumentParser(description="Extract climate data.")
    parser.add_argument("--date", type=str, help="Date in ISO format (YYYY-MM-DD)")
    parser.add_argument(
        "--output_dir", type=str, help="Directory to save the output CSV files"
    )
    return parser.parse_args()

def clear_old_files(output_dir):

    for file in os.listdir(output_dir):
        if file.endswith(".xlsx"):
            os.remove(os.path.join(output_dir, file))

    return

def main(date, output_dir):
    stacks = [
        "capitais.csv",
        "eptvmg.csv",
        "eptvsc.csv",
        "rpc.csv",
        "tvmorena.csv",
        "eptvkp.csv",
        "eptvrp.csv",
        "rbs.csv",
        "tvca.csv",
    ]

    init = date.strftime("%Y%m%dT00")
    end = (date + timedelta(days=2)).strftime("%Y%m%dT23")
    output_dir = date.strftime(output_dir)

    os.makedirs(output_dir, exist_ok=True)

    clear_old_files(output_dir)

    outputs = list()
    variables = ["total_precipitation", "2m_air_temperature"]
    for stack in stacks:
        path_points = f"/airflow/tools/extract-coct/static/{stack}"
        df_points = pd.read_csv(path_points)
        points_name = os.path.basename(path_points).split(".")[0]
        path_out = os.path.join(output_dir, f"{points_name}.xlsx")
        os.makedirs(os.path.dirname(path_out), exist_ok=True)
        for variable in variables:
            args = {'path':path_out, 'mode':'a', 'if_sheet_exists':"replace"} if os.path.isfile(path_out) \
                    else {'path':path_out, 'mode':'w', 'if_sheet_exists':None}
            with pd.ExcelWriter(**args) as writer:
                path_in = date.strftime(
                    f"/data/forecast/chimera_as/{variable}/%Y/%j/chimera_as_{variable}_M000_%Y%m%d00.nc"
                )

                lats = df_points.lat.to_list()
                lons = df_points.lon.to_list()
                ids = df_points.name.to_list()

                ds = xr.open_dataarray(path_in).sel(time=slice(init, end))
                ds_times = pd.to_datetime(ds.time.values)

                dfs = list()
                for i in range(len(ids)):
                    prec_values = ds.sel(
                        longitude=lons[i], latitude=lats[i], method="nearest"
                    ).values
                    df = pd.DataFrame({ids[i]: prec_values}, index=ds_times)
                    dfs.append(df.round(2))

                
                df_concat = pd.concat(dfs, axis=1)
                df_concat.to_excel(writer, sheet_name=variable)

        outputs.append(path_out)
        print(f"created: {path_out}")

    return outputs


if __name__ == "__main__":
    args = parse_arguments()
    date = datetime.fromisoformat(str(args.date))
    outputs = main(date, args.output_dir)
    send_mail.run(outputs, subject="Extração - COCT")
