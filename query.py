import xarray as xr
import pandas as pd
import numpy as np
from get_whole_period import (
    get_whole_period_between,
    get_last_date_of_month,
    get_total_hours_in_year,
    get_total_hours_in_month,
    get_total_hours_between,
)

long_short_name_dict = {
    "2m_temperature": "t2m",
}


def gen_file_list(
    variable: str,
    start_datetime: str,
    end_datetime: str,
    time_resolution: str,  # e.g., "hour", "day", "month", "year"
    time_agg_method: str,  # e.g., "mean", "max", "min"
):
    file_list = []
    if time_resolution == "hour":
        start_year = start_datetime[:4]
        end_year = end_datetime[:4]
        for year in range(int(start_year), int(end_year) + 1):
            file_path = f"/data/era5/raw/{variable}/{variable}-{year}.nc"
            file_list.append(file_path)
    else:
        file_path = (
            f"/home/huan1531/iharp-quick-aggregate/data/output/{variable}-{time_resolution}-{time_agg_method}.nc"
        )
        file_list.append(file_path)
    print(file_list)
    return file_list


def get_raster(
    variable: str,
    start_datetime: str,
    end_datetime: str,
    time_resolution: str,  # e.g., "hour", "day", "month", "year"
    time_agg_method: str,  # e.g., "mean", "max", "min"
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    # spatial_resolution: float,  # e.g., 0.25, 0.5, 1.0, 2.5, 5.0
):
    file_list = gen_file_list(variable, start_datetime, end_datetime, time_resolution, time_agg_method)
    ds_list = []
    for file in file_list:
        ds = xr.open_dataset(file, engine="netcdf4").sel(
            time=slice(start_datetime, end_datetime),
            latitude=slice(max_lat, min_lat),
            longitude=slice(min_lon, max_lon),
        )
        ds_list.append(ds)
    ds = xr.concat(ds_list, dim="time")
    return ds


def get_timeseries(
    variable: str,
    start_datetime: str,
    end_datetime: str,
    time_resolution: str,  # e.g., "hour", "day", "month", "year"
    time_agg_method: str,  # e.g., "mean", "max", "min"
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
    time_series_aggregation_method: str,  # e.g., "mean", "max", "min"
):
    ds = get_raster(
        variable,
        start_datetime,
        end_datetime,
        time_resolution,
        time_agg_method,
        min_lat,
        max_lat,
        min_lon,
        max_lon,
    )
    if time_series_aggregation_method == "mean":
        ts = ds.mean(dim=["latitude", "longitude"])
    elif time_series_aggregation_method == "max":
        ts = ds.max(dim=["latitude", "longitude"])
    elif time_series_aggregation_method == "min":
        ts = ds.min(dim=["latitude", "longitude"])
    return ts


def get_mean_heatmap(
    variable: str,
    start_datetime: str,
    end_datetime: str,
    min_lat: float,
    max_lat: float,
    min_lon: float,
    max_lon: float,
):
    years, months, days, hours = get_whole_period_between(start_datetime, end_datetime)
    year_hours = []
    month_hours = []
    day_hours = []
    hour_hours = []
    xrds_list = []

    if years:
        year = xr.open_dataset(f"/home/huan1531/iharp-quick-aggregate/data/output/{variable}-year-mean.nc")
        year_match = [f"{y}-12-31 00:00:00" for y in years]
        year_selected = year.sel(time=year_match, latitude=slice(max_lat, min_lat), longitude=slice(min_lon, max_lon))
        year_hours = [get_total_hours_in_year(y) for y in years]
        xrds_list.append(year_selected)

    if months:
        month = xr.open_dataset(f"/home/huan1531/iharp-quick-aggregate/data/output/{variable}-month-mean.nc")
        month_match = [f"{m}-{get_last_date_of_month(pd.Timestamp(m))} 00:00:00" for m in months]
        month_selected = month.sel(
            time=month_match, latitude=slice(max_lat, min_lat), longitude=slice(min_lon, max_lon)
        )
        month_hours = [get_total_hours_in_month(m) for m in months]
        xrds_list.append(month_selected)

    if days:
        day = xr.open_dataset(f"/home/huan1531/iharp-quick-aggregate/data/output/{variable}-day-mean.nc")
        day_match = [f"{d} 00:00:00" for d in days]
        day_selected = day.sel(time=day_match, latitude=slice(max_lat, min_lat), longitude=slice(min_lon, max_lon))
        day_hours = [24 for _ in days]
        xrds_list.append(day_selected)

    if hours:
        year_hour_dict = {}
        for h in hours:
            year = h.split("-")[0]
            if year not in year_hour_dict:
                year_hour_dict[year] = []
            year_hour_dict[year].append(h)

        ds_list = []
        for y in year_hour_dict:
            file_path = f"/data/era5/raw/{variable}/{variable}-{y}.nc"
            ds = xr.open_dataset(file_path, engine="netcdf4").sel(
                time=year_hour_dict[y], latitude=slice(max_lat, min_lat), longitude=slice(min_lon, max_lon)
            )
            ds_list.append(ds)
        hour_selected = xr.concat(ds_list, dim="time")
        hour_hours = [1 for _ in hours]
        xrds_list.append(hour_selected)

    xrds_concat = xr.concat(xrds_list, dim="time")
    nd_array = xrds_concat["t2m"].to_numpy()
    weights = np.array(year_hours + month_hours + day_hours + hour_hours)
    total_hours = get_total_hours_between(start_datetime, end_datetime)
    weights = weights / total_hours
    average = np.average(nd_array, axis=0, weights=weights)
    res = xr.Dataset(
        {long_short_name_dict[variable]: (["latitude", "longitude"], average)},
        coords={"latitude": xrds_concat.latitude, "longitude": xrds_concat.longitude},
    )
    return res
