import xarray as xr


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


# def get_raster(
#     variable: str,
#     start_datetime: str,
#     end_datetime: str,
#     time_resolution: str,  # e.g., "hour", "day", "month", "year"
#     time_agg_method: str,  # e.g., "mean", "max", "min"
#     min_lat: float,
#     max_lat: float,
#     min_lon: float,
#     max_lon: float,
#     # spatial_resolution: float,  # e.g., 0.25, 0.5, 1.0, 2.5, 5.0
# ):
#     file_list = gen_file_list(variable, start_datetime, end_datetime, time_resolution, time_agg_method)
#     ds = xr.open_mfdataset(file_list, engine="netcdf4", parallel=True, chunks={"time": 100})
#     ds = ds.sel(
#         time=slice(start_datetime, end_datetime),
#         latitude=slice(max_lat, min_lat),
#         longitude=slice(min_lon, max_lon),
#     )
#     return ds


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
    file_list = gen_file_list(variable, start_datetime, end_datetime, time_resolution, time_agg_method)
    ds = xr.open_mfdataset(file_list, engine="netcdf4", parallel=True, chunks={"time": 100})
    ds = ds.sel(
        time=slice(start_datetime, end_datetime),
        latitude=slice(max_lat, min_lat),
        longitude=slice(min_lon, max_lon),
    )
    if time_series_aggregation_method == "mean":
        ts = ds.mean(dim=["latitude", "longitude"])
    elif time_series_aggregation_method == "max":
        ts = ds.max(dim=["latitude", "longitude"])
    elif time_series_aggregation_method == "min":
        ts = ds.min(dim=["latitude", "longitude"])
    return ts.compute()
