try:
    from importlib.metadata import PackageNotFoundError, version
except ImportError:
    # for python < 3.8 (remove when dropping 3.7 support)
    from importlib_metadata import PackageNotFoundError, version

try:
    __version__ = version(__package__)
except PackageNotFoundError:
    __version__ = "0+unknown"


from inspire_oedi_access.main import downloadAgriPVData, concatenateData
from inspire_oedi_access.main import load_lookup_table, open_zarr_dataset, load_data_by_gid, load_data_by_gid_multiple_setups, find_nearest_gid, load_data_by_lat_lon, load_data_by_lat_lon_multiple_setups, load_data_by_lat_lon_range, load_data_by_lat_lon_range_multiple_setups