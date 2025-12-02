#!/usr/bin/env python
# coding: utf-8

# # 2 - Accessing Agrivoltaics Irradiance Data by Nearest Lat/Lon
# 
# This notebook demonstrates how to:
# 
# 1. Load the GID lookup table.
# 2. Find the nearest grid cell (GID) to a given latitude/longitude.
# 3. Load irradiance data from multiple setups for that nearest GID.

# ## 1. Setup

# In[ ]:


# if running on google colab, uncomment the next line and execute this cell to install the dependencies and prevent "ModuleNotFoundError" in later cells:
# !pip install inspire_openei_access


# In[1]:


import xarray as xr
import pandas as pd

from agrivoltaics_io import (
    S3_BUCKET_PATH,
    LOOKUP_TABLE_PATH,
    load_lookup_table,
    load_data_by_lat_lon_multiple_setups,
)


# ## Load the lookup table

# In[2]:


lookup_df = load_lookup_table()
lookup_df.head()


# Optionally show columns:

# In[ ]:


lookup_df.columns


# ## Choose a target location
# 
# Specify the latitude/longitude of interest and the setups you want to include.
# In this example we use a location near Denver, Colorado.

# In[ ]:


# Example target location (Denver area)
target_lat = 39.7392
target_lon = -104.9903

# Setups to include in the combined dataset
setup_nums = [1, 2, 3]

target_lat, target_lon, setup_nums


# In[ ]:


# Find neartes GID and load data

(
    data_by_latlon,
    nearest_gid,
    distance,
    nearest_lat,
    nearest_lon,
) = load_data_by_lat_lon_multiple_setups(
    target_lat, target_lon, setup_nums, lookup_df=lookup_df
)

print("Nearest GID:", nearest_gid)
print(f"Nearest location: ({nearest_lat:.4f}, {nearest_lon:.4f})")
print(f"Approximate distance to target (units depend on implementation): {distance}")


# In[ ]:


# Inspect combined dataset
if data_by_latlon is not None:
    print("Dataset dimensions:")
    display(dict(data_by_latlon.sizes))

    print("\nSetups included:")
    display(data_by_latlon.setup.values.tolist())

    display(data_by_latlon)
else:
    print("No data returned for this location.")
    


# ## Example: Time series plot
# 
# If you want a quick plot:

# In[ ]:


import matplotlib.pyplot as plt

if data_by_latlon is not None and "ground_irradiance" in data_by_latlon.data_vars:
    gi = data_by_latlon["ground_irradiance"]

    # Example: take the first setup and first distance
    ts = gi.isel(setup=0, distance=0)

    plt.figure()
    ts.plot()
    plt.title("Ground Irradiance Time Series (nearest GID)")
    plt.ylabel("Irradiance")
    plt.xlabel("Time")
    plt.show()


# ## Tips and next steps
# 
# - Change `target_lat` and `target_lon` to query a different location.
# - Adjust `setup_nums` to explore other experimental configurations.
# - Use `.sel(setup=...)` or `.isel(distance=...)` to select individual setups or
#   positions in the agrivoltaic layout.
# - You can save the subset to NetCDF or Zarr for local analysis:
#   ```python
#   data_by_latlon.to_netcdf("nearest_location.nc")
