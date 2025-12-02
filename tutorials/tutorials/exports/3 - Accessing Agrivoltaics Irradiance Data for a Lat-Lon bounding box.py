#!/usr/bin/env python
# coding: utf-8

# # 3 - Accessing Agrivoltaics Irradiance Data for a Lat-Lon bounding box
# 
# This notebook shows how to:
# 
# 1. Specify a latitude/longitude bounding box (e.g., a state or region).
# 2. Use the lookup table to identify all GIDs in that region.
# 3. Load and combine irradiance data from multiple setups for those GIDs.
# 4. Inspect the resulting dataset and list the included locations.

# ## 1. Setup

# In[ ]:


# if running on google colab, uncomment the next line and execute this cell to install the dependencies and prevent "ModuleNotFoundError" in later cells:
# !pip install inspire_openei_access


# In[1]:


import xarray as xr
import pandas as pd

from agrivoltaics_io import (
    LOOKUP_TABLE_PATH,
    load_lookup_table,
    load_data_by_lat_lon_range_multiple_setups,
)


# ## Load the lookup table

# In[2]:


lookup_df = load_lookup_table()
lookup_df.head()


# Optionally show columns:

# In[ ]:


lookup_df.columns


# ## Define the bounding box
# 
# Here we use a bounding box roughly corresponding to Colorado. You can adapt
# these latitude/longitude limits to any region of interest.

# In[ ]:


# Approximate bounding box for Colorado
colorado_lat_min = 37.0
colorado_lat_max = 41.0
colorado_lon_min = -109.0
colorado_lon_max = -102.0

# Setups to include
setup_nums = [1, 2, 3]

(
    colorado_lat_min,
    colorado_lat_max,
    colorado_lon_min,
    colorado_lon_max,
    setup_nums,
)


# In[ ]:


# Load data for GIDS in range
(
    data_by_range,
    gids_in_range,
    matching_gids_dict,
) = load_data_by_lat_lon_range_multiple_setups(
    colorado_lat_min,
    colorado_lat_max,
    colorado_lon_min,
    colorado_lon_max,
    setup_nums,
    lookup_df=lookup_df,
)

if data_by_range is None:
    print("WARNING: No GIDs found in the specified range")
else:
    print("Number of GIDs found in range:", len(gids_in_range))
    print("\nMatching GIDs by setup:")
    for setup_num, matching_gids in matching_gids_dict.items():
        print(f"  Setup {setup_num}: {len(matching_gids)} GIDs")


# In[ ]:


# Inpsect GID table

if data_by_range is not None:
    # Show first 10 GIDs and their coordinates
    gids_in_range[["gid", "latitude", "longitude"]].head(10)


# If you want to know how many more:

# In[ ]:


if data_by_range is not None and len(gids_in_range) > 10:
    print(f"... and {len(gids_in_range) - 10} more GIDs in this range.")


# ## Inspect combined dataset

# In[ ]:


if data_by_range is not None:
    print("Combined dataset dimensions:")
    display(dict(data_by_range.sizes))

    print("\nSetups included:")
    display(data_by_range.setup.values.tolist())

    display(data_by_range)


# In[ ]:


## Aggregate/visualize


# In[ ]:


import matplotlib.pyplot as plt

if data_by_range is not None and "ground_irradiance" in data_by_range.data_vars:
    gi = data_by_range["ground_irradiance"]

    # Example: mean over distance and setup, first 500 time steps
    gi_mean = gi.mean(dim=["distance", "setup"]).isel(time=slice(0, 500))

    plt.figure()
    gi_mean.mean(dim="gid").plot()
    plt.title("Mean Ground Irradiance (all GIDs in range)")
    plt.ylabel("Irradiance")
    plt.xlabel("Time")
    plt.show()


# ## Tips and next steps
# 
# - Modify the bounding box to focus on smaller regions or specific project areas.
# - Use the `gids_in_range` DataFrame to:
#   - Filter GIDs by latitude/longitude.
#   - Join with external metadata (e.g., site characteristics).
# - Subset `data_by_range` by GID:
#   ```python
#   some_gid = gids_in_range["gid"].iloc[0]
#   subset = data_by_range.sel(gid=some_gid)

# Save the region subset to a local file:

# In[ ]:


data_by_range.to_netcdf("colorado_region.nc")


