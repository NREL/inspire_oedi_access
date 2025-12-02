#!/usr/bin/env python
# coding: utf-8

# # 1 - Accessing Agrivoltaics Irradiance Data by GID
# 
# This notebook shows how to load agrivoltaics irradiance data stored as Zarr
# files on S3 when you already know the spatial indices (`gid`). We also show
# how to:
# 
# - Select multiple experimental setups (e.g., different row spacings, heights).
# - Combine data from several setups into one `xarray.Dataset`.
# - Inspect the available variables and dimensions.
# 
# Steps:
# * <a href='#step1'>1. Setup</a>
# * <a href='#step2'>2. Download data</a>
# * <a href='#step3'>3. Load and plot data from the file</a>

# In[3]:


# if running on google colab, uncomment the next line and execute this cell to install the dependencies and prevent "ModuleNotFoundError" in later cells:
# !pip install inspire_openei_access


# In[1]:


import xarray as xr
import pandas as pd

from agrivoltaics_io import (
    S3_BUCKET_PATH,
    load_data_by_gid_multiple_setups,
)


# <a id='step1'></a>

# ## Choose setups and GIDs
# 
# Each Zarr store on S3 corresponds to a given **setup** (e.g. 01, 02, 03, ...).
# Here we’ll load data for a small list of GIDs across several setups.

# <div class="alert alert-block alert-info">
# <b>System ID for the Webinar:</b> We will be using location 243498 and 886847.</div>

# In[1]:


# Setups we want to use (you can change these)
setup_nums = [1, 2, 3]

# Example GIDs (replace with your own list as needed)
example_gids = [243498, 886847]

setup_nums, example_gids


# In[3]:


# Preview ZARR Paths

zarr_paths = [
    f"s3://{S3_BUCKET_PATH}/preliminary_{setup_num:02d}.zarr"
    for setup_num in setup_nums
]
zarr_paths


# <a id='step2'></a>

# In[ ]:


# Load Data by GID
data_by_gid, matching_gids_dict = load_data_by_gid_multiple_setups(
    setup_nums, example_gids
)

if data_by_gid is None:
    print(f"WARNING: None of the requested GIDs {example_gids} are in these datasets")
else:
    print("Matching GIDs by setup:")
    for setup_num, matching_gids in matching_gids_dict.items():
        print(f"  Setup {setup_num}: {matching_gids}")



# In[ ]:


# Inspect hte combined dataset

if data_by_gid is not None:
    print("Dataset dimensions:")
    display(dict(data_by_gid.sizes))

    print("\nData variables:")
    display(list(data_by_gid.data_vars))

    # Show basic dataset summary
    display(data_by_gid)


# In[ ]:


# Inspect ground_irradiance

if data_by_gid is not None and "ground_irradiance" in data_by_gid.data_vars:
    gi = data_by_gid["ground_irradiance"]
    print("`ground_irradiance` shape:", gi.shape)
    print("`ground_irradiance` dims:", gi.dims)

    # Example: sample a single point (first setup, first time, first distance)
    sample = gi.isel(setup=0, time=0, distance=0)
    print("\nSample value (setup=0, time=0, distance=0):")
    display(sample)


# ## Tips and next steps
# 
# - Change `setup_nums` to access different experimental setups (e.g. 1–10).
# - Use `data_by_gid.sel(setup=1)` to focus on a single setup.
# - Explore additional variables in `data_by_gid.data_vars`, such as:
#   - `direct_irradiance`
#   - `diffuse_irradiance`
#   - any other fields stored in the Zarr.
# - Use time- or distance-based subsetting, for example:
#   ```python
#   subset = data_by_gid.sel(time=slice("2020-06-01", "2020-06-30"))
