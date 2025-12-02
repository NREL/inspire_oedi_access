import os
import boto3
import botocore
import argparse
from botocore.handlers import disable_signing
import pandas as pd

def downloadAgriPVData(state, path, file_type='csv'):
    '''
    DEPRECATED Method to access and pull data from the OEDI Data Lake for Inspire AgriPV geospatial data

    Parameters:
    -----------------------
    system_id : str - system id value found from query of OEDI PVDAQ queue 
    of available .
    path : str - local system location files are to be stored in.
    file_type : str - default is .csv, but parquet canbe passed in as option
    
    Returns
    -----------------------
    void
    
    '''
    s3 = boto3.resource("s3")
    s3.meta.client.meta.events.register("choose-signer.s3.*", disable_signing)
    bucket = s3.Bucket("oedi-data-lake")
    
    http://oedi-data-lake/inspire/agrivoltaics_irradiance/
    #Find each target file in buckets
    objects = bucket.objects.filter(
        Prefix="inspire/agrivoltaics_irradiance/" + state + file_type)
        # prefix =  "pvdaq/2023-solar-data-prize/" +  target_dir + "_OEDI/data/"


    for obj in objects:
        try:
            bucket.download_file(obj.key, os.path.join(path, os.path.basename(obj.key)))
        except botocore.exceptions.ClientError as e:
            print ('ERROR: Boto3 exception ' + str(e))
        else:
            print ('File ' + os.path.join(path, os.path.basename(obj.key)) + " downloaded successfully.")
            
    return


def concatenateData(state_id, path):
    '''
    DEPRECATED Method to merge the multiple files coming in from OEDI
    Parameters:
    -----------------------
    state_id : str - state id value found from query of OEDI PVDAQ queue 
    of available .
    path : str - local system location files are to be stored in.
    
    Returns
    -----------------------
    void
    
    '''
    dfs = []
    #get list of files in directory
    file_list=os.listdir(path)
    # column_name = 'sensor_name'
    #Build a dataframe from current file
    print ("Starting data extraction")
    for file in file_list:
        print("Extracting file " + file)
        df_file= pd.read_csv(path + '/' + file)
        dfs.append(df_file)
         
    #Build the master data frame from the assembled individual frames.
    print ("Concatenating all files")
    df = pd.concat(dfs, ignore_index=True)
    target_outputfile = path + "/state_" + state_id + "_data.csv"
    print ("File is " + target_outputfile)
    df.to_csv(target_outputfile, sep=",", index=False)
    return


# S3 bucket configuration
S3_BUCKET_PATH = "oedi-data-lake/inspire/agrivoltaics_irradiance/v1.1"
LOOKUP_TABLE_PATH = f"s3://{S3_BUCKET_PATH}/gid-lat-lon.csv"


def load_lookup_table():
    """
    Load the GID to lat/lon lookup table from S3.
    
    Returns
    -------
    pd.DataFrame
        DataFrame with columns: gid, latitude, longitude
    """
    df = pd.read_csv(LOOKUP_TABLE_PATH, index_col=0)
    
    # Reset index to make GID a column
    df = df.reset_index(names='gid')
    
    return df


def open_zarr_dataset(setup_num, s3_bucket_path=S3_BUCKET_PATH):
    """
    Open a zarr dataset for a specific setup from S3.
    
    Parameters
    ----------
    setup_num : int
        Setup number (1-10)
    s3_bucket_path : str
        S3 path to the zarr files directory
    
    Returns
    -------
    xr.Dataset
        Opened xarray dataset
    """
    zarr_filename = f"preliminary_{setup_num:02d}.zarr"
    zarr_path = f"s3://{s3_bucket_path}/{zarr_filename}"
    
    # Create fsspec mapper for S3 (anonymous access)
    mapper = fsspec.get_mapper(zarr_path, anon=True)
    
    # Open zarr dataset
    ds = xr.open_zarr(mapper)
    
    return ds


def load_data_by_gid(setup_num, gids, s3_bucket_path=S3_BUCKET_PATH):
    """
    Load data for specific GIDs from a setup.
    
    Parameters
    ----------
    setup_num : int
        Setup number (1-10)
    gids : list of int
        List of GIDs to load
    s3_bucket_path : str
        S3 path to the zarr files directory
    
    Returns
    -------
    xr.Dataset
        Dataset subset containing only the specified GIDs, or None if no matching GIDs found
    list
        List of matching GIDs found in the dataset
    """
    # Open the zarr dataset
    ds = open_zarr_dataset(setup_num, s3_bucket_path)
    
    # Get all GIDs in the dataset
    dataset_gids = ds['gid'].values
    
    # Find which requested GIDs are in this dataset
    gid_mask = np.isin(dataset_gids, gids)
    matching_gids = dataset_gids[gid_mask].tolist()
    
    if len(matching_gids) == 0:
        return None, []
    
    # Get indices of matching GIDs
    gid_indices = np.where(gid_mask)[0]
    
    # Select data for matching GIDs
    selected_data = ds.isel(gid=gid_indices)
    
    return selected_data, matching_gids


def load_data_by_gid_multiple_setups(setup_nums, gids, s3_bucket_path=S3_BUCKET_PATH):
    """
    Load data for specific GIDs from multiple setups and combine them.
    
    Parameters
    ----------
    setup_nums : list of int
        List of setup numbers (1-10)
    gids : list of int
        List of GIDs to load
    s3_bucket_path : str
        S3 path to the zarr files directory
    
    Returns
    -------
    xr.Dataset
        Combined dataset with a 'setup' dimension, or None if no matching GIDs found
    dict
        Dictionary mapping setup numbers to lists of matching GIDs found in each dataset
    """
    datasets = []
    matching_gids_dict = {}
    
    for setup_num in setup_nums:
        data, matching_gids = load_data_by_gid(setup_num, gids, s3_bucket_path)
        if data is not None:
            # Add setup dimension
            data = data.expand_dims('setup')
            data = data.assign_coords(setup=[setup_num])
            datasets.append(data)
            matching_gids_dict[setup_num] = matching_gids
    
    if len(datasets) == 0:
        return None, {}
    
    # Concatenate along setup dimension
    combined_data = xr.concat(datasets, dim='setup')
    
    return combined_data, matching_gids_dict


def find_nearest_gid(latitude, longitude, lookup_df=None):
    """
    Find the nearest GID for a given latitude/longitude using nearest neighbor search.
    
    Parameters
    ----------
    latitude : float
        Target latitude
    longitude : float
        Target longitude
    lookup_df : pd.DataFrame, optional
        Lookup table DataFrame. If None, will load from S3.
    
    Returns
    -------
    int
        Nearest GID
    float
        Distance to nearest point (in degrees)
    float
        Nearest latitude
    float
        Nearest longitude
    """
    # Load lookup table if not provided
    if lookup_df is None:
        lookup_df = load_lookup_table()
    
    # Extract lat/lon coordinates
    coords = lookup_df[['latitude', 'longitude']].values
    
    # Target point
    target = np.array([[latitude, longitude]])
    
    # Calculate distances (using Euclidean distance in lat/lon space)
    distances = cdist(target, coords, metric='euclidean')[0]
    
    # Find nearest point
    nearest_idx = np.argmin(distances)
    nearest_distance = distances[nearest_idx]
    
    # Get nearest GID and coordinates
    nearest_row = lookup_df.iloc[nearest_idx]
    nearest_gid = int(nearest_row['gid'])
    nearest_lat = float(nearest_row['latitude'])
    nearest_lon = float(nearest_row['longitude'])
    
    return nearest_gid, nearest_distance, nearest_lat, nearest_lon


def load_data_by_lat_lon(latitude, longitude, setup_num, s3_bucket_path=S3_BUCKET_PATH, 
                         lookup_df=None):
    """
    Load data for a specific lat/lon by finding the nearest GID.
    
    Parameters
    ----------
    latitude : float
        Target latitude
    longitude : float
        Target longitude
    setup_num : int
        Setup number (1-10)
    s3_bucket_path : str
        S3 path to the zarr files directory
    lookup_df : pd.DataFrame, optional
        Lookup table DataFrame. If None, will load from S3.
    
    Returns
    -------
    xr.Dataset or None
        Dataset for the nearest GID, or None if GID not found
    int
        GID that was used
    float
        Distance to nearest point (in degrees)
    float
        Nearest latitude
    float
        Nearest longitude
    """
    # Find nearest GID
    nearest_gid, distance, nearest_lat, nearest_lon = find_nearest_gid(
        latitude, longitude, lookup_df=lookup_df
    )
    
    # Load data for that GID
    data, matching_gids = load_data_by_gid(setup_num, [nearest_gid], s3_bucket_path)
    
    return data, nearest_gid, distance, nearest_lat, nearest_lon


def load_data_by_lat_lon_multiple_setups(latitude, longitude, setup_nums, 
                                         s3_bucket_path=S3_BUCKET_PATH, lookup_df=None):
    """
    Load data for a specific lat/lon by finding the nearest GID, from multiple setups.
    
    Parameters
    ----------
    latitude : float
        Target latitude
    longitude : float
        Target longitude
    setup_nums : list of int
        List of setup numbers (1-10)
    s3_bucket_path : str
        S3 path to the zarr files directory
    lookup_df : pd.DataFrame, optional
        Lookup table DataFrame. If None, will load from S3.
    
    Returns
    -------
    xr.Dataset or None
        Combined dataset with a 'setup' dimension, or None if GID not found
    int
        GID that was used
    float
        Distance to nearest point (in degrees)
    float
        Nearest latitude
    float
        Nearest longitude
    """
    # Find nearest GID
    nearest_gid, distance, nearest_lat, nearest_lon = find_nearest_gid(
        latitude, longitude, lookup_df=lookup_df
    )
    
    # Load data for that GID from multiple setups
    data, matching_gids_dict = load_data_by_gid_multiple_setups(
        setup_nums, [nearest_gid], s3_bucket_path
    )
    
    return data, nearest_gid, distance, nearest_lat, nearest_lon


def load_data_by_lat_lon_range(lat_min, lat_max, lon_min, lon_max, setup_num, 
                                s3_bucket_path=S3_BUCKET_PATH, lookup_df=None):
    """
    Load data for all GIDs within a lat/lon bounding box.
    
    Parameters
    ----------
    lat_min : float
        Minimum latitude
    lat_max : float
        Maximum latitude
    lon_min : float
        Minimum longitude
    lon_max : float
        Maximum longitude
    setup_num : int
        Setup number (1-10)
    s3_bucket_path : str
        S3 path to the zarr files directory
    lookup_df : pd.DataFrame, optional
        Lookup table DataFrame. If None, will load from S3.
    
    Returns
    -------
    xr.Dataset or None
        Dataset containing all GIDs within the bounding box, or None if no GIDs found
    pd.DataFrame
        DataFrame of GIDs and their coordinates within the range
    list
        List of matching GIDs found in the dataset
    """
    # Load lookup table if not provided
    if lookup_df is None:
        lookup_df = load_lookup_table()
    
    # Find GIDs within the bounding box
    mask = (
        (lookup_df['latitude'] >= lat_min) & 
        (lookup_df['latitude'] <= lat_max) &
        (lookup_df['longitude'] >= lon_min) & 
        (lookup_df['longitude'] <= lon_max)
    )
    
    gids_in_range = lookup_df[mask]
    
    if len(gids_in_range) == 0:
        return None, None, []
    
    # Get list of GIDs
    gid_list = gids_in_range['gid'].tolist()
    
    # Load data for these GIDs
    data, matching_gids = load_data_by_gid(setup_num, gid_list, s3_bucket_path)
    
    return data, gids_in_range, matching_gids


def load_data_by_lat_lon_range_multiple_setups(lat_min, lat_max, lon_min, lon_max, setup_nums,
                                               s3_bucket_path=S3_BUCKET_PATH, lookup_df=None):
    """
    Load data for all GIDs within a lat/lon bounding box from multiple setups.
    
    Parameters
    ----------
    lat_min : float
        Minimum latitude
    lat_max : float
        Maximum latitude
    lon_min : float
        Minimum longitude
    lon_max : float
        Maximum longitude
    setup_nums : list of int
        List of setup numbers (1-10)
    s3_bucket_path : str
        S3 path to the zarr files directory
    lookup_df : pd.DataFrame, optional
        Lookup table DataFrame. If None, will load from S3.
    
    Returns
    -------
    xr.Dataset or None
        Combined dataset with a 'setup' dimension, or None if no GIDs found
    pd.DataFrame
        DataFrame of GIDs and their coordinates within the range
    dict
        Dictionary mapping setup numbers to lists of matching GIDs found in each dataset
    """
    # Load lookup table if not provided
    if lookup_df is None:
        lookup_df = load_lookup_table()
    
    # Find GIDs within the bounding box
    mask = (
        (lookup_df['latitude'] >= lat_min) & 
        (lookup_df['latitude'] <= lat_max) &
        (lookup_df['longitude'] >= lon_min) & 
        (lookup_df['longitude'] <= lon_max)
    )
    
    gids_in_range = lookup_df[mask]
    
    if len(gids_in_range) == 0:
        return None, None, {}
    
    # Get list of GIDs
    gid_list = gids_in_range['gid'].tolist()
    
    # Load data for these GIDs from multiple setups
    data, matching_gids_dict = load_data_by_gid_multiple_setups(
        setup_nums, gid_list, s3_bucket_path
    )
    
    return data, gids_in_range, matching_gids_dict


