import os
import boto3
import botocore
import argparse
from botocore.handlers import disable_signing
import pandas as pd

def downloadAgriPVData(state, path, file_type='csv'):
    '''
    Method to access and pull data from the OEDI Data Lake for Inspire AgriPV geospatial data

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
    Method to merge the multiple files coming in from OEDI
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