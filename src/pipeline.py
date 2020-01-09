## pipeline script for country-level data

import time
import os
import argparse
import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError
import pyspark as ps
import pyspark.sql.functions as f
import probability_functions as paxton

SCRIPT_DIRECTORY = os.path.realpath("")
HOME_DIRECTORY = os.path.split(SCRIPT_DIRECTORY)[0]
DATA_DIRECTORY = os.path.join(HOME_DIRECTORY, "data")

## functions for reading and writing files to and from spark
def loaddata(filename, sparkobject, s3bucket):
    path = f'{DATA_DIRECTORY}/{filename}'
    if filename not in os.listdir(DATA_DIRECTORY):
        s3_client = boto3.client('s3')
        s3_client.download_file(s3bucket,\
                        filename,\
                        path)
    extension = filename.split('.')[1]
    return sparkobject.read.load(path, format = extension, header='true')

def uploaddata(sparkdataframe, filename, s3bucket, mode="overwrite"):
    path = f'{DATA_DIRECTORY}/{filename}'
    extension = filename.split('.')[1]
    sparkdataframe.write.save(path=path, format=extension, mode=mode)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(path, s3bucket, filename)
    except ClientError:
        print(f"Failed uploading {filename} to bucket {s3bucket}.")
    
    return True

if __name__ == "__main__":
    ## argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', required=True,\
                         type=str, help='file of moral machine responses to process.')
    args = vars(parser.parse_args())

    ## spark builder
    spark = (ps.sql.SparkSession.builder 
        .master("local[*]") 
        .appName("country-pipeline")
        .getOrCreate()
        )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    ## loading responses with survey data into a spark dataframe
    ## SharedResponsesSurvey.csv has 11286141 rows
    bucketname = 'paxton-dsi-capstone-i'
    fname = args['file']
    responses = loaddata(fname, spark, bucketname)

    ## pick/sample responses
    responses = responses.select(["UserID", "UserCountry3", "Saved", "Intervention", "CrossingSignal",\
        "PedPed", "ScenarioType", "AttributeLevel", "Review_age","Review_education", \
        "Review_gender", "Review_income", "Review_political" ,"Review_religious"])

    ## pulling the list of all country ISO3 codes
    countries = loaddata('country_cluster_map.csv', spark, bucketname).select("ISO3")
    ## creating a pandas dataframe to hold preferences by country
    pandas_cols = ["ISO3", "p_intervention", "n_intervention", "p_legality", "n_legality",\
               "p_util", "n_util", "p_gender", "n_gender", \
               "p_social", "n_social", "p_age", "n_age"]
    factors = ["Intervention", "Legality", "Utilitarian", "Gender", "Social Status", "Age"]
    country_probs = pd.DataFrame(columns=pandas_cols)

    sample_size = 39000

    for row in countries.collect():
        country = row.ISO3
        country_responses = responses.filter(f"UserCountry3 = '{country}' ").limit(sample_size)

        country_data_out = [str(country)]

        start_time = time.time()
        try:
            for fac in factors:
                p, n = paxton.p_factor2(country_responses, fac)
                country_data_out.extend((p, n))
        except TypeError:
            print(f"{country} had no relevant entries.")
            continue

        trial_time = time.time() - start_time
        print(f'{country} took {trial_time}s.')

        country_data_out_df = pd.DataFrame([country_data_out], columns=pandas_cols)
        country_probs = country_probs.append(country_data_out_df)

        print(f"Wrote data for {country}.")

    ## writing the pandas dataframe to a new .csv file
    fname = "country_preferences.csv"
    path = f"{DATA_DIRECTORY}/{fname}"
    country_probs.to_csv(path_or_buf=path, index=False)
    print(f"Wrote data to {fname}")
    
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(path, bucketname, fname)
    except ClientError:
        print(f"Failed uploading {fname} to bucket {bucketname}.")
    print(f"Uploaded {fname} to {bucketname}.")
