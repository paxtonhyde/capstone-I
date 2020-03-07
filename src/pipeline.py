import time
import os
import argparse
import pandas as pd
import numpy as np
import boto3
import pyspark as ps
import pyspark.sql.functions as f
import probability_functions as paxton
from readwrite import loaddata, uploaddata

SCRIPT_DIRECTORY = os.path.realpath("")
HOME_DIRECTORY = os.path.split(SCRIPT_DIRECTORY)[0]
DATA_DIRECTORY = os.path.join(HOME_DIRECTORY, "data")

if __name__ == "__main__":
    ## argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', required=True,\
                         type=str, help='file of moral machine responses to process.')
    parser.add_argument('-m', '--mode', default="bootstrap"
                         type=str, help='Bootstrap individual samples from country means (bootstrap) \
                         or use the country-level means only (mean). Default : bootstrap')
    args = vars(parser.parse_args())

    fname = args['file']

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
    responses = loaddata(fname, spark, bucketname)
    responses = responses.select(["UserID", "UserCountry3", "Saved", "Intervention", "CrossingSignal",\
        "PedPed", "ScenarioType", "AttributeLevel"])

    ## pulling the list of all country ISO3 codes
    countries = loaddata('country_cluster_map.csv', spark, bucketname).select("ISO3")
    ## creating a pandas dataframe to hold preferences by country
    pandas_cols = ["ISO3", "p_intervention", "n_intervention", "p_legality", "n_legality",\
               "p_util", "n_util", "p_gender", "n_gender", \
               "p_social", "n_social", "p_age", "n_age"]
    country_probs = pd.DataFrame(columns=pandas_cols)

    factors = ["Intervention", "Legality", "Utilitarian", "Gender", "Social Status", "Age"]
    for row in countries.collect():
        country = row.ISO3
        country_responses = responses.filter(f"UserCountry3 = '{country}' ")

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

    ## writing the pandas dataframe to /data and s3 bucket
    fname = "country_preferences.csv"
    t = uploaddata(country_probs, fname, bucketname)
    if t:
        print(f"Uploaded {fname} to {bucketname}.")
