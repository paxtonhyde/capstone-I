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
from readwrite import loaddata

SCRIPT_DIRECTORY = os.path.realpath("")
HOME_DIRECTORY = os.path.split(SCRIPT_DIRECTORY)[0]
DATA_DIRECTORY = os.path.join(HOME_DIRECTORY, "data")

if __name__ == "__main__":
    ## argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', required=True,\
                         type=str, help='file of moral machine responses to process.')
    args = vars(parser.parse_args())

    ## spark builder
    spark = (ps.sql.SparkSession.builder 
        .master("local[*]") 
        .appName("spark-tester")
        .getOrCreate()
        )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    ## loading responses with survey data into a spark dataframe
    ## SharedResponsesSurvey.csv has 11286141 rows
    bucketname = 'paxton-dsi-capstone-i'
    fname = args['file']
    responses = loaddata(fname, spark, bucketname)

    sample_sizes = [(10**q)*n for q in range(1, 7) for n in range(1, 6)]
    #sample_sizes = [(10**q) for q in range(1, 7)]
    trials = 1
    test_data = np.zeros((len(sample_sizes), 5))

    for i, size in enumerate(sample_sizes):
        responses_trial = responses.select('*').limit(size)

        trial_data = np.zeros((trials, 5))
        for trial in range(trials):
            country = "FRA"
            attribute = "Utilitarian"

            ## filter
            start_time = time.time()
            filtered = responses_trial.filter(f"UserCountry3 = '{country}'")
            filter_time = time.time() - start_time
            trial_data[trial][0] = filter_time

            ## count
            start_time = time.time()
            counted = responses_trial.count()
            count_time = time.time() - start_time
            if counted != size:
                print(f"Sample size was supposed to be {size}, count was {counted}.")
            trial_data[trial][1] = count_time

            ## p_factor (3 counts)
            start_time = time.time()
            _,_,_,_ = paxton.p_factor(responses_trial, attribute)
            p_time = time.time() - start_time
            trial_data[trial][2] = p_time

            ## p_factor2 (2 counts)
            start_time = time.time()
            _,_ = paxton.p_factor2(responses_trial, attribute)
            p2_time = time.time() - start_time
            trial_data[trial][3] = p2_time

            ## groupby
            start_time = time.time()
            grouped = responses_trial.groupby("UserID").agg({'Saved':'mean'})
            group_time = time.time() - start_time
            trial_data[trial][4] = group_time

            print(f"sample {size}, trial {trial}")

        test_data[i] = trial_data

    results = pd.DataFrame(test_data, index=sample_sizes, columns=['filter', 'count', 'old', 'new', 'groupby'])

    fname = "spark_data.csv"
    results.to_csv(path_or_buf=f"{DATA_DIRECTORY}/{fname}")
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(f"{DATA_DIRECTORY}/{fname}", bucketname, fname)
    except ClientError:
        print(f"Failed uploading {fname} to bucket {bucketname}.")