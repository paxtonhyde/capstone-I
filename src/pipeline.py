## pipeline script for country-level data

## modules
import pandas as pd
import pyspark as ps
import pyspark.sql.functions as f
import probability_functions as paxton


if __name__ == "__main__":
    ## builder
    ## goal is to build a multiple core cluster
    spark = (ps.sql.SparkSession.builder 
        .master("local") 
        .appName("pipeline-country")
        .getOrCreate()
        )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    ## loading responses with survey data into a spark dataframe
    path = "../data/SharedResponsesSurvey_10000.csv"
    responses = spark.read.csv(path, header=True)

    ## pulling the list of all country ISO3 codes with n > 100
    path = "../data/country_cluster_map.csv"
    countries = spark.read.csv(path, header=True).select("ISO3")

    ## creating a pandas dataframe to hold preferences by country
    pandas_cols = ["ISO3", "p_n_intervention",  "p_n_legality", "p_n_util", "p_n_gender",\
        "p_n_social", "p_n_age"]
    factors = ["Utilitarian", "Gender", "Social Status", "Age"]
    country_probs = pd.DataFrame(columns=pandas_cols)

    ## selecting and processing data by country
    for row in countries.collect():
        country = row.ISO3
        country_responses = responses.filter(f"UserCountry3 = '{country}' ") 
        try:
            country_data_out = [country, paxton.p_intervention(country_responses),\
                            paxton.p_legality(country_responses)]
            for fac in factors:
                p, n, _, _ = paxton.p_factor(country_responses, fac)
                country_data_out.append((p,n))
        except TypeError:
            print(f"{country} had no relevant entries.")
            continue
        
        country_data_out_df = pd.DataFrame([country_data_out], columns=pandas_cols)
        country_probs = country_probs.append(country_data_out_df)

    print(f"Finished processing.")

    ## writing the pandas dataframe to a new .csv file
    country_probs.to_csv(path_or_buf="../data/country_preferences.csv", index=False)
    print(f"Wrote data to country_preferences.csv")
