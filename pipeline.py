## pipeline script

## modules
import pyspark as ps
import pyspark.sql.functions as f

if __name__ == "__main__":
    ## builder
    spark = (ps.sql.SparkSession.builder 
        .master("local") 
        .appName("pipeline")
        .getOrCreate()
        )
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    ## loading the file into a spark dataframe
    path = "data/SharedResponsesSurvey_10000.csv"
    #rdd = sc.textFile(path)
    responses = spark.read.csv(path, header=True)
    responses.show(10)

    # us_responses = df.select("*").filter("UserCountry3 = 'USA' ").show("10")
    # print(type(us_responses))
    ##end