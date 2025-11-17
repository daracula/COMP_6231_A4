import os
from itertools import permutations
from dotenv import load_dotenv
from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession

### install dependency ###
# pip install python-dotenv
# pip install pyspark # make sure you have jdk installed
#####################################

### please update your relative path while running your code ###
temp_airline_textfile = r"flights_data.txt"
temp_airline_csvfile = r"flights_data.csv"
default_spark_context = "local[*]"  # only update if you need
#######################################


### please don't update these lines ###
load_dotenv()
airline_textfile = os.getenv("AIRLINE_TXT_FILE", temp_airline_textfile)
airline_csvfile = os.getenv("AIRLINE_CSV_FILE", temp_airline_csvfile)
spark_context = os.getenv("SPARK_CONTEXT", default_spark_context)
#######################################


def co_occurring_airline_pairs_by_origin(flights_data: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the flights_data from txt file.
    Performs a series of MapReduce operations via PySpark to calculate
    the number of co-occurring airlines with same origin airports operating on the same date,
    determine count of such occurrences pairwise.
    Returns the results as an RDD sorted by the airline pairs alphabetically ascending (by first and then second value in the pair) with the counts in descending order.

    :param flights_dat: RDD object of the contents of flights_data
    :return: RDD of pairs of airline and the number of occurrences
        Example output:     [((Airline_A,Airline_B),3),
                                ((Airline_A,Airline_C),2),
                                ((Airline_B,Airline_C),1)]
    """

    # find pairs of airlines with same origin and date

    # parse, clean data
    # Input: "2021-01-01, Airline_A, Org_1"
    # Output Type: RDD[((Date, Origin, Airline), 1)]
    parsed_rdd = flights_data.map(lambda line: line.split(',')).map(lambda x: ((x[0].strip(), x[2].strip(), x[1].strip()), 1))

    # aggregate individual airline counts
    # Operation: Sum appearances of specific airline at specific date/origin
    # Output Type: RDD[((Date, Origin, Airline), Int)]
    airline_counts = parsed_rdd.reduceByKey(lambda x, y: x + y)

    # RE-KEY FOR CONTEXT GROUPING
    # Operation: Move Airline/Count to value, keep Date/Origin as key
    # Output Type: RDD[((Date, Origin), (Airline, Count))]
    context_keyed = airline_counts.map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))

    # GROUP BY CONTEXT
    # Operation: Gather all airlines present at a specific Date/Origin
    # Output Type: RDD[((Date, Origin), Iterable[(Airline, Count)])]
    grouped_context = context_keyed.groupByKey()

    # GENERATE PAIRS (FLATMAP)
    # Operation: Create pairs and calculate the "pair count" (Min logic)
    # Output Type: RDD[((Airline_1, Airline_2), Calculated_Count)]
    def generate_pairs(record):
        key, airlines_iterable = record
        # Convert to list and sort by Airline Name to ensure (A, B) is same as (B, A)
        airlines_list = sorted(list(airlines_iterable), key=lambda x: x[0])

        pairs = []
        n = len(airlines_list)

        # Double loop to generate unique pairs
        for i in range(n):
            for j in range(i + 1, n):
                airline_a = airlines_list[i]
                airline_b = airlines_list[j]

                name_a = airline_a[0]
                count_a = airline_a[1]
                name_b = airline_b[0]
                count_b = airline_b[1]

                # Logic: "if (A, 5) and (B, 3) then ((A, B), 3)"
                pair_count = min(count_a, count_b)

                # Create key as tuple of names
                pairs.append(((name_a, name_b), pair_count))

        return pairs

    pairs_rdd = grouped_context.flatMap(generate_pairs)

    # GLOBAL AGGREGATION
    # Operation: Sum the calculated pair counts across all Dates/Origins
    # Output Type: RDD[((Airline_A, Airline_B), Total_Count)]
    final_counts = pairs_rdd.reduceByKey(lambda x, y: x + y)

    # SORTING
    # Logic: Sort by Count (Desc), then Airline_A (Asc), then Airline_B (Asc)
    # Output Type: RDD[((Airline_A, Airline_B), Total_Count)]
    sorted_output = final_counts.sortBy(lambda x: (-x[1], x[0][0], x[0][1]))

    return sorted_output


def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Jan. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Jan. 2021.
    """
    raise NotImplementedError("Your Implementation Here.")


def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of
    1-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 1-30 Nov. 2021.
    """
    raise NotImplementedError("Your Implementation Here.")


def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Los Angeles, CA to
    New York, NY.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Los Angeles, CA  to
    New York, NY.
    """
    raise NotImplementedError("Your Implementation Here.")


def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing.
    """
    raise NotImplementedError("Your Implementation Here.")


def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    print("########################## Problem 1 ########################")
    # problem 1: co-occurring operating flights with Spark and MapReduce
    # read the file
    flights_data = sc.textFile(airline_textfile)
    sorted_airline_pairs = co_occurring_airline_pairs_by_origin(flights_data)
    sorted_airline_pairs.persist()
    for pair, count in sorted_airline_pairs.take(10):
        print(f"{pair}: {count}")

    '''print("########################## Problem 2 ########################")
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv(airline_csvfile, header=True, inferSchema=True)
    print(
        "Q1:",
        air_flights_most_canceled_flights(flights),
        "had the most canceled flights in September 2021.",
    )
    print(
        "Q2:",
        air_flights_diverted_flights(flights),
        "flights were diverted between the period of 1st-30th November 2021.",
    )
    print(
        "Q3:",
        air_flights_avg_airtime(flights),
        "is the average airtime for flights that were flying from "
        "Nashville to Chicago.",
    )
    print(
        "Q4:",
        air_flights_missing_departure_time(flights),
        "unique dates where departure time (DepTime) was not recorded.",
    )'''


if __name__ == "__main__":
    main()
