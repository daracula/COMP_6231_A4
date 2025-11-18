import pandas as pd

def air_flights_most_canceled_flights(df: pd.DataFrame) -> str:
     # 1. Create a boolean mask for the filter conditions
    mask = (df["Year"] == 2021) & \
           (df["Month"] == 1) & \
           (df["Cancelled"] == True)

    # 2. Apply the mask
    canceled_flights = df[mask]

    if canceled_flights.empty:
        return "No flights found"

    # 3. Group by Airline, count the rows (.size()), and find the index (Airline)
    #    of the largest value (.idxmax()).
    grouped_counts = canceled_flights.groupby("Airline").size()

    return grouped_counts.idxmax()

# --- Task 2: Count Diverted Flights ---
def diverted_nov_2021(df: pd.DataFrame) -> int:
    """Finds the number of diverted flights between 1-30 Nov 2021."""

    # 1. Create the filter mask
    mask = (df["Year"] == 2021) & \
           (df["Month"] == 11) & \
           (df["Diverted"] == True)

    # 2. Sum the boolean mask. (True=1, False=0)
    # This is the most efficient way to count rows matching a condition.
    return int(mask.sum())

# --- Task 3: Average Airtime LA to NYC ---
def avg_airtime_la_to_nyc(df: pd.DataFrame) -> float:
    """Calculates average airtime for flights from LA to NYC."""

    # 1. Create the filter mask
    mask = (df["OriginCityName"] == "Los Angeles, CA") & \
           (df["DestCityName"] == "New York, NY")

    # 2. Apply mask, select the "Airtime" column, and calculate the mean.
    #    .mean() automatically skips null values by default.
    avg_val = df[mask]["AirTime"].mean()

    # 3. Handle the case where no flights match (mean will be NaN)
    if pd.isna(avg_val):
        return 0.0

    return float(avg_val)

# --- Task 4: Unique Days Missing Departure Time ---
def count_days_missing_departure(df: pd.DataFrame) -> int:
    """Calculates how many unique days are missing departure time."""

    # 1. Filter for rows where DepTime is null (.isna() checks for None and np.nan)
    # 2. Select the "FlightDate" column for those rows
    # 3. Count the number of unique dates (.nunique())

    unique_days_count = df[df["DepTime"].isna()]["FlightDate"].nunique()

    return int(unique_days_count)

if __name__ == "__main__":

    data = pd.read_csv("flights_data.csv")

    flights_df = pd.DataFrame(data)

    print("--- Pandas Results ---")

    # Test Task 1
    most_canceled = air_flights_most_canceled_flights(flights_df)
    print(f"Most Canceled (Jan 2021): {most_canceled}")

    # Test Task 2
    diverted_count = diverted_nov_2021(flights_df)
    print(f"Diverted (Nov 2021): {diverted_count}")

    # Test Task 3
    avg_airtime = avg_airtime_la_to_nyc(flights_df)
    print(f"Avg Airtime (LA->NYC): {avg_airtime}")

    # Test Task 4
    missing_days = count_days_missing_departure(flights_df)
    print(f"Unique days missing DepTime: {missing_days}")