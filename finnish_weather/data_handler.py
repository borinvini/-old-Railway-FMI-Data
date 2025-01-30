import pandas as pd
import time
from datetime import datetime, timedelta
from fmiopendata.wfs import download_stored_query

from misc.const import FMI_OBSERVATIONS, FMI_EMS


def fetch_fmi_data(location, date, max_retries=3):
    """
    Fetches weather observation data and station metadata from the Finnish Meteorological Institute (FMI)
    for a single date in 1-hour chunks.

    Parameters:
        location (str): Bounding box coordinates (e.g., "18,55,35,75" for Finland).
        date (datetime.date): The date for which to fetch weather data (YYYY-MM-DD).
        max_retries (int): Maximum number of retries in case of connection failures.

    Returns:
        pd.DataFrame: DataFrame containing full weather observations for the given date.
        pd.DataFrame: DataFrame containing metadata of weather stations.
    """
    all_data = []
    station_metadata = {}

    # Loop through 24 hours of the given date in 1-hour intervals
    for hour_offset in range(24):
        start_time = datetime.combine(date, datetime.min.time()) + timedelta(hours=hour_offset)
        end_time = start_time + timedelta(hours=1)

        start_time_iso = start_time.isoformat() + "Z"
        end_time_iso = end_time.isoformat() + "Z"

        print(f"Fetching FMI data from {start_time_iso} to {end_time_iso}")

        # Introduce a small delay to avoid API rate limits
        time.sleep(5)

        # Construct the query arguments
        query_args = [
            f"bbox={location}",
            f"starttime={start_time_iso}",
            f"endtime={end_time_iso}"
        ]

        attempt = 1
        while attempt <= max_retries:
            try:
                # Query the FMI data
                obs = download_stored_query(FMI_OBSERVATIONS, args=query_args)

                # Check if data is available
                if not obs.data:
                    print(f"No data retrieved for {start_time.strftime('%Y-%m-%d %H:%M')} - Skipping")
                    break  # Move to next hour

                # Extract station metadata (only once)
                if not station_metadata:
                    station_metadata = obs.location_metadata  # Metadata for all stations

                # Convert observation data to DataFrame
                data = []
                for timestamp, station_data in obs.data.items():
                    for station_name, variables in station_data.items():
                        row = {"timestamp": timestamp, "station_name": station_name}  # Add timestamp & station
                        row.update({param: values["value"] for param, values in variables.items()})
                        data.append(row)

                df_data = pd.DataFrame(data)
                df_data["timestamp"] = pd.to_datetime(df_data["timestamp"])

                all_data.append(df_data)
                break  # Successful request, move to the next hour

            except Exception as e:
                print(f"Attempt {attempt} failed for {start_time_iso}: {e}")
                if attempt < max_retries:
                    wait_time = 10 * attempt  # Exponential backoff (10s, 20s, 30s)
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Skipping {start_time.strftime('%Y-%m-%d %H:%M')} after {max_retries} failed attempts.")

            attempt += 1

    # Combine all hourly chunks into one DataFrame
    df_data_combined = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    # Convert station metadata to DataFrame
    df_metadata = pd.DataFrame.from_dict(station_metadata, orient="index").reset_index()
    df_metadata.rename(columns={"index": "station_name"}, inplace=True)

    return df_data_combined, df_metadata

def interpolate_ems_data(df):
    """
    Interpolates missing EMS station data by filling in missing values with the closest available measurement.

    Parameters:
        df (pd.DataFrame): DataFrame containing EMS station data, where missing values are represented as None.

    Returns:
        pd.DataFrame: DataFrame with missing values interpolated.
    """
    # Ensure the timestamp column is sorted
    df = df.sort_values(by="timestamp")

    # Replace None with NaN for processing
    df.replace({None: pd.NA}, inplace=True)

    # Apply forward fill (ffill) and then backward fill (bfill)
    df.fillna(method="ffill", inplace=True)  # Fill missing values with the last known value
    df.fillna(method="bfill", inplace=True)  # Fill remaining missing values with the next known value

    return df


def transform_fmi_data(df):
    """
    Transforms the FMI data by keeping the timestamp as the first column,
    converting station names into individual columns, and storing measurements
    in dictionaries for each row.

    Parameters:
        df (pd.DataFrame): Original FMI data with 'station_name' and measurements.

    Returns:
        pd.DataFrame: Transformed DataFrame with one column per station and measurements as dictionaries.
    """
    # Drop duplicate rows (keeping the first occurrence)
    df = df.drop_duplicates(subset=["timestamp", "station_name"])

    # Ensure timestamp is in datetime format
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Create a pivot table with timestamp as index and station_name as columns
    transformed_df = df.pivot(index="timestamp", columns="station_name")

    # Convert MultiIndex columns into a dictionary for each timestamp
    transformed_df = (
        transformed_df.stack(level=0)
        .groupby(level=0)
        .apply(lambda x: {station: {key: value for key, value in data.items() if key != "timestamp"}
                          for station, data in x.to_dict(orient="index").items()})
        .reset_index()
    )

    # Rename columns for clarity
    transformed_df.columns = ["timestamp", "station_measurements"]

    return transformed_df
