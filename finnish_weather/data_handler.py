import pandas as pd
import time
from datetime import datetime, timedelta
from fmiopendata.wfs import download_stored_query

from misc.const import FMI_OBSERVATIONS, FMI_EMS
from misc.misc_functions import log_message


def fetch_fmi_data(location, date, max_retries=3, log_file="fmi_fetch.log"):
    """
    Fetches weather observation data and station metadata from the Finnish Meteorological Institute (FMI)
    for a single date in 1-hour chunks.

    Parameters:
        location (str): Bounding box coordinates (e.g., "18,55,35,75" for Finland).
        date (datetime.date): The date for which to fetch weather data (YYYY-MM-DD).
        max_retries (int): Maximum number of retries in case of connection failures.
        log_file (str): Path to the log file.

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
                    log_message(f"No data retrieved for {start_time.strftime('%Y-%m-%d %H:%M')} - Skipping", log_file)
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
                log_message(f"Attempt {attempt} failed for {start_time_iso}: {e}", log_file)
                if attempt < max_retries:
                    wait_time = 10 * attempt  # Exponential backoff (10s, 20s, 30s)
                    log_message(f"Retrying in {wait_time} seconds...", log_file)
                    time.sleep(wait_time)
                else:
                    log_message(f"Skipping {start_time.strftime('%Y-%m-%d %H:%M')} after {max_retries} failed attempts.", log_file)

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


def clean_fmi_data(df):
    """
    Cleans the FMI dataset by:
    1. Removing duplicate entries based on timestamp and station_name.
    2. Reordering columns: station_name first, timestamp second, followed by all other columns.
    3. Sorting by station_name and timestamp.

    Parameters:
        df (pd.DataFrame): The original FMI DataFrame.

    Returns:
        pd.DataFrame: The cleaned and ordered FMI DataFrame.
    """
    # Drop duplicates based on timestamp and station_name
    df = df.drop_duplicates(subset=["timestamp", "station_name"], keep="first")

    # Reorder columns: station_name first, timestamp second, followed by the rest
    columns_order = ["station_name", "timestamp"] + [col for col in df.columns if col not in ["station_name", "timestamp"]]
    df = df[columns_order]

    # Sort by station_name and timestamp
    df = df.sort_values(by=["station_name", "timestamp"])

    return df