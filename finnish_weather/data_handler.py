import pandas as pd
import time
from datetime import datetime
from fmiopendata.wfs import download_stored_query

from misc.const import FMI_OBSERVATIONS


def fetch_fmi_data(location, date, max_retries=3):
    """
    Fetches weather observation data from the Finnish Meteorological Institute (FMI) for a single date.

    Parameters:
        location (str): Bounding box coordinates (e.g., "18,55,35,75" for Finland).
        date (datetime.date): The date for which to fetch weather data (YYYY-MM-DD).
        max_retries (int): Maximum number of retries in case of connection failures.

    Returns:
        pd.DataFrame: DataFrame containing full weather observations for the given date.
    """
    try:
        # Convert date to string and format to ISO 8601 (UTC format)
        date_start_iso = datetime.combine(date, datetime.min.time()).isoformat() + "Z"
        date_end_iso = datetime.combine(date, datetime.max.time()).replace(microsecond=0).isoformat() + "Z"

        print(f"Fetching FMI data for {date} ({date_start_iso} - {date_end_iso})")

        # Introduce a delay to avoid overwhelming the API
        time.sleep(30)

        # Construct the query arguments
        query_args = [
            f"bbox={location}",
            f"starttime={date_start_iso}",
            f"endtime={date_end_iso}"
        ]

        attempt = 1
        while attempt <= max_retries:
            try:
                # Query the FMI data
                obs = download_stored_query(FMI_OBSERVATIONS, args=query_args)

                # Check if data is available
                if not obs.data:
                    print(f"No data retrieved for {date}")
                    return pd.DataFrame()

                # Convert response to DataFrame
                data = []
                for timestamp, variables in obs.data.items():
                    row = {"timestamp": timestamp}  # Add timestamp as a column
                    row.update(variables)  # Add all observed variables
                    data.append(row)

                df = pd.DataFrame(data)

                # Convert timestamp to datetime format
                df["timestamp"] = pd.to_datetime(df["timestamp"])

                return df  # Return the full dataset

            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    wait_time = 10 * attempt  # Exponential backoff (10s, 20s, 30s)
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Failed to fetch data after {max_retries} attempts. Skipping {date}.")
                    return pd.DataFrame()

            attempt += 1

    except Exception as e:
        print(f"Unexpected error fetching FMI data: {e}")
        return pd.DataFrame()
