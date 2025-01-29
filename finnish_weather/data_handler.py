import pandas as pd
import time
from datetime import datetime, timedelta
from fmiopendata.wfs import download_stored_query

from misc.const import FMI_OBSERVATIONS

def fetch_fmi_data(location, date):
    """
    Fetches weather observation data from the Finnish Meteorological Institute (FMI) for a single date in 6-hour chunks.

    Parameters:
        location (str): Bounding box coordinates (e.g., "18,55,35,75" for Finland).
        date (datetime.date): The date for which to fetch weather data (YYYY-MM-DD).

    Returns:
        pd.DataFrame: DataFrame containing weather observations for the given date.
    """
    try:
        all_data = []
        
        # Split the request into 6-hour chunks to reduce the load
        for hour_offset in range(0, 24, 6):
            start_time = datetime.combine(date, datetime.min.time()) + timedelta(hours=hour_offset)
            end_time = start_time + timedelta(hours=6)  # 6-hour intervals

            # Convert to ISO format as required by FMI
            start_time_iso = start_time.isoformat(timespec="seconds") + "Z"
            end_time_iso = end_time.isoformat(timespec="seconds") + "Z"

            print(f"Fetching FMI data from {start_time_iso} to {end_time_iso}")

            # Construct the query arguments in the correct format
            query_args = [
                f"bbox={location}",
                f"starttime={start_time_iso}",
                f"endtime={end_time_iso}"
            ]
            print("Query Arguments:", query_args)

            # Attempt the request with retries
            retries = 3
            while retries > 0:
                try:
                    # Correctly format the query
                    obs = download_stored_query(FMI_OBSERVATIONS, args=query_args)

                    # 🔹 Check if obs is empty
                    if not obs:
                        print(f"No data received for {start_time_iso} - {end_time_iso}")
                        break

                    print(f"Response type: {type(obs)}")

                    # Convert response to DataFrame properly
                    df = pd.DataFrame(obs)

                    if not df.empty:
                        all_data.append(df)

                    break  # Break if successful
                except Exception as e:
                    print(f"Retrying... ({3 - retries}/3). Error: {e}")
                    retries -= 1
                    time.sleep(2)  # Short delay before retrying

        # Combine all retrieved data
        if all_data:
            return pd.concat(all_data, ignore_index=True)

        print("No data retrieved.")
        return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching FMI data: {e}")
        return pd.DataFrame()