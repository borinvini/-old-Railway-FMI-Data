import time
import pandas as pd
import os
import streamlit as st
import ast
from datetime import datetime, timedelta
from fmiopendata.wfs import download_stored_query

from finnish_railway.data_handler import get_trains_by_date, load_railway_metadata
from finnish_weather.data_handler import fetch_fmi_data
from misc.misc_functions import print_memory_usage, save_dataframe_to_csv

from misc.const import END_DATE, FIN_RAILWAY_BASE_URL, FIN_RAILWAY_STATIONS, FMI_BBOX, START_DATE
from misc.const import CSV_ALL_TRAINS, CSV_TRAIN_STATIONS, FOLDER_NAME

# Ensure the output_data folder exists
if not os.path.exists(FOLDER_NAME):
    os.makedirs(FOLDER_NAME)

# Streamlit App
st.title("Data Fetcher")

# Check if the output_data folder is empty
output_data_folder = FOLDER_NAME
output_files = os.listdir(output_data_folder)

if output_files:
    # If the folder is not empty, display file names and their sizes as warnings
    st.info("We already have train data stored in the application.", icon="ℹ️")
    st.write("Files in Output Data Folder:")

    # Display file names and sizes
    for file_name in output_files:
        file_path = os.path.join(output_data_folder, file_name)
        if os.path.isfile(file_path):
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert size to MB
            if file_size_mb > 2014:
                file_size_gb = file_size_mb / 1024  # Convert size to GB
                st.warning(f"File: `{file_name}` - Size: {file_size_gb:.2f} GB", icon="📂")
            else:
                st.warning(f"File: `{file_name}` - Size: {file_size_mb:.2f} MB", icon="📂")
else:
    # If the folder is empty
    st.warning("The data folder is empty. You may need to fetch new data.", icon="⚠️")

# Select date range for data fetching
st.write("Select Date Range for Train Data Fetching")

# Use columns to display start and end date side by side
col1, col2 = st.columns(2)

# Convert START_DATE and END_DATE to datetime
default_start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
default_end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

# Date inputs for start and end dates in separate columns
with col1:
    start_date = st.date_input("Start Date", value=default_start_date, min_value=datetime(2000, 1, 1))

with col2:
    end_date = st.date_input("End Date", value=default_end_date, min_value=start_date)

# Button to fetch new data
fetch_data = st.button("Fetch New Data")

# * Fetch data from the API
if fetch_data:

    st.info("Fetching new data from the API. Please wait...", icon="ℹ️")

    # Validate date range
    if start_date > end_date:
        st.error("Error: Start Date must be before or equal to End Date.", icon="🚨")
        st.stop()

    # * FETCHING RAILWAY DATA *

    # Set the base URL for the Finnish Railway API    
    base_url = FIN_RAILWAY_BASE_URL

    # Fetch and save station metadata
    api_url_path = f"{base_url}{FIN_RAILWAY_STATIONS}"
    station_metadata = load_railway_metadata(api_url_path)
    if station_metadata.empty:
        st.error("No station metadata available. Exiting...", icon="🚨")
    else:
        st.success("Station metadata loaded successfully.", icon="✅")
        save_dataframe_to_csv(station_metadata, CSV_TRAIN_STATIONS)

    # Fetch train data for the selected date range
    current_date = start_date
    all_trains_data = []

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        st.write(f"Fetching train data for {date_str}...")
        daily_data = get_trains_by_date(date_str)

        if not daily_data.empty:
            all_trains_data.append(daily_data)

        current_date += timedelta(days=1)

    # Combine all daily data into a single DataFrame
    if all_trains_data:
        trains_data = pd.concat(all_trains_data, ignore_index=True)
        st.success(f"Fetched a total of {len(trains_data)} trains from {start_date} to {end_date}.", icon="✅")

        # Enrich timeTableRows with stationName month by month
        if "timeTableRows" in trains_data.columns:
            trains_data["departureMonth"] = pd.to_datetime(trains_data["departureDate"]).dt.to_period("M")
            months = trains_data["departureMonth"].unique()

            enriched_data = []

            for month in months:
                st.write(f"Processing data for: `{month}`. Wait...")
                month_data = trains_data[trains_data["departureMonth"] == month]

                def enrich_timetable_row(row):
                    try:
                        # Parse the timeTableRows data (JSON-like string)
                        parsed_row = ast.literal_eval(row) if isinstance(row, str) else row
                        if isinstance(parsed_row, list):  # Ensure it's a list of dictionaries
                            enriched_rows = []
                            for entry in parsed_row:
                                # Match stationShortCode with stationName from station_metadata
                                station_name = station_metadata.loc[
                                    station_metadata["stationShortCode"] == entry["stationShortCode"], "stationName"
                                ]
                                # If a match is found, add the stationName to the entry
                                station_name_value = station_name.iloc[0] if not station_name.empty else None
                                # Create a new dictionary with stationName as the first key
                                enriched_entry = {"stationName": station_name_value}
                                enriched_entry.update(entry)  # Add the remaining keys/values
                                enriched_rows.append(enriched_entry)
                            return enriched_rows
                        return parsed_row
                    except Exception as e:
                        print(f"Error processing timeTableRows: {e}")
                        return row  # Return the original row in case of an error

                # Apply the enrichment to the timeTableRows column
                month_data["timeTableRows"] = month_data["timeTableRows"].apply(enrich_timetable_row)

                enriched_data.append(month_data)

            st.success(f"Data processing is done.", icon="✅")
            # Combine enriched data
            trains_data = pd.concat(enriched_data, ignore_index=True)

        else:
            st.write("No 'timeTableRows' column found in the trains_data DataFrame.")

        # Save the enriched trains_data DataFrame to CSV
        save_dataframe_to_csv(trains_data, CSV_ALL_TRAINS)

        # Print memory usage of the DataFrame
        print_memory_usage(trains_data, "trains_data")

    else:
        st.error("No train data available for the specified date range.")

    # * FETCHING FMI DATA *
    # ! TEST

    # Define the date and time range
    date_str = "2024-12-16"  # Example date
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Define the bbox variable for Finland
    Finland_location = "bbox=18,55,35,75"

    # Store all data
    all_data = []

    # Loop through the date in **6-hour intervals**
    for hour_offset in range(0, 24, 6):
        start_time = datetime.combine(date_obj, datetime.min.time()) + timedelta(hours=hour_offset)
        end_time = start_time + timedelta(hours=6)

        start_time_iso = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time_iso = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        print(f"Fetching FMI data from {start_time_iso} to {end_time_iso}")

        # Store the query arguments in a variable
        query_args = [
            Finland_location,
            f"starttime={start_time_iso}",
            f"endtime={end_time_iso}"
        ]
        print(query_args)

        # Retry mechanism for failed requests
        retries = 3
        while retries > 0:
            try:
                # Query the FMI data
                obs = download_stored_query("fmi::observations::weather::multipointcoverage", args=query_args)

                if not obs.data:
                    print(f"No data retrieved for {start_time_iso} - {end_time_iso}")
                    break  

                # Convert to DataFrame
                df = pd.DataFrame(sorted(obs.data.keys()), columns=["Observation Times"])

                # Store data if not empty
                if not df.empty:
                    all_data.append(df)
                
                break  # Exit retry loop if successful
            except Exception as e:
                print(f"Retrying... ({3 - retries}/3). Error: {e}")
                retries -= 1
                time.sleep(2)  # Short delay before retrying

    # Combine all retrieved data
    if all_data:
        times_df = pd.concat(all_data, ignore_index=True)
    else:
        times_df = pd.DataFrame(columns=["Observation Times"])  # Return empty DataFrame

    # Display the DataFrame
    print(times_df)

    sizeof_df_time = times_df.shape
    print(f"Number of rows in the DataFrame: {sizeof_df_time[0]}")

    # ! TEST end
        
    #fmi_data = fetch_fmi_data(FMI_BBOX, start_date)
    #print(fmi_data)
else:
    st.write("Click the button above to fetch new data from the APIs.")
