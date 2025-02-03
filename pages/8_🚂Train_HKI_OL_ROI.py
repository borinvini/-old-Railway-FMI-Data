import time
import os
import ast
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from fmiopendata.wfs import download_stored_query

from cross_data.cross_functions import match_train_with_ems, merge_train_weather_data
from finnish_railway.data_handler import get_trains_by_date, load_railway_metadata
from finnish_weather.data_handler import clean_fmi_data, fetch_fmi_data, fetch_fmi_data_by_latlon
from misc.misc_functions import print_memory_usage, save_dataframe_to_csv
from misc.const import (
    CSV_CROSS_DATA, CSV_CROSS_STATIONS, CSV_FMI, CSV_FMI_EMS, END_DATE,
    FIN_RAILWAY_BASE_URL, FIN_RAILWAY_STATIONS, FMI_BBOX, START_DATE,
    CSV_ALL_TRAINS, CSV_TRAIN_STATIONS, FOLDER_NAME
)

# Ensure the output folder exists
if not os.path.exists(FOLDER_NAME):
    os.makedirs(FOLDER_NAME)

st.title("Filtered Data Fetcher")

# Show existing files if any
output_files = os.listdir(FOLDER_NAME)
if output_files:
    st.info("Existing data files found in the output folder.", icon="ℹ️")
    for file_name in output_files:
        file_path = os.path.join(FOLDER_NAME, file_name)
        if os.path.isfile(file_path):
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            if file_size_mb > 2014:
                file_size_gb = file_size_mb / 1024
                st.warning(f"File: `{file_name}` - Size: {file_size_gb:.2f} GB", icon="📂")
            else:
                st.warning(f"File: `{file_name}` - Size: {file_size_mb:.2f} MB", icon="📂")
else:
    st.warning("The data folder is empty. You may need to fetch new data.", icon="⚠️")

st.write("Select Date Range for Train Data Fetching")

# Date input widgets
col1, col2 = st.columns(2)
default_start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
default_end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
with col1:
    start_date = st.date_input("Start Date", value=default_start_date, min_value=datetime(2000, 1, 1))
with col2:
    end_date = st.date_input("End Date", value=default_end_date, min_value=start_date)

fetch_data = st.button("Fetch and Process New Data")

# Define a helper function to check if a train passes the required stations.
def train_passes_required_stations(timeTableRows, required_codes={"HKI", "OL", "ROI"}):
    """
    Checks whether the given timetable rows contain all required station short codes.
    `timeTableRows` can be either a list of dictionaries or a string representation of one.
    """
    try:
        # Parse if necessary
        if isinstance(timeTableRows, str):
            timeTableRows = ast.literal_eval(timeTableRows)
        # If not a list, then skip filtering.
        if not isinstance(timeTableRows, list):
            return False

        # Gather all station short codes from this train's timetable.
        station_codes = {entry.get("stationShortCode") for entry in timeTableRows if isinstance(entry, dict)}
        # Check if all required station codes are present
        return required_codes.issubset(station_codes)
    except Exception as e:
        st.error(f"Error during train filtering: {e}")
        return False

if fetch_data:
    st.info("Fetching new data from the API. Please wait...", icon="ℹ️")

    if start_date > end_date:
        st.error("Error: Start Date must be before or equal to End Date.", icon="🚨")
        st.stop()

    # === FETCH RAILWAY DATA ===

    # Load station metadata
    base_url = FIN_RAILWAY_BASE_URL
    api_url_path = f"{base_url}{FIN_RAILWAY_STATIONS}"
    station_metadata = load_railway_metadata(api_url_path)
    if station_metadata.empty:
        st.error("No station metadata available. Exiting...", icon="🚨")
        st.stop()
    else:
        st.success("Station metadata loaded successfully.", icon="✅")
        save_dataframe_to_csv(station_metadata, CSV_TRAIN_STATIONS)

    # Fetch train data for each day
    current_date = start_date
    all_trains_data = []
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        st.write(f"Fetching train data for {date_str}...")
        daily_data = get_trains_by_date(date_str)
        if not daily_data.empty:
            all_trains_data.append(daily_data)
        current_date += timedelta(days=1)

    if all_trains_data:
        trains_data = pd.concat(all_trains_data, ignore_index=True)
        st.success(f"Fetched a total of {len(trains_data)} train entries from {start_date} to {end_date}.", icon="✅")

        # ---- FILTER TRAIN DATA ----
        st.write("Filtering train data to only include trains that pass through stations: HKI, OL, ROI.")
        # Only keep trains that have timeTableRows and pass the required stations filter.
        mask = trains_data["timeTableRows"].apply(train_passes_required_stations)
        filtered_trains = trains_data[mask]
        st.success(f"Filtered down to {len(filtered_trains)} trains that pass all required stations.", icon="✅")

        # === ENRICH THE FILTERED TRAIN DATA ===
        if "timeTableRows" in filtered_trains.columns:
            # Create a column to help processing by month
            filtered_trains["departureMonth"] = pd.to_datetime(filtered_trains["departureDate"]).dt.to_period("M")
            months = filtered_trains["departureMonth"].unique()
            enriched_data = []

            for month in months:
                st.write(f"Enriching data for month: `{month}`. Please wait...")
                month_data = filtered_trains[filtered_trains["departureMonth"] == month]

                def enrich_timetable_row(row):
                    try:
                        parsed_row = ast.literal_eval(row) if isinstance(row, str) else row
                        if isinstance(parsed_row, list):
                            enriched_rows = []
                            for entry in parsed_row:
                                station_name = station_metadata.loc[
                                    station_metadata["stationShortCode"] == entry["stationShortCode"],
                                    "stationName"
                                ]
                                station_name_value = station_name.iloc[0] if not station_name.empty else None
                                enriched_entry = {"stationName": station_name_value}
                                enriched_entry.update(entry)
                                enriched_rows.append(enriched_entry)
                            return enriched_rows
                        return parsed_row
                    except Exception as e:
                        st.error(f"Error enriching timeTableRows: {e}")
                        return row

                month_data["timeTableRows"] = month_data["timeTableRows"].apply(enrich_timetable_row)
                enriched_data.append(month_data)

            st.success("Train data enrichment complete.", icon="✅")
            trains_data_enriched = pd.concat(enriched_data, ignore_index=True)
        else:
            st.error("No 'timeTableRows' column found in the train data.")

        # Save the enriched train data
        save_dataframe_to_csv(trains_data_enriched, CSV_ALL_TRAINS)
        print_memory_usage(trains_data_enriched, "trains_data_enriched")
    else:
        st.error("No train data available for the specified date range.", icon="🚨")
        st.stop()

    # * === FETCH FMI WEATHER DATA ===

    # TODO Interpolate data??

    all_fmi_data = []  # List to store weather data for each day
    ems_metadata = []  # List to store station metadata (only once)

    current_date = start_date

    while current_date <= end_date:
        st.write(f"Fetching FMI data for {current_date}...")

        # Call fetch_fmi_data for the current date
        daily_fmi_data, daily_ems_metadata = fetch_fmi_data(FMI_BBOX, current_date, chunk_hours=2)

        if not daily_fmi_data.empty:
            all_fmi_data.append(daily_fmi_data)  # Store the retrieved weather data

            # Store metadata only if it's the first call (it remains constant)
            if not ems_metadata:
                ems_metadata.append(daily_ems_metadata)

        else:
            st.warning(f"No weather data available for {current_date}", icon="⚠️")

        # Move to the next day
        current_date += timedelta(days=1)

    # Combine all fetched data into a single DataFrame
    if all_fmi_data:
        fmi_data_combined = pd.concat(all_fmi_data, ignore_index=True)

        # Drop duplicates based on timestamp and station_name
        fmi_data_combined = clean_fmi_data(fmi_data_combined)

        st.write("Data manipulation completed.")

        st.success(f"Fetched and processed FMI data from {start_date} to {end_date}.", icon="✅")

        # Print memory usage of the transformed DataFrame
        print_memory_usage(fmi_data_combined, "fmi_data_final")

        # Save the transformed weather data to CSV
        save_dataframe_to_csv(fmi_data_combined, CSV_FMI)

        # Save the EMS metadata (only once)
        if ems_metadata:
            ems_metadata_combined = pd.concat(ems_metadata, ignore_index=True)
            save_dataframe_to_csv(ems_metadata_combined, CSV_FMI_EMS)
            st.success("Station metadata saved successfully.", icon="✅")

    else:
        st.error("No FMI weather data available for the selected date range.", icon="🚨")

    matched_stations_df = match_train_with_ems(station_metadata, ems_metadata_combined)
    save_dataframe_to_csv(matched_stations_df, CSV_CROSS_STATIONS)
    print(matched_stations_df.head())


    updated_trains_df = merge_train_weather_data(trains_data, fmi_data_combined, matched_stations_df)
    save_dataframe_to_csv(updated_trains_df, CSV_CROSS_DATA)

    print("EXECUTION COMPLETE!")
else:
    st.write("Click the button above to fetch new data from the APIs.")
