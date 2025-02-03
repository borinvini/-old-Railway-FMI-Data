import time
import os
import ast
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from fmiopendata.wfs import download_stored_query

from cross_data.cross_functions import match_train_with_ems, merge_train_weather_data
from finnish_railway.data_handler import get_trains_by_date, load_railway_metadata
from finnish_weather.data_handler import clean_fmi_data, fetch_fmi_data
from misc.misc_functions import append_dataframe_to_csv, print_memory_usage, save_dataframe_to_csv
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

    # * === FETCH RAILWAY DATA ===

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

    # * === FETCH FMI WEATHER DATA BY MONTH ===

    all_ems_metadata = []  # List to store station metadata (only once)
    monthly_fmi_data = []  # List to accumulate daily data for the current month

    current_date = start_date
    current_month = current_date.month  # Track the current month

    while current_date <= end_date:
        st.write(f"Fetching FMI data for {current_date}...")

        # Call fetch_fmi_data for the current date with chunk_hours=2
        daily_fmi_data, daily_ems_metadata = fetch_fmi_data(FMI_BBOX, current_date, chunk_hours=2)

        if not daily_fmi_data.empty:
            monthly_fmi_data.append(daily_fmi_data)  # Store the day's weather data

            # Store EMS metadata only once (assuming it remains constant)
            if not all_ems_metadata:
                all_ems_metadata.append(daily_ems_metadata)
        else:
            st.warning(f"No weather data available for {current_date}", icon="⚠️")

        # Check if we've reached a new month or if this is the last day in the range
        if (current_date.month != current_month) or (current_date == end_date):
            if monthly_fmi_data:
                # Combine daily data for the current month
                fmi_data_month = pd.concat(monthly_fmi_data, ignore_index=True)
                fmi_data_month = clean_fmi_data(fmi_data_month)
                
                st.write(f"Data manipulation completed for month {current_month}.")

                st.success(f"Fetched and processed FMI data for month {current_month}.", icon="✅")

                # Print memory usage for the monthly data
                print_memory_usage(fmi_data_month, f"fmi_data_month_{current_month}")

                # Append the monthly data to the CSV file
                append_dataframe_to_csv(fmi_data_month, CSV_FMI)

                # Free the monthly data from memory by resetting the list
                monthly_fmi_data = []

            # Update current_month to the month of the current_date
            current_month = current_date.month

        # Move to the next day
        current_date += timedelta(days=1)

    # Process any remaining data if the loop ended mid-month
    if monthly_fmi_data:
        fmi_data_month = pd.concat(monthly_fmi_data, ignore_index=True)
        fmi_data_month = clean_fmi_data(fmi_data_month)
        st.write(f"Final data manipulation completed for month {current_month}.")
        st.success(f"Fetched and processed FMI data for month {current_month}.", icon="✅")
        print_memory_usage(fmi_data_month, f"fmi_data_month_{current_month}")
        append_dataframe_to_csv(fmi_data_month, CSV_FMI)
        monthly_fmi_data = []

    # Combine and save EMS metadata (only once)
    if all_ems_metadata:
        ems_metadata_combined = pd.concat(all_ems_metadata, ignore_index=True)
        # Optionally drop duplicate rows if the same EMS station appears more than once
        ems_metadata_combined.drop_duplicates(inplace=True)
        save_dataframe_to_csv(ems_metadata_combined, CSV_FMI_EMS)
        st.success("Station metadata saved successfully.", icon="✅")
    else:
        st.error("No EMS station metadata available.", icon="🚨")
    
    # -------------------------------------------------------------------
    # * === CROSS DATA ===
    # -------------------------------------------------------------------
    
    # -------------------------------------------------------------------
    # Load FMI data from the CSV file into fmi_data_combined
    # -------------------------------------------------------------------
    fmi_file_path = os.path.join(os.getcwd(), FOLDER_NAME, CSV_FMI)
    fmi_data_combined = pd.read_csv(fmi_file_path)
    print(f"FMI data loaded from {fmi_file_path} with {len(fmi_data_combined)} rows.")

    # -------------------------------------------------------------------
    # Proceed with merging data
    # -------------------------------------------------------------------

    # Create the matched stations DataFrame using your station metadata and EMS metadata.
    matched_stations_df = match_train_with_ems(station_metadata, ems_metadata_combined)
    save_dataframe_to_csv(matched_stations_df, CSV_CROSS_STATIONS)
    print("Matched stations:")
    print(matched_stations_df.head())

    # Merge the train data with the FMI data using the matched stations.
    updated_trains_df = merge_train_weather_data(trains_data, fmi_data_combined, matched_stations_df)
    save_dataframe_to_csv(updated_trains_df, CSV_CROSS_DATA)

    print("EXECUTION COMPLETE!")
    fmi_data_combined = []
else:
    st.write("Click the button above to fetch new data from the APIs.")
