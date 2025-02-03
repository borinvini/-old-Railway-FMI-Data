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

    # -------------------------------------------------------------------
    # FETCH RAILWAY DATA
    # -------------------------------------------------------------------

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

        # === ENRICH THE FILTERED TRAIN DATA BY MONTH ===
        if "timeTableRows" in filtered_trains.columns:
            # Create a column to help processing by month
            filtered_trains["departureMonth"] = pd.to_datetime(filtered_trains["departureDate"]).dt.to_period("M")
            months = filtered_trains["departureMonth"].unique()
            
            # Define a function to enrich the timetable rows.
            def enrich_timetable_row(row):
                try:
                    parsed_row = ast.literal_eval(row) if isinstance(row, str) else row
                    if isinstance(parsed_row, list):
                        enriched_rows = []
                        for entry in parsed_row:
                            # Look up the station name using the station metadata.
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

            # Process each month separately.
            for month in months:
                st.write(f"Enriching data for month: `{month}`. Please wait...")
                month_data = filtered_trains[filtered_trains["departureMonth"] == month].copy()
                
                # Convert the timeTableRows for each row.
                month_data["timeTableRows"] = month_data["timeTableRows"].apply(enrich_timetable_row)
                
                st.success(f"Train data enrichment complete for month: `{month}`.", icon="✅")
                print_memory_usage(month_data, f"trains_data_enriched_{month}")

                # Save the enriched train data for the month to a separate CSV file.
                # We assume month is a Period; convert it to a two-digit month string.
                month_str = str(month.month).zfill(2) if hasattr(month, 'month') else str(month).split('-')[1]
                csv_filename = f"all_trains_data_{month_str}.csv"
                save_dataframe_to_csv(month_data, csv_filename)
                st.success(f"Enriched train data for month {month_str} saved to {csv_filename}.", icon="✅")
        else:
            st.error("No 'timeTableRows' column found in the train data.")
    else:
        st.error("No train data available for the specified date range.", icon="🚨")
        st.stop()

    # -------------------------------------------------------------------
    # * === FETCH FMI WEATHER DATA BY MONTH ===
    # -------------------------------------------------------------------

    all_ems_metadata = []  # List to store station metadata (only once)
    monthly_fmi_data = []  # List to accumulate daily data for the current month

    current_date = start_date
    current_month = current_date.month  # Track the current month

    while current_date <= end_date:
        # Check if the current day is in a new month compared to the accumulator
        if current_date.month != current_month:
            # Process the accumulated data for the previous month before moving to the new month
            if monthly_fmi_data:
                fmi_data_month = pd.concat(monthly_fmi_data, ignore_index=True)
                fmi_data_month = clean_fmi_data(fmi_data_month)
                
                st.write(f"Data manipulation completed for month {current_month}.")
                st.success(f"Fetched and processed FMI data for month {current_month}.", icon="✅")
                print_memory_usage(fmi_data_month, f"fmi_data_month_{current_month}")
                
                month_str = str(current_month).zfill(2)
                csv_filename = f"fmi_weather_observations_{month_str}.csv"
                save_dataframe_to_csv(fmi_data_month, csv_filename)
                st.success(f"Monthly FMI data saved to {csv_filename}", icon="✅")
                
                # Reset the monthly accumulator for the new month
                monthly_fmi_data = []
            
            # Update the current_month to the new month before processing the day's data
            current_month = current_date.month

        st.write(f"Fetching FMI data for {current_date}...")
        daily_fmi_data, daily_ems_metadata = fetch_fmi_data(FMI_BBOX, current_date, chunk_hours=2)
        
        if not daily_fmi_data.empty:
            monthly_fmi_data.append(daily_fmi_data)
            if not all_ems_metadata:
                all_ems_metadata.append(daily_ems_metadata)
        else:
            st.warning(f"No weather data available for {current_date}", icon="⚠️")
        
        current_date += timedelta(days=1)

    # Process any remaining data if the loop ended mid-month
    if monthly_fmi_data:
        fmi_data_month = pd.concat(monthly_fmi_data, ignore_index=True)
        fmi_data_month = clean_fmi_data(fmi_data_month)
        st.write(f"Final data manipulation completed for month {current_month}.")
        st.success(f"Fetched and processed FMI data for month {current_month}.", icon="✅")
        print_memory_usage(fmi_data_month, f"fmi_data_month_{current_month}")
        month_str = str(current_month).zfill(2)
        csv_filename = f"fmi_weather_observations_{month_str}.csv"
        save_dataframe_to_csv(fmi_data_month, csv_filename)
        monthly_fmi_data = []

    # Combine and save EMS metadata (only once)
    if all_ems_metadata:
        ems_metadata_combined = pd.concat(all_ems_metadata, ignore_index=True)
        ems_metadata_combined.drop_duplicates(inplace=True)
        save_dataframe_to_csv(ems_metadata_combined, CSV_FMI_EMS)
        st.success("Station metadata saved successfully.", icon="✅")
    else:
        st.error("No EMS station metadata available.", icon="🚨")
    
    # -------------------------------------------------------------------
    # * === CROSS DATA ===
    # -------------------------------------------------------------------
    
    # -------------------------------------------------------------------
    # Step 1: Create and save the matched stations DataFrame
    # -------------------------------------------------------------------
    # (Assume station_metadata and ems_metadata_combined are already loaded)
    matched_stations_df = match_train_with_ems(station_metadata, ems_metadata_combined)
    save_dataframe_to_csv(matched_stations_df, CSV_CROSS_STATIONS)
    st.write("Matched stations:")
    st.write(matched_stations_df.head())

    # -------------------------------------------------------------------
    # Step 2: Process monthly data
    # -------------------------------------------------------------------
    # We'll loop over the months 1 to 12. For each month, we load the corresponding
    # train and FMI CSV files, perform the merge, and save the output as a monthly cross data file.
    for m in range(1, 13):
        month_str = str(m).zfill(2)  # e.g., "01", "02", etc.
        trains_file = os.path.join(os.getcwd(), FOLDER_NAME, f"all_trains_data_{month_str}.csv")
        fmi_file = os.path.join(os.getcwd(), FOLDER_NAME, f"fmi_weather_observations_{month_str}.csv")
        
        # Check if both files exist.
        if os.path.exists(trains_file) and os.path.exists(fmi_file):
            st.write(f"Processing data for month {month_str}...")
            
            # Load the monthly train and FMI data.
            trains_data_month = pd.read_csv(trains_file)
            fmi_data_month = pd.read_csv(fmi_file)
            
            # (Optional) Convert date/time columns if needed.
            if "departureDate" in trains_data_month.columns:
                trains_data_month["departureDate"] = pd.to_datetime(trains_data_month["departureDate"])
            if "timestamp" in fmi_data_month.columns:
                fmi_data_month["timestamp"] = pd.to_datetime(fmi_data_month["timestamp"])
            
            # ---- Convert "timeTableRows" from string to Python objects ----
            if "timeTableRows" in trains_data_month.columns:
                trains_data_month["timeTableRows"] = trains_data_month["timeTableRows"].apply(
                    lambda x: ast.literal_eval(x) if isinstance(x, str) else x
                )
            
            # Merge the monthly train data with the monthly FMI data using matched stations.
            cross_data_month = merge_train_weather_data(trains_data_month, fmi_data_month, matched_stations_df)
            
            # Optionally, print memory usage for the merged data of the month.
            print_memory_usage(cross_data_month, f"cross_data_{month_str}")
            
            # Save the merged result as cross_data_XX.csv (where XX is the month number)
            output_filename = f"cross_data_{month_str}.csv"
            save_dataframe_to_csv(cross_data_month, output_filename)
            st.success(f"Cross data for month {month_str} saved as {output_filename}.")
        else:
            st.warning(f"Data files for month {month_str} not found. Skipping this month.")

    st.success("All monthly cross data processing completed!")

    print("EXECUTION COMPLETE!")

else:
    st.write("Click the button above to fetch new data from the APIs.")
