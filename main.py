import streamlit as st
import pandas as pd
import os
import ast
from datetime import datetime, timedelta

from finnish_railway.data_handler import get_trains_by_date, load_railway_metadata
from finnish_railway.data_visualization import display_train_details
from misc.misc_functions import print_memory_usage, save_dataframe_to_csv

from misc.const import END_DATE, FIN_RAILWAY_BASE_URL, FIN_RAILWAY_STATIONS, FIN_RAILWAY_TRAIN_CAT, START_DATE
from misc.const import CSV_TRAIN_CATEGORIES, CSV_ALL_TRAINS, CSV_TRAIN_STATIONS, FOLDER_NAME

# Streamlit App
st.title("Data Fetcher")

st.subheader("Finnish Railway")

# Check if the output_data folder is empty
output_data_folder = FOLDER_NAME
output_files = os.listdir(output_data_folder)

if output_files:
    # If the folder is not empty, display file names and their sizes as warnings
    st.info("We already have train data stored in the application.", icon="ℹ️")
    st.write("Files in output_data Folder:")

    # Display file names and sizes
    for file_name in output_files:
        file_path = os.path.join(output_data_folder, file_name)
        if os.path.isfile(file_path):
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert size to MB
            st.warning(f"File: `{file_name}` - Size: {file_size:.2f} MB", icon="📂")
else:
    # If the folder is empty
    st.warning("The train data folder is empty. You may need to fetch new train data.", icon="⚠️")

# Select date range for data fetching
st.write("Select Date Range for Train Data Fetching")

# Convert START_DATE and END_DATE to datetime
default_start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
default_end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

# Date inputs for start and end dates
start_date = st.date_input("Start Date", value=default_start_date, min_value=datetime(2000, 1, 1))
end_date = st.date_input("End Date", value=default_end_date, min_value=start_date)

# Button to fetch new data
fetch_data = st.button("Fetch New Data from Finnish Railway")

if fetch_data:
    st.info("Fetching new data from the API. Please wait...", icon="ℹ️")

    # Validate date range
    if start_date > end_date:
        st.error("Error: Start Date must be before or equal to End Date.", icon="🚨")
        st.stop()

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

        # Function to extract timetable rows into a DataFrame
        def extract_timetable_rows(rows_column):
            timetable_rows_list = []
            for row in rows_column:
                try:
                    # Parse the row (JSON-like string)
                    parsed_row = ast.literal_eval(row) if isinstance(row, str) else row
                    if isinstance(parsed_row, list):  # Ensure it's a list of dictionaries
                        timetable_rows_list.extend(parsed_row)
                except Exception as e:
                    print(f"Error processing row: {e}")
            # Create a DataFrame from the list of dictionaries
            return pd.DataFrame(timetable_rows_list)

        # Extract timetable rows
        if "timeTableRows" in trains_data.columns:
            timetable_rows_data = extract_timetable_rows(trains_data["timeTableRows"])

            # Merge stationName into timetable_rows_data using stationShortCode as the key
            timetable_rows_data = timetable_rows_data.merge(
                station_metadata[["stationShortCode", "stationName"]],
                on="stationShortCode",
                how="left"
            )

            # Reorder columns to place stationName as the first column
            columns_order = ["stationName"] + [col for col in timetable_rows_data.columns if col != "stationName"]
            timetable_rows_data = timetable_rows_data[columns_order]

            # Save the timetable_rows_data DataFrame to a CSV file
            timetable_rows_csv_path = os.path.join(output_data_folder, "timetable_rows_data.csv")
            timetable_rows_data.to_csv(timetable_rows_csv_path, index=False)
            st.success(f"Saved timetable_rows_data to {timetable_rows_csv_path}.", icon="✅")

            # Print the updated timetable_rows_data DataFrame
            print("\nUpdated TimeTableRows Data with stationName as the first column:")
            print(timetable_rows_data.head())
            print(f"Updated {len(timetable_rows_data)} rows with station names.")
        else:
            print("No 'timeTableRows' column found in the trains_data DataFrame.")

        # Save the enriched trains_data DataFrame to CSV
        save_dataframe_to_csv(trains_data, CSV_ALL_TRAINS)

        # Print memory usage of the DataFrame
        print_memory_usage(trains_data, "trains_data")

        # Display details for a specific train
        display_train_details(trains_data, train_number=1, departure_date=start_date.strftime("%Y-%m-%d"))
    else:
        st.error("No train data available for the specified date range.")
else:
    st.write("Click the button above to fetch new data from the Finnish Railway API.")
