import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from finnish_railway.data_handler import get_trains_by_date, load_railway_metadata
from finnish_railway.data_visualization import display_train_details
from misc.misc_functions import print_memory_usage, save_dataframe_to_csv

from misc.const import START_DATE, END_DATE
from misc.const import FIN_RAILWAY_BASE_URL, FIN_RAILWAY_STATIONS, FIN_RAILWAY_TRAIN_CAT
from misc.const import CSV_TRAIN_CATEGORIES, CSV_ALL_TRAINS, CSV_TRAIN_STATIONS


# Streamlit App
st.title("Finnish Railway Data Fetcher")

# Button to fetch new data
fetch_data = st.button("Fetch New Data from API")

if fetch_data:
    st.info("Fetching new data from the API. Please wait...")

    # Set the base URL for the Finnish Railway API    
    base_url = FIN_RAILWAY_BASE_URL

    # Fetch and save station metadata
    api_url_path = f"{base_url}{FIN_RAILWAY_STATIONS}"
    station_metadata = load_railway_metadata(api_url_path)
    if station_metadata.empty:
        st.error("No station metadata available. Exiting...")
    else:
        st.success("Station metadata loaded successfully.")
        save_dataframe_to_csv(station_metadata, CSV_TRAIN_STATIONS)

    # Fetch train data from START_DATE to END_DATE
    if isinstance(START_DATE, str) and isinstance(END_DATE, str):
        current_date = datetime.strptime(START_DATE, "%Y-%m-%d")
        end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
    else:
        st.error("Error: START_DATE and END_DATE must be strings in 'YYYY-MM-DD' format.")
        st.stop()

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
        st.success(f"Fetched a total of {len(trains_data)} trains from {START_DATE} to {END_DATE}.")
        save_dataframe_to_csv(trains_data, CSV_ALL_TRAINS)

        # Display DataFrame information
        st.subheader("Train DataFrame Information:")
        st.text(trains_data.info(memory_usage="deep"))

        # Print memory usage of the DataFrame
        print_memory_usage(trains_data)

        # Display details for a specific train
        display_train_details(trains_data, train_number=1, departure_date=START_DATE)
    else:
        st.error("No train data available for the specified date range.")
else:
    st.write("Click the button above to fetch new data from the API.")
