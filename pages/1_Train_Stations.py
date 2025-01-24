import pandas as pd
import os

import folium
import streamlit as st
from streamlit_folium import st_folium

from misc.const import CSV_TRAIN_STATIONS, FOLDER_NAME

# Title of the Streamlit App
st.title("Station Metadata Viewer")

# Subheader for the map
st.subheader("Station Locations on Map")

# Path to the CSV file inside the output_data folder
csv_file = os.path.join(FOLDER_NAME, CSV_TRAIN_STATIONS)

try:
    # Load the DataFrame from the CSV file
    station_metadata = pd.read_csv(csv_file)
    
    # Check if the DataFrame is not empty
    if not station_metadata.empty:
        # Explanation of marker colors
        st.markdown(
            """
            <div style="font-size:16px;">
                <span style="color:blue;"><b>Blue markers</b></span>: Passenger stations<br>
                <span style="color:orange;"><b>Orange markers</b></span>: Non-passenger stations
            </div>
            """,
            unsafe_allow_html=True
        )

        # Create a Folium Map centered at the middle of Finland
        map_center = [64.0, 26.0]  # Approximate geographic center of Finland
        m = folium.Map(location=map_center, zoom_start=6)

        # Add markers for each station with color based on passengerTraffic
        for _, row in station_metadata.iterrows():
            marker_color = "blue" if row["passengerTraffic"] else "orange"
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=f"Station: {row['stationName']}",
                tooltip=row["stationName"],
                icon=folium.Icon(color=marker_color, icon="info-sign"),
            ).add_to(m)

        # Display the Folium map in Streamlit
        st_folium(m, width=1000, height=600, returned_objects=[])

        # Subheader for the table
        st.subheader("Displaying the Station Metadata DataFrame")

        # Display the DataFrame
        st.dataframe(station_metadata, height=1000, width=600)

        # Additional information about the DataFrame
        st.write("Number of rows:", station_metadata.shape[0])
        st.write("Number of columns:", station_metadata.shape[1])
    else:
        st.error("The station_metadata DataFrame is empty.")
except FileNotFoundError:
    st.error(f"The CSV file '{csv_file}' was not found.")
except Exception as e:
    st.error(f"An error occurred while loading the CSV file: {e}")
