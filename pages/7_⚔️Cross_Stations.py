import pandas as pd
import os
import streamlit as st
import folium
from streamlit_folium import st_folium

from misc.const import CSV_CROSS_STATIONS, FOLDER_NAME

# Title of the Streamlit App
st.title("Train Stations & Closest EMS Viewer")

# Path to the CSV file inside the output_data folder
csv_file = os.path.join(FOLDER_NAME, CSV_CROSS_STATIONS)

try:
    # Load the DataFrame from the CSV file
    df = pd.read_csv(csv_file)

    # Check if the DataFrame is not empty
    if not df.empty:
        # Subheader for the map
        st.subheader("🚆 Train Stations & Closest EMS Stations 📍")

        # Initialize the Folium Map
        map_center = [df["train_lat"].mean(), df["train_long"].mean()]
        m = folium.Map(location=map_center, zoom_start=6)

        # Add train stations as blue markers with train icon
        for _, row in df.iterrows():
            folium.Marker(
                location=[row["train_lat"], row["train_long"]],
                popup=f"🚆 Train Station: {row['train_station_name']} ({row['train_station_short_code']})",
                tooltip=row["train_station_name"],
                icon=folium.Icon(color="blue", icon="train"),
            ).add_to(m)

        # Add EMS stations as pink markers
        for _, row in df.iterrows():
            folium.Marker(
                location=[row["ems_latitude"], row["ems_longitude"]],
                popup=f"☁️ EMS Station: {row['closest_ems_station']}",
                tooltip=row["closest_ems_station"],
                icon=folium.Icon(color="pink", icon="cloud"),
            ).add_to(m)

            # Draw a dashed line between the train station and its closest EMS
            folium.PolyLine(
                [(row["train_lat"], row["train_long"]), (row["ems_latitude"], row["ems_longitude"])],
                color="red",
                weight=2.5,
                opacity=0.7,
                dash_array="5,5",  # Makes the line dashed
                tooltip=f"📏 Distance: {row['distance_km']:.2f} km",  # Shows distance in km on hover
            ).add_to(m)

        # Display the Folium map in Streamlit
        st_folium(m, width=1000, height=600, returned_objects=[])

        # Subheader for the table (Below the Map)
        st.subheader("📊 Matched Train Stations with Closest EMS Stations")

        # Display the DataFrame
        st.dataframe(df, height=800, width=1000)

        # Additional information about the DataFrame
        st.write(f"📌 **Total Train Stations:** {df.shape[0]}")
        st.write(f"📍 **Columns in Dataset:** {df.shape[1]}")

    else:
        st.error("The dataset is empty. No data to display.")

except FileNotFoundError:
    st.error(f"The CSV file '{csv_file}' was not found.")
except Exception as e:
    st.error(f"An error occurred while loading the CSV file: {e}")
