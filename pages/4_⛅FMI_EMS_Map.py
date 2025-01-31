import pandas as pd
import os
import folium
import streamlit as st
from streamlit_folium import st_folium

from misc.const import CSV_FMI_EMS, FOLDER_NAME

# Title of the Streamlit App
st.title("EMS Station Metadata Viewer")

# Subheader for the map
st.subheader("EMS Station Locations on Map")

# Path to the CSV file inside the output_data folder
csv_file = os.path.join(FOLDER_NAME, CSV_FMI_EMS)

try:
    # Load the DataFrame from the CSV file
    ems_metadata = pd.read_csv(csv_file)

    # Check if the DataFrame is not empty
    if not ems_metadata.empty:
        # Explanation of marker colors
        st.markdown(
            """
            <div style="font-size:16px;">
                <span style="color:green;"><b>Green markers</b></span>: Active EMS stations<br>
                <span style="color:red;"><b>Red markers</b></span>: Inactive EMS stations
            </div>
            """,
            unsafe_allow_html=True
        )

        # Create a Folium Map centered at Finland
        map_center = [64.0, 26.0]  # Approximate geographic center of Finland
        m = folium.Map(location=map_center, zoom_start=6)

        # Add markers for each EMS station
        for _, row in ems_metadata.iterrows():
            marker_color = "green" if row.get("active", True) else "red"  # Default to green if 'active' column is missing
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=f"EMS Station: {row['station_name']}<br>Network: {row.get('network', 'Unknown')}",
                tooltip=row["station_name"],
                icon=folium.Icon(color=marker_color, icon="info-sign"),
            ).add_to(m)

        # Display the Folium map in Streamlit
        st_folium(m, width=1000, height=600, returned_objects=[])

        # Subheader for the table
        st.subheader("EMS Metadata Table")

        # Display the DataFrame
        st.dataframe(ems_metadata, height=600, width=1000)

        # Additional information about the DataFrame
        st.write(f"**Number of EMS Stations:** {ems_metadata.shape[0]}")
        st.write(f"**Number of Columns:** {ems_metadata.shape[1]}")
    else:
        st.error("The EMS metadata DataFrame is empty.")

except FileNotFoundError:
    st.error(f"The CSV file '{csv_file}' was not found.")
except Exception as e:
    st.error(f"An error occurred while loading the CSV file: {e}")
