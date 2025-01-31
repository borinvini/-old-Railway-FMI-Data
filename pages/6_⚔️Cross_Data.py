import pandas as pd
import os
import streamlit as st

from misc.const import CSV_CROSS_DATA, FOLDER_NAME

# Title of the Streamlit App
st.title("📊 Train & Weather Data Viewer")

# Path to the CSV file inside the output_data folder
csv_file = os.path.join(FOLDER_NAME, CSV_CROSS_DATA)

try:
    # Load the DataFrame from the CSV file
    df = pd.read_csv(csv_file)

    # Check if the DataFrame is not empty
    if not df.empty:
        # Subheader for the table
        st.subheader("🚆 Matched Train Schedule & Weather Data")

        # Display the DataFrame
        st.dataframe(df, height=800, width=1000)

        # Additional information about the DataFrame
        st.write(f"📌 **Total Rows:** {df.shape[0]}")
        st.write(f"📍 **Total Columns:** {df.shape[1]}")
    else:
        st.error("The dataset is empty. No data to display.")

except FileNotFoundError:
    st.error(f"The CSV file '{csv_file}' was not found.")
except Exception as e:
    st.error(f"An error occurred while loading the CSV file: {e}")
