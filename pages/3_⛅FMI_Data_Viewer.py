import pandas as pd
import os
import streamlit as st

from misc.const import CSV_FMI, FOLDER_NAME

# Title of the Streamlit App
st.title("FMI Data Viewer")

# Path to the CSV file
csv_file = os.path.join(FOLDER_NAME, CSV_FMI)

# Session State to Manage Loading and Delay
if "show_table" not in st.session_state:
    st.session_state.show_table = False

# If the table is not ready to be shown
if not st.session_state.show_table:
    # Inform the user that the CSV file is being loaded
    st.info("Loading FMI data from the CSV file. Please wait...")

    try:
        # Load the DataFrame from the CSV file
        fmi_data = pd.read_csv(csv_file)

        # Message indicating that the loading is complete
        st.success("FMI data successfully loaded from the CSV file! 🚂")

        # Update session state to show the table and rerun the app
        st.session_state.show_table = True
        st.rerun()

    except FileNotFoundError:
        st.error(f"The CSV file '{csv_file}' was not found.")
    except Exception as e:
        st.error(f"An error occurred while loading the CSV file: {e}")

# If the table is ready to be shown
else:
    try:
        # Load the DataFrame from the CSV file
        fmi_data = pd.read_csv(csv_file, parse_dates=["timestamp"])
        fmi_data = fmi_data.sort_values(by="timestamp")
        fmi_data["date"] = fmi_data["timestamp"].dt.strftime("%Y-%m-%d")
        
        # Get unique dates for the selectbox
        unique_dates = fmi_data["date"].unique()
        selected_date = st.selectbox("Select a date:", unique_dates)
        
        # Get EMS names from columns (excluding timestamp), sort them alphabetically, and add "All EMS" option
        ems_names = ["All EMS"] + sorted([col for col in fmi_data.columns if col not in ["timestamp", "date"]])
        selected_ems = st.selectbox("Select an EMS:", ems_names)
        
        # Filter data by the selected date
        filtered_data = fmi_data[fmi_data["date"] == selected_date].drop(columns=["date"])  # Remove 'date' column
        
        # If a specific EMS is selected, filter only that column
        if selected_ems != "All EMS":
            filtered_data = filtered_data[["timestamp", selected_ems]]
        
        # Display the filtered data
        st.subheader(f"Data for {selected_date} - EMS: {selected_ems}")
        st.dataframe(filtered_data)
        
        # Additional information about the DataFrame
        st.write("Total number of rows:", filtered_data.shape[0])
        st.write("Number of columns:", filtered_data.shape[1])
    
    except Exception as e:
        st.error(f"An error occurred while processing the CSV file: {e}")
