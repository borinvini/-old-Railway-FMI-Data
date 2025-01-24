import pandas as pd
from IPython.display import display

def display_train_details(trains_data, train_number, departure_date):
    """
    Display details of a specific train on a specific date, including its other columns and formatted timeTableRows.

    Args:
        trains_data (pd.DataFrame): The DataFrame containing train data.
        train_number (int): The train number to display.
        departure_date (str): The departure date of the train in 'YYYY-MM-DD' format.
    """
    if trains_data.empty:
        print("The trains_data DataFrame is empty.")
        return

    # Filter the DataFrame for the specified train number and departure date
    train_row = trains_data[(trains_data["trainNumber"] == train_number) & (trains_data["departureDate"] == departure_date)]

    if train_row.empty:
        print(f"No train found with trainNumber {train_number} on {departure_date}.")
        return

    # Display other columns for the train (excluding 'timeTableRows')
    print(f"\nDetails of Train Number {train_number} on {departure_date}:")
    other_cols = train_row.iloc[0].drop("timeTableRows")  # Exclude 'timeTableRows'
    other_cols_df = pd.DataFrame([other_cols])
    print(other_cols_df.to_markdown(index=False))

    # Extract and display 'timeTableRows' in table format
    time_table_rows = train_row.iloc[0]["timeTableRows"]

    if isinstance(time_table_rows, list):  # Ensure it's a list
        time_table_df = pd.DataFrame(time_table_rows)

        # Columns to drop
        columns_to_drop = ["trainReady", "commercialStop", "commercialTrack", "liveEstimateTime", "estimateSource", "causes"]

        # Determine which columns are being dropped
        dropped_columns = [col for col in columns_to_drop if col in time_table_df.columns]
        time_table_df = time_table_df.drop(columns=dropped_columns)

        # Print the table
        print("\nTimeTableRows in Table Format (excluding specified columns):")
        print(time_table_df.to_markdown(index=False))

        # Print the excluded column names
        if dropped_columns:
            print("\nExcluded Columns:")
            print(", ".join(dropped_columns))
        else:
            print("\nNo columns were excluded.")
    else:
        print("\ntimeTableRows is not a list or is empty.")