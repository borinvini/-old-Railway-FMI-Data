from finnish_railway.data_handler import get_trains_by_date, load_railway_metadata
from misc.misc_functions import save_dataframe_to_csv

from const import START_DATE, END_DATE
from const import FIN_RAILWAY_BASE_URL, FIN_RAILWAY_STATIONS, FIN_RAILWAY_TRAIN_CAT
from const import CSV_TRAIN_CATEGORIES, CSV_ALL_TRAINS, CSV_TRAIN_STATIONS

# Main program
if __name__ == "__main__":
    # Set the base URL for the Finnish Railway API    
    base_url = FIN_RAILWAY_BASE_URL

    # Fetch and save station metadata
    api_url_path = f"{base_url}{FIN_RAILWAY_STATIONS}"
    station_metadata = load_railway_metadata(api_url_path)
    if station_metadata.empty:
        print("No station metadata available. Exiting...")
    else:
        print("Station metadata loaded successfully.")
        save_dataframe_to_csv(station_metadata, CSV_TRAIN_STATIONS)

    # Fetch and save train categories metadata
    api_url_path = f"{base_url}{FIN_RAILWAY_TRAIN_CAT}"
    train_categories_metadata = load_railway_metadata(api_url_path)
    if train_categories_metadata.empty:
        print("No train categories metadata available. Exiting...")
    else:
        print("Train categories metadata loaded successfully.")
        save_dataframe_to_csv(train_categories_metadata, CSV_TRAIN_CATEGORIES)

    # Fetch train data for a specific date and save
    trains_data = get_trains_by_date(START_DATE)
    if not trains_data.empty:
        print(f"Fetched {len(trains_data)} trains for {START_DATE}")
        save_dataframe_to_csv(trains_data, CSV_ALL_TRAINS)
    else:
        print("No train data available.")

    if not trains_data.empty:
        print("\nTrain DataFrame Information:")
        print(trains_data.info(memory_usage='deep'))

    # Print memory usage of the DataFrame
    memory_usage = trains_data.memory_usage(deep=True).sum()
    print(f"\nMemory usage of the DataFrame: {memory_usage / (1024 * 1024):.2f} MB")


    