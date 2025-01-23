import os

from const import FOLDER_NAME

def save_dataframe_to_csv(df, csv_file):
    """
    Save a DataFrame to a specified CSV file inside the output_data folder.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        csv_file (str): The name of the CSV file.
    """

    try:
        output_dir = os.path.join(os.getcwd(), FOLDER_NAME)
        os.makedirs(output_dir, exist_ok=True)
        csv_file_path = os.path.join(output_dir, csv_file)
        df.to_csv(csv_file_path, index=False)
        print(f"DataFrame successfully saved to {csv_file_path}")
    except Exception as e:
        print(f"Error saving DataFrame to {csv_file}: {e}")