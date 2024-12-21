import os

def save_file(dataframe, filename):
    """Save a DataFrame to a CSV file."""
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, filename)
    dataframe.to_csv(file_path, index=False)
    return file_path
