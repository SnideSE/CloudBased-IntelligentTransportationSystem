import csv
import os
import re

# Define headers for the dataset
headers = [
    "driverID",
    "carPlateNumber",
    "Latitude",
    "Longitude",
    "Speed",
    "Direction",
    "siteName",
    "Time",
    "isRapidlySpeedup",
    "isRapidlySlowdown",
    "isNeutralSlide",
    "isNeutralSlideFinished",
    "neutralSlideTime",
    "isOverspeed",
    "isOverspeedFinished",
    "overspeedTime",
    "isFatigueDriving",
    "isHthrottleStop",
    "isOilLeak",
]

# Folder containing the input files
input_folder = "input_data"

# Folder to store the output CSV files
output_folder = "output_data"

# Create the output folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Function to clean the data by removing non-alphanumeric characters
def clean_row(row):
    cleaned_row = []
    for i, cell in enumerate(row):
        if i == 1:  # Assuming the car plate number is the second field (index 1)
            cleaned_cell = re.sub(r'[^a-zA-Z0-9]', '', cell)
        else:
            cleaned_cell = cell
        cleaned_row.append(cleaned_cell)
    return cleaned_row

# Loop through the files in the input folder
for filename in os.listdir(input_folder):
    input_file = os.path.join(input_folder, filename)
    output_file = os.path.join(output_folder, f"output_{filename}.csv")

    # Parse and write the data to a CSV file
    with open(input_file, "r", encoding="utf-8") as infile, open(output_file, "w", newline="", encoding="utf-8") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        writer.writerow(headers)
        for row in reader:
            cleaned_row = clean_row(row)
            writer.writerow(cleaned_row)

    print(f"Data from {input_file} has been formatted and saved to {output_file}")