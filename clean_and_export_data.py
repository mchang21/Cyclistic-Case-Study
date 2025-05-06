import pandas as pd

# Import all csv files
data = []
for i in range(1, 13):
    index = "0" + str(i) if i < 10 else str(i) 
    file_path = f"data/2024{index}-divvy-tripdata.csv"
    data.append(pd.read_csv(file_path))

# Merge files to one dataframe
df = pd.concat(data, ignore_index=True)

# Check data types of the dataframe
print(df.dtypes, '\n')

# View which columns contain missing data
num_unique_missing_rows_before_cleaning = df[df.isna().any(axis=1)].drop_duplicates().shape[0]
print(f"Number of unique rows with missing data: {num_unique_missing_rows_before_cleaning}", '\n')

missing_values = df.isna().sum()
missing_columns = missing_values[missing_values > 0]

print("Columns with missing values:")
print(missing_columns, '\n')

# Drop rows missing values from end lat/lng columns
df = df.dropna(subset=["end_lat", "end_lng"])

coord_to_start_station_name = dict()
coord_to_end_station_name = dict()

# Precompute coordinates
df["start_coord"] = list(zip(df["start_lat"].round(2), df["start_lng"].round(2)))
df["end_coord"] = list(zip(df["end_lat"].round(2), df["end_lng"].round(2)))

# Filter rows with valid station names
valid_start = df[pd.notna(df["start_station_name"])]
valid_end = df[pd.notna(df["end_station_name"])]

# Map coordinates to station name
for coord, name in zip(valid_start["start_coord"], valid_start["start_station_name"]):
    if coord not in coord_to_start_station_name:
        coord_to_start_station_name[coord] = name

for coord, name in zip(valid_end["end_coord"], valid_end["end_station_name"]):
    if coord not in coord_to_end_station_name:
        coord_to_end_station_name[coord] = name

# Validate that each coordinate corresponds to a single station
print("Is there a 1:1 ratio between coordinates and name: ",len(coord_to_start_station_name) == len(coord_to_start_station_name.values()))
print("Is there a 1:1 ratio between coordinates and name: ",len(coord_to_end_station_name) == len(coord_to_end_station_name.values()), '\n')

# Fill missing start station names using the coord mapping
df["start_station_name"] = df.apply(
    lambda row: coord_to_start_station_name.get(row["start_coord"], row["start_station_name"])
    if pd.isna(row["start_station_name"]) else row["start_station_name"],
    axis=1
)

# Fill missing end station names using the coord mapping
df["end_station_name"] = df.apply(
    lambda row: coord_to_end_station_name.get(row["end_coord"], row["end_station_name"])
    if pd.isna(row["end_station_name"]) else row["end_station_name"],
    axis=1
)

# Optionally drop the temporary coord columns
df.drop(columns=["start_coord", "end_coord"], inplace=True)

missing_values = df.isna().sum()
missing_columns = missing_values[missing_values > 0]

print("Columns with missing values after processing station names:")
print(missing_columns, '\n')

start_station_name_to_id = dict()
end_station_name_to_id = dict()

# Filter rows with valid station ids
valid_start = df[pd.notna(df["start_station_id"])]
valid_end = df[pd.notna(df["end_station_id"])]

# Map station name to id
for name, station_id in zip(valid_start["start_station_name"], valid_start["start_station_id"]):
    if pd.notna(name) and name not in start_station_name_to_id:
        start_station_name_to_id[name] = station_id

for name, station_id in zip(valid_end["end_station_name"], valid_end["end_station_id"]):
    if pd.notna(name) and name not in end_station_name_to_id:
        end_station_name_to_id[name] = station_id

# Fill missing start station ids using the name-to-id mapping
df["start_station_id"] = df.apply(
    lambda row: start_station_name_to_id.get(row["start_station_name"])
    if pd.isna(row["start_station_id"]) else row["start_station_id"],
    axis=1
)

# Fill missing end station ids using the name-to-id mapping
df["end_station_id"] = df.apply(
    lambda row: end_station_name_to_id.get(row["end_station_name"])
    if pd.isna(row["end_station_id"]) else row["end_station_id"],
    axis=1
)

# View final number of rows with missing data
num_unique_missing_rows_after_cleaning = df[df.isna().any(axis=1)].drop_duplicates().shape[0]
print(f"Number of unique rows with missing data: {num_unique_missing_rows_after_cleaning}", '\n')

missing_values = df.isna().sum()
missing_columns = missing_values[missing_values > 0]

print("Columns with missing values after processing names and IDs:")
print(missing_columns, '\n')

percent_missing = (num_unique_missing_rows_before_cleaning / len(df)) * 100
print(f"Percentage of data lost if dropping rows with missing values before cleaning: {percent_missing:.2f}%")
percent_missing = (num_unique_missing_rows_after_cleaning / len(df)) * 100
print(f"Percentage of data lost if dropping rows with missing values after cleaning : {percent_missing:.2f}%")

# Output:
# Percentage of data lost if dropping rows with missing values before cleaning: 28.23%
# Percentage of data lost if dropping rows with missing values after cleaning : 0.68%

# Drop rows with missing data
df = df.dropna()

# Write the clean data to a csv
# df.to_csv("cleaned_data.csv", index=False)
