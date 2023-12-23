import pandas as pd 
import glob, os, gzip 

# for csvs in the raw_data folder, delete the first row
for file in glob.glob("raw_data_12.15.2022/*.csv"):
    df = pd.read_csv(file, encoding = "ISO-8859-1",header=1, low_memory=False)
    df = df.iloc[1:]
    # write to clean_data_12.15.2022 folder, get filename from after the slash 
    filename = file.split("/")[-1]
    print(filename)
    df['hospital_id'] = filename.split('.')[0]
    df.to_csv(f"clean_data_12.15.2022/{filename}", index=False)

# for csvs in clean_data_12.15.2022 folder, check if all headers are the same
all_columns, previous_columns = [], None
for file in glob.glob("clean_data_12.15.2022/*.csv"):
    df = pd.read_csv(file, encoding="ISO-8859-1")
    current_columns = df.columns
    all_columns.append(df.columns) 
    if previous_columns is not None and len(current_columns) != len(previous_columns):
        print("File:", file)
        print("Columns that are not the same:")
        for column in current_columns:
            if column not in previous_columns:
                print(column)
        print("Columns that are missing:")
        for column in previous_columns:
            if column not in current_columns:
                print(column)
    previous_columns = current_columns

# convert all_columns index to set  
all_columns = [item for sublist in all_columns for item in sublist]
columns_set = set(all_columns)
print(f"Total number of columns: {len(columns_set)}")

# concatenate all files in clean_data_12.15.2022 folder on shared columns
df = pd.concat([pd.read_csv(file, encoding="ISO-8859-1", low_memory=False) for file in glob.glob("clean_data_12.15.2022/*.csv")])
# assert no columns begin with Unnamed 
assert not any(column.startswith('Unnamed') for column in df.columns)

# separate out kahi facility which uses entirely different columns 
kahi_df = df[df['hospital_id'] == '990298651-1477519908_SUTTER-PACIFIC-KAHI-MOHALA_standardcharges'].dropna(axis=1, how='all')
# drop kahi from initial df 
df = df[df['hospital_id'] != '990298651-1477519908_SUTTER-PACIFIC-KAHI-MOHALA_standardcharges']

# compress file 
df.to_pickle('sutter_combined_chargemasters.zip', index=False, compression='zip')
kahi_df.to_csv('kahi_chargemaster.csv', index=False)

# Compress the file 'sutter_combined_chargemasters.csv' using gzip
with open('sutter_combined_chargemasters.csv', 'rb') as file_in:
    with gzip.open('sutter_combined_chargemasters.gz', 'wb') as file_out:
        file_out.writelines(file_in)
