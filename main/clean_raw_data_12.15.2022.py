import pandas as pd 
import glob 

# for csvs in the raw_data folder, delete the first row
for file in glob.glob("raw_data_12.15.2022/*.csv"):
    df = pd.read_csv(file, encoding = "ISO-8859-1",header=1, low_memory=False)
    df = df.iloc[1:]
    # write to clean_data_12.15.2022 folder, get filename from after the slash 
    filename = file.split("/")[-1]
    print(filename)
    df.to_csv(f"clean_data_12.15.2022/{filename}", index=False)

# for csvs in clean_data_12.15.2022 folder, check if all headers are the same
columns = None
for file in glob.glob("clean_data_12.15.2022/*.csv"):
    df = pd.read_csv(file, encoding = "ISO-8859-1")
    if columns is not None:
        previous_columns = columns
        columns = df.columns
        # check if column arrays are the same
        print(len(columns))
        print(len(previous_columns))
        if len(columns) != len(previous_columns):
            # print columns that are not the same
            print(file)
            for column in columns:
                if column not in previous_columns:
                    print(column)
            for column in previous_columns:
                if column not in columns:
                    print(column)
