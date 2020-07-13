import multiprocessing as mp
import os
import pandas as pd
import re

from functools import partial
from pathlib import Path

# TODO: Parallelize this
def chop_by_size(df, group_size, filename_part=""):
    i = 1
    df_dict = {}
    
    # Creates dfs of up to group_size rows and
    # return dict of unique file naming info and filtered df
    for start in range(0, df.shape[0], group_size):
        output_df = df.iloc[start : start + group_size]
        copy_filename_part = f"{filename_part}{i}"
        df_dict[copy_filename_part] = output_df
        i += 1
    return df_dict


# Handles a single df filter operation
def chop_by_columns_process(filter_df, df, column_list, group_size):
    df_dict = {}
    
    # Multi-column split shouild be like val1_val2_val3_etc
    filename_part_df = filter_df.apply(
        lambda x: "_".join(x.dropna().astype(str).values), axis=1
    )
    filename_part = filename_part_df.to_string(index=False)
    
    # Inner join on filter_df to get filtered result
    output_df = pd.merge(df, filter_df, on=column_list, how="inner")
    
    # Send to chop_by_size if group_size was set to a valid value
    if group_size > 0:
        filename_part = f"{filename_part}_"
        df_dict_update = chop_by_size(output_df, group_size, filename_part)
        df_dict.update(df_dict_update)
    else:
        df_dict[filename_part] = output_df

    return df_dict


def chop_by_columns(df, columns_to_split, group_size=0):
    column_list = [x.strip() for x in columns_to_split.split(",")]
    all_filters = df[column_list].drop_duplicates().sort_values(by=column_list)

    print(f"Found {len(all_filters)} unique combinations in the specified columns.")

    df_dict = {}
    filter_dfs = []

    # Each row is it's own filter_df. 
    # We'll use pd.merge to inner join each to the main df.
    for i, row in all_filters.iterrows():
        filter_df = all_filters.loc[i:i, :]
        filter_dfs.append(filter_df)

    # Split df for each unique combo of vals in specified column.
    # Returns a list of dicts with unique file naming info and filtered df.
    with mp.Pool() as pool:
        df_dict_list = pool.map(
            partial(
                chop_by_columns_process,
                df=df,
                column_list=column_list,
                group_size=group_size,
            ),
            filter_dfs,
        )

    # Merge dicts into one.
    for df_dict_update in df_dict_list:
        df_dict.update(df_dict_update)

    return df_dict


def clean_filename_parts(filename_part):
    return re.sub(r"[^\w\-. ]", "", filename_part)


def save_file(filename_part_df_tuple, destination, prefix):
    filename_part, df = filename_part_df_tuple
    prefix = clean_filename_parts(prefix)
    filename_part = clean_filename_parts(filename_part)
    filename = f"{destination}{prefix}_{filename_part}.csv"
    df.to_csv(filename, index=False)


def main():
    print(
        """
Welcome to CHOPPER - Creating Hundreds Of Puny Pieces Employing REPL
Right now, CHOPPER only supports CSV/Delimited Text files :-(\n
Input file path:"""
    )

    input_file = input()

    print(
        """
What is your file's separator?
(1) Comma (',') - DEFAULT
(2) Tab ('\\t')
(3) Other"""
    )

    sep_choice = input()

    sep = "," # Default
    if sep_choice == "2":
        sep = "\t"
    elif sep_choice == "3":
        sep = input("Type out your custom delimiter, please!\n")

    df = pd.read_csv(input_file, sep=sep)

    print(
        """
Cool file! Much data.

CHOPPER can:
    1. Split this into separate files by unique values in specified columns
       (ex. if your file includes 'State' and 'City' columns, you can get one 
       file per city per state).
    2. Set a max file size by row count (ex. split the whole file into files 
       of 250000 rows).
    3. Do both of the above (ex. one file per 1000 rows per city per state).
    
Pretty cool, I know!"""
    )

    print(
        """
Would you like to split by values in one or more columns?
(1) No - DEFAULT
(2) Yes"""
    )

    cols_choice = input()

    split_by_columns = False # Default
    if cols_choice == "2":
        split_by_columns = True
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.replace(" ", "_")
        print("\nHere is a breakdown of unique values per column in your file:")
        print(df.nunique())
        print("\nProvide a comma-separated list of column headers to split by.")
        columns_to_split = input()

    print(
        """
Would you like to limit file size by number of rows?
(1) No - DEFAULT
(2) Yes
"""
    )

    size_choice = input()

    split_by_size = False # Default
    group_size = 0 # Default. group_size <= 0 will bypass split_by_size.
    if size_choice == "2":
        split_by_size = True
        print("\nMax rows per file?")
        group_size = int(input())

    print("Splitting file. Please wait...")

    # split_by_columns will direct output to split_by_size if group_size > 0
    if split_by_columns:
        df_dict = chop_by_columns(df, columns_to_split, group_size)
    elif split_by_size:
        df_dict = chop_by_size(df, group_size)
    else:
        raise RuntimeError("You must split by colums and/or row count.")

    print(f"\nCreated {len(df_dict)} files.")

    print(
        """
Files will be named like:
'{prefix_}{unique_column_value}{_file_number_if_split_by_size}.csv'.

Example: 'CoolPeopleWhoLiveIn_Somerville_MA_1.csv'.

What would you like to use as a filename prefix?"""
    )

    prefix = input()

    print(
        """
Where would you like to output these files?
Will create a new directory if the specified path does not exist"""
    )
    destination = input()
    if not destination.endswith("/"):
        destination += "/"

    Path(destination).mkdir(parents=True, exist_ok=True)

    df_list = df_dict.items()

    # Save all dfs as CSVs in the destination folder.
    with mp.Pool() as pool:
        pool.map(partial(save_file, destination=destination, prefix=prefix), df_list)

    print("All done!")


if __name__ == "__main__":
    main()
