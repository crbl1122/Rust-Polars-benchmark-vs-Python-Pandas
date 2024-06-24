import pandas as pd

def do_some_processing(path: str) -> pd.DataFrame:
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(path)

    # Check if the expected columns exist
    if 'random_letter' not in df.columns or '0' not in df.columns:
        raise KeyError("Columns 'random_letter' or '0' not found in the DataFrame.")

    # Group by the 'random_letter' column
    grouped_0 = df.groupby('random_letter')['0'].apply(tuple)

    # Perform the aggregations
    last_col_0_sorted_by_random_letter = grouped_0.map(lambda x: x[-1])
    min_per_group_0 = grouped_0.map(lambda x: min(x))
    reverse_group_0 = grouped_0.map(lambda x: list(reversed(x)))
    

    # Combine the results into a single DataFrame
    result_0 = pd.DataFrame({
        'last_col_0_sorted_by_random_letter': last_col_0_sorted_by_random_letter,
        'min_per_group_0': min_per_group_0,
        'reverse_group_0': reverse_group_0
    }).reset_index()
    
    
    # next columns
    # Group by the 'random_letter' column
    grouped_1 = df.groupby('random_letter')['1'].apply(tuple)

    # Perform the aggregations
    last_col_1_sorted_by_random_letter = grouped_1.map(lambda x: x[-1])
    min_per_group_1 = grouped_1.map(lambda x: min(x))
    reverse_group_1 = grouped_1.map(lambda x: list(reversed(x)))
    

    # Combine the results into a single DataFrame
    result_1 = pd.DataFrame({
        'last_col_1_sorted_by_random_letter': last_col_1_sorted_by_random_letter,
        'min_per_group_1': min_per_group_1,
        'reverse_group_1': reverse_group_1
    }).reset_index()

    #joining
    result = result_0.merge(result_1, left_on='random_letter', right_on='random_letter', how = 'left')
    
    return result

# benchmarking in Python
start_time = time.time()
# py_transformed_df = loaded_df.fillna(0)
py_transformed_df = do_some_processing("/path/to/large_dataframe_with_nans.parquet")
# Record the total duration
processing_duration = time.time() - start_time
print(f"Processing duration: {processing_duration:.2f} seconds")

start_time = time.time()
py_transformed_df.to_parquet('py_transformed_large_dataframe.parquet')
writing_duration = time.time() - start_time
print(f"Writing duration: {writing_duration:.2f} seconds")
print('Shape of py_transformed_df:', py_transformed_df.shape)
