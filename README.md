# Rust-Polars-benchmark-vs-Python-Pandas
Rust Polars benchmarking with Python Pandas

This Rust command-line tool, reads a dataframe saved as Parquet file, do some transformations and record the processing time for reading/processing and writing the transformation result also as Parquet file.

I used this tool to benchmark and make a case for using Rust and Polars for data wrangling instead of Python and Pandas.

The Rust Polars solution proved to be **10x faster** for data processing than Python/Pandas. The writing to disk durations were similar. The results were:

**Rust/Polars:**
- Processing duration: **4.36 seconds**
- Transformed data writing on disk duration: **3.16 seconds**

**Python/Pandas:**
- Processing duration: **47.28 seconds**

- Transformed data writing on disk duration: **4.57 seconds**

The data is processed in parallel with all CPU's using lazy API:
![Screenshot lazy API](https://github.com/crbl1122/Rust-Polars-benchmark-vs-Python-Pandas/assets/30111494/a87e0706-7270-484f-9086-9034d5e11120)

About the source parquet file I used for benchmarking: It contains a dataframe generated in Python/Pandas with 	**10.000.000 rows** and three columns: one string column "random_letters" which has **1.000.000 unique combinations of 5 letters** and two random generated float columns. I saved the dataframe as Parquet file and I used Rust to read and process it. The file size is 234Mb.

For benchmarking I created a function in Rust which does the following transformation steps:
- Aggregate each numerical column using "random_letters" column and generate two Polars dataframes, one for each aggregated column.
- Create the final Polars dataframe by joining the two aggregated dataframes using "random_letters" as key.
###
Sample of source dataframe with 10.000.000 rows and 1.000.000 unique combinations of letters:

  random_letters	"0"	"1"
  --------------  --- ---
0	NGOXW	0.496714	-0.138264

1	OVSPI	0.647689	1.523030

2	ZNUQT	-0.234153	-0.234137
