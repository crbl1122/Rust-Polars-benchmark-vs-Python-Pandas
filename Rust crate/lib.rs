// utilities for working with polars dataframes
//
use polars::prelude::*;
use std::fs::File;

//read in a parquet file
pub fn read_parquet(path: &str) -> DataFrame {
    // Open file
    let file = File::open(path).expect("Failed to open file");

    // Read to DataFrame and Return
    ParquetReader::new(file)
        .finish()
        .expect("Failed to read Parquet file")
}

// write dataframe to parquet file
pub fn write_parquet(df: &mut DataFrame, path: &str) {
    // create a file
    let file = File::create(path)
            .expect("could not create output file");

    // write dataframe to output parquet
    ParquetWriter::new(file)
        .finish(df)
        .expect("Failed to write dataframe.");

}

//print "n" rows of a dataframe
pub fn print_df(df: &DataFrame, n: usize) {
    println!("{:?}", df.head(Some(n)));
}

//print the schema of a dataframe
pub fn print_schema(df: &DataFrame) {
    println!("{:?}", df.schema());
}

//print the shape of a dataframe
pub fn print_shape(df: &DataFrame) {
    println!("{:?}", df.shape());
}

pub fn do_some_processing(path: &str) -> PolarsResult<DataFrame> {
    let lf1 = LazyFrame::scan_parquet(path, Default::default())?
    .group_by([col("random_letter")])
    .agg([
        // Replace rank with a sort expression
        col("0")
            .sort(false)
            .last()
            .alias("last_col_0_sorted_by_random_letter"),
        // Replace cum_min with min
        col("0").min().alias("min_per_group"),
        // every expression runs in parallel
        col("0").reverse().implode().alias("reverse_group"),
    ]);

    let lf2 = LazyFrame::scan_parquet(path, Default::default())?
    .group_by([col("random_letter")])
    .agg([
        // Replace rank with a sort expression
        col("1")
            .sort(false)
            .last()
            .alias("last_col_1_sorted_by_random_letter"),
        // Replace cum_min with min
        col("1").min().alias("min_per_group"),
        // every expression runs in parallel
        col("1").reverse().implode().alias("reverse_group"),
    ]);


    let df = lf1
        .join(lf2, [col("random_letter")], [col("random_letter")], JoinArgs::new(JoinType::Left))
        // now we finally materialize the result.
        .collect()?;

Ok(df)

}
