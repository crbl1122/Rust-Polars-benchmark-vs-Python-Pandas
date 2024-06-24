//command-line tool that reads a PARQUET file and prints the contents of the file as a DataFrame
use clap::Parser;
use std::time::Instant;

const PARQUET_FILE: &str = "/path/to/large_dataframe_with_nans.parquet";
const OUTPUT_PARQUET_FILE: &str = "path/to/transformed_large_dataframe.parquet";

#[derive(Parser)]
//add extended help
#[clap(
    version = "1.0",
    author = "",
    about = "This is a modification of an example command-line tool found here https://github.com/noahgift. 
            It reads a PAQUET file in Polars, do some processing, record the duration of the steps and save the contents of the transformed 
            file as a DataFrame in parquet format. I used it to benchmark against Python/ Pandas processing",
    after_help = "Example: cargo run -- print --rows 3"
)]
struct Cli {
    #[clap(subcommand)]
    command: Option<Commands>,
}

#[derive(Parser)]
enum Commands {
    Print {
        #[clap(long, default_value = PARQUET_FILE)]
        path: String,
        #[clap(long, default_value = "10")]
        rows: usize,
    },
    Describe {
        #[clap(long, default_value = PARQUET_FILE)]
        path: String,
    },
    Schema {
        #[clap(long, default_value = PARQUET_FILE)]
        path: String,
    },
    Shape {
        #[clap(long, default_value = PARQUET_FILE)]
        path: String,
    },

    Transform {
        #[clap(long, default_value = PARQUET_FILE)]
        input_path: String,
        #[clap(long, default_value = OUTPUT_PARQUET_FILE)]
        output_path:String,
    }

}

fn main() {
    let args = Cli::parse();
    match args.command {
        Some(Commands::Print { path, rows }) => {
            let df = polars2::read_parquet(&path);
            println!("{:?}", df.head(Some(rows)));
        }
        Some(Commands::Describe { path }) => {
            let df = polars2::read_parquet(&path);
            println!("{:?}", df);
        }
        Some(Commands::Schema { path }) => {
            let df = polars2::read_parquet(&path);
            println!("{:?}", df.schema());
        }
        Some(Commands::Shape { path }) => {
            let df = polars2::read_parquet(&path);
            println!("{:?}", df.shape());
        }
        Some(Commands::Transform { input_path, output_path }) => {

            // Start measuring time
            let start = Instant::now();
            
            let mut transformed_df = polars2::do_some_processing(&input_path).expect("Error do some processing");

            // End measuring time
            let duration = start.elapsed();
            println!("Processing duration: {:.2} seconds", duration.as_secs_f64());


            // Start measuring time
            let start = Instant::now();

            polars2::write_parquet(&mut transformed_df, &output_path);

            // End measuring time
            let duration = start.elapsed();
            println!("Total data writing duration: {:.2} seconds", duration.as_secs_f64());

        }

        None => {
            println!("No subcommand was used. Ex: cargo run --print --rows 3");


        }
    }
}
