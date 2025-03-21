# Query Engine

## How to Use

1. Place your JSON file in the appropriate location.
2. Modify the `FILE_PATH` variable in `run.sh` to point to your JSON file.
3. Run the script with the desired query name:

```bash
./run.sh query1
./run.sh query2
./run.sh query3
./run.sh query4# CustomQueryEngine




The solution builds a custom query engine to process large datasets (e.g., 20 million taxi trip records). It uses the `split_json` function to divide large JSON files into smaller chunks, which reduces memory usage and enhances performance. 

The `TaxiQueryEngine` class handles different queries, including counting trips, filtering by distance, and aggregating data by payment type, vendor, and date. Parallel processing with `multiprocessing.Pool` speeds up the execution by utilizing multiple CPU cores, and results from different chunks are merged for accurate statistics.

Results are formatted in `main.py` for user-friendly output. Key design decisions include chunking for memory efficiency, parallel processing for faster execution, and a modular structure for maintainability. This solution efficiently handles large datasets without external libraries like Pandas or Spark.
