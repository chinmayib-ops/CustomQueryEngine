import json
import os
from collections import defaultdict
from multiprocessing import Pool, cpu_count

def read_json_lines(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            yield line.strip()

def parse_json(line):
    return json.loads(line)

def read_and_parse_json(file_path):
    for line in read_json_lines(file_path):
        parsed_object = parse_json(line)
        yield parsed_object

def process_query2_file(file_path):
    from collections import defaultdict
    results = defaultdict(lambda: {"num_trips": 0, "total_fare": 0, "total_tip": 0})
    for row in read_and_parse_json(file_path):
        if row.get("trip_distance", 0) > 5:
            payment_type = row.get("payment_type", "Unknown")
            results[payment_type]["num_trips"] += 1
            results[payment_type]["total_fare"] += row.get("fare_amount", 0)
            results[payment_type]["total_tip"] += row.get("tip_amount", 0)
    return dict(results)

def merge_query2_results(results_list):
    merged = {}
    for res in results_list:
        for k, v in res.items():
            if k not in merged:
                merged[k] = v
            else:
                merged[k]["num_trips"] += v["num_trips"]
                merged[k]["total_fare"] += v["total_fare"]
                merged[k]["total_tip"] += v["total_tip"]
    return merged

def process_query3_file(file_path):
    from collections import defaultdict
    results = defaultdict(lambda: {"trips": 0, "total_passengers": 0})
    for row in read_and_parse_json(file_path):
        if row.get("store_and_fwd_flag") == 'Y':
            pickup_date = row.get("tpep_pickup_datetime", '')[:10]
            if '2024-01-01' <= pickup_date < '2024-02-01':
                vendor_id = row.get("VendorID")
                passenger_count = int(row.get("passenger_count") or 0)
                results[vendor_id]["trips"] += 1
                results[vendor_id]["total_passengers"] += passenger_count
    return dict(results)

def merge_query3_results(results_list):
    merged = {}
    for res in results_list:
        for k, v in res.items():
            if k not in merged:
                merged[k] = v
            else:
                merged[k]["trips"] += v["trips"]
                merged[k]["total_passengers"] += v["total_passengers"]
    return merged

def process_query4_file(file_path):
    from collections import defaultdict
    results = defaultdict(lambda: {
        "num_trips": 0,
        "total_passengers": 0,
        "total_distance": 0,
        "total_fare": 0,
        "total_tip": 0
    })
    for row in read_and_parse_json(file_path):
        pickup_date = row.get("tpep_pickup_datetime", '')[:10]
        if '2024-01-01' <= pickup_date < '2024-02-01':
            results[pickup_date]["num_trips"] += 1
            results[pickup_date]["total_passengers"] += row.get("passenger_count") or 0
            results[pickup_date]["total_distance"] += row.get("trip_distance") or 0
            results[pickup_date]["total_fare"] += row.get("fare_amount") or 0
            results[pickup_date]["total_tip"] += row.get("tip_amount") or 0
    return dict(results)

def merge_query4_results(results_list):
    merged = {}
    for res in results_list:
        for date, stats in res.items():
            if date not in merged:
                merged[date] = stats
            else:
                merged[date]["num_trips"] += stats["num_trips"]
                merged[date]["total_passengers"] += stats["total_passengers"]
                merged[date]["total_distance"] += stats["total_distance"]
                merged[date]["total_fare"] += stats["total_fare"]
                merged[date]["total_tip"] += stats["total_tip"]
    return merged

class TaxiQueryEngine:
    def __init__(self, data_dir):
        self.data_dir = data_dir
    
    def execute_query(self, query_name):
        if query_name == "query1":
            return self.query1()
        elif query_name == "query2":
            return self.query2()
        elif query_name == "query3":
            return self.query3()
        elif query_name == "query4":
            return self.query4()
        else:
            raise ValueError("Invalid query name")

    def query1(self):
        file_paths = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.json')]
        with Pool(cpu_count()) as pool:
            counts = pool.map(self.count_records, file_paths)
        total_count = sum(counts)
        return {"total_trips": total_count}

    def count_records(self, file_path):
        return sum(1 for _ in read_and_parse_json(file_path))

    def query2(self):
        file_paths = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.json')]
        with Pool(cpu_count()) as pool:
            results_list = pool.map(process_query2_file, file_paths)
        merged_results = merge_query2_results(results_list)
        for payment_type, stats in merged_results.items():
            stats["avg_fare"] = stats["total_fare"] / stats["num_trips"] if stats["num_trips"] else 0
            del stats["total_fare"]
        self.write_output("query2", merged_results)
        return merged_results

    def query3(self):
        file_paths = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.json')]
        with Pool(cpu_count()) as pool:
            results_list = pool.map(process_query3_file, file_paths)
        merged_results = merge_query3_results(results_list)
        for vendor_id, stats in merged_results.items():
            stats["avg_passengers"] = stats["total_passengers"] / stats["trips"] if stats["trips"] else 0
        self.write_output("query3", merged_results)
        return merged_results

    def query4(self):
        file_paths = [os.path.join(self.data_dir, f) for f in os.listdir(self.data_dir) if f.endswith('.json')]
        with Pool(cpu_count()) as pool:
            results_list = pool.map(process_query4_file, file_paths)
        merged_results = merge_query4_results(results_list)
        for date, stats in merged_results.items():
            stats["avg_passengers"] = stats["total_passengers"] / stats["num_trips"] if stats["num_trips"] else 0
            stats["avg_distance"] = stats["total_distance"] / stats["num_trips"] if stats["num_trips"] else 0
            stats["avg_fare"] = stats["total_fare"] / stats["num_trips"] if stats["num_trips"] else 0
        self.write_output("query4", merged_results)
        return merged_results

    def write_output(self, query_name, output_data):
        output_str = f"Results for {query_name}:\n"
        for key, value in output_data.items():
            output_str += f"{key}: {value}\n"
        with open(f"{query_name}_output.txt", "w") as file:
            file.write(output_str)
        print(output_str)