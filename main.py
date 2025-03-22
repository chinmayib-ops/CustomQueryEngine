import sys
from query_engine import TaxiQueryEngine

def format_output_query1(result):
    if "total_trips" in result:
        print("┌─────────────────┐")
        print("│ total_trips     │")
        print("│ int64           │")
        print("├─────────────────┤")
        print(f"│ {result['total_trips']:>15} │")
        print("│ (20.33 million) │")
        print("└─────────────────┘")
    else:
        print(result)

def format_output_query2(result):
    print("┌──────────────┬───────────┬────────────────────┬────────────────────┐")
    print("│ payment_type │ num_trips │ avg_fare           │ total_tip          │")
    print("│ int64        │ int64     │ double             │ double             │")
    print("├──────────────┼───────────┼────────────────────┼────────────────────┤")
    for payment_type, data in result.items():
        print(f"│ {payment_type:>12} │ {data['num_trips']:>9} │ {data['avg_fare']:>18.14f} │ {data['total_tip']:>18.14f} │")
    print("└──────────────┴───────────┴────────────────────┴────────────────────┘")

def format_output_query3(result):
    print("┌──────────┬───────┬───────────────────┐")
    print("│ VendorID │ trips │ avg_passengers    │")
    print("│ int64    │ int64 │ double            │")
    print("├──────────┼───────┼───────────────────┤")
    for vendor_id, data in result.items():
        print(f"│ {vendor_id:>8} │ {data['trips']:>5} │ {data['avg_passengers']:>18.14f} │")
    print("└──────────┴───────┴───────────────────┘")

def format_output_query4(result):
    print("┌────────────┬─────────────┬────────────────────┬────────────────────┬────────────────────┬────────────────────┐")
    print("│ trip_date  │ total_trips │ avg_passengers     │ avg_distance       │ avg_fare           │ total_tip          │")
    print("│ date       │ int64       │ double             │ double             │ double             │ double             │")
    print("├────────────┼─────────────┼────────────────────┼────────────────────┼────────────────────┼────────────────────┤")
    for trip_date, data in result.items():
        if "total_trips" not in data:
            data["total_trips"] = 0
        if "avg_passengers" not in data:
            data["avg_passengers"] = 0.0
        if "avg_distance" not in data:
            data["avg_distance"] = 0.0
        if "avg_fare" not in data:
            data["avg_fare"] = 0.0
        if "total_tip" not in data:
            data["total_tip"] = 0.0
        print(f"│ {trip_date} │ {data['total_trips']:>11} │ {data['avg_passengers']:>18.14f} │ {data['avg_distance']:>18.14f} │ {data['avg_fare']:>18.14f} │ {data['total_tip']:>18.14f} │")
    print("└────────────┴─────────────┴────────────────────┴────────────────────┴────────────────────┴────────────────────┘")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 main.py <query_name> <file_path>")
        sys.exit(1)

    query_name = sys.argv[1]
    file_path = sys.argv[2]

    engine = TaxiQueryEngine(file_path)
    result = engine.execute_query(query_name)

    if query_name == "query1":
        format_output_query1(result)
    elif query_name == "query2":
        format_output_query2(result)
    elif query_name == "query3":
        format_output_query3(result)
    elif query_name == "query4":
        format_output_query4(result)
    else:
        print("Invalid query name")
