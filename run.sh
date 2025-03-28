#!/bin/bash

OUTPUT_DIR="results"
SUMMARY_FILE="$OUTPUT_DIR/summary.txt"

# Create results directory if it doesn't exist
mkdir -p "$OUTPUT_DIR" 

# Initialize summary file with timestamp (overwriting any previous summary)
TIMESTAMP=$(date +"%H:%M:%S")
echo "QUERY ENGINE EXECUTION SUMMARY" > "$SUMMARY_FILE" 
echo "=============================" >> "$SUMMARY_FILE"
echo "Timestamp: $TIMESTAMP" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

# Function to run a query and capture results
run_query() {
    QUERY=$1
    OUTPUT_FILE="$OUTPUT_DIR/${QUERY}.txt"
    ERROR_FILE="$OUTPUT_DIR/${QUERY}_error.log"
    
    # Measure execution time
    START_TIME=$(date +%s.%N)
    
    # Set PYTHONPATH to include the src directory
    PYTHONPATH=$(dirname "$0")/src
    
    # Execute the query and capture both stdout and stderr (overwriting previous files)
    PYTHONPATH=$PYTHONPATH python3 src/main.py "$QUERY" "/home/yapper/Downloads/hackathon/chunks" > "$OUTPUT_FILE" 2>"$ERROR_FILE"
    EXIT_CODE=$?
    
    END_TIME=$(date +%s.%N)
    EXECUTION_TIME=$(echo "$END_TIME - $START_TIME" | bc | awk '{printf "%.2f", $0}')
    
    # Check if the query succeeded
    if [ "$EXIT_CODE" -eq 0 ] && [ -s "$OUTPUT_FILE" ]; then
        STATUS="SUCCESS"
    else
        STATUS="FAILED"
    fi
    
    # Add detailed result to summary file
    echo "[$QUERY]" >> "$SUMMARY_FILE"
    echo "Status: $STATUS" >> "$SUMMARY_FILE"
    echo "Execution time: ${EXECUTION_TIME}s" >> "$SUMMARY_FILE"
    
    if [ "$STATUS" == "FAILED" ]; then
        if [ -f "$ERROR_FILE" ]; then
            ERROR_LOG=$(cat "$ERROR_FILE")
            echo "Error details: $ERROR_LOG" >> "$SUMMARY_FILE"
            echo "Error details: $ERROR_LOG"
        else:
            echo "Error details: No error log found." >> "$SUMMARY_FILE"
            echo "Error details: No error log found."
        fi
    fi
    echo "" >> "$SUMMARY_FILE"
    
    # Print minimal info to terminal
    if [ "$STATUS" == "SUCCESS" ]; then
        echo "$QUERY: ${EXECUTION_TIME}s - $STATUS"
    else
        echo "$QUERY: ${EXECUTION_TIME}s - $STATUS (see $ERROR_FILE)"
    fi
}

# Check if a specific query was requested
if [ "$#" -ne 1 ]; then
    echo "Usage: ./run.sh <query_name>"
    exit 1
fi

QUERY_NAME=$1

if [[ "$QUERY_NAME" =~ ^query[1-4]$ ]]; then
    run_query "$QUERY_NAME"
    exit 0
else
    echo "Error: Invalid query parameter '$QUERY_NAME'. Use query1, query2, query3, or query4."
    exit 1
fi

# If no parameter provided, run all queries
echo "Running all queries..."

# Run all queries
run_query "query1"
run_query "query2"
run_query "query3"
run_query "query4"

# Add final stats to summary file
echo "OVERALL STATS:" >> "$SUMMARY_FILE"
echo "-------------" >> "$SUMMARY_FILE"
SUCCESS_COUNT=$(grep -c "Status: SUCCESS" "$SUMMARY_FILE")
FAILED_COUNT=$(grep -c "Status: FAILED" "$SUMMARY_FILE")
echo "Total: 4, Success: $SUCCESS_COUNT, Failed: $FAILED_COUNT" >> "$SUMMARY_FILE"

# Print minimal summary to terminal
echo "----------------------------------------"
echo "All done. Summary saved to $SUMMARY_FILE"
echo "Total: 4, Success: $SUCCESS_COUNT, Failed: $FAILED_COUNT"