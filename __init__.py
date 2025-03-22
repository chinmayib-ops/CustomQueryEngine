```

### [main.py](http://_vscodecontentref_/1)
This is the main entry point for the program.

```python
<vscode_codeblock_uri>file:///home/yapper/Downloads/hackathon/src/main.py</vscode_codeblock_uri>import sys
from src.query_engine import run_query

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 main.py <query_name> <file_path>")
        sys.exit(1)

    query_name = sys.argv[1]
    file_path = sys.argv[2]

    result = run_query(query_name, file_path)
    print(result)