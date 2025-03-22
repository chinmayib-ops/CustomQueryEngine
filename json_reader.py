import json

def split_json(file_path, output_dir, chunk_size=1000000):
    with open(file_path, 'r') as file:
        chunk = []
        chunk_index = 0
        for line in file:
            chunk.append(line.strip())
            if len(chunk) >= chunk_size:
                with open(f"{output_dir}/chunk_{chunk_index}.json", 'w') as chunk_file:
                    chunk_file.write("\n".join(chunk))
                chunk = []
                chunk_index += 1
        if chunk:
            with open(f"{output_dir}/chunk_{chunk_index}.json", 'w') as chunk_file:
                chunk_file.write("\n".join(chunk))

if __name__ == "__main__":
    file_path = '/home/yapper/Downloads/taxi-trips-data.json'
    output_dir = '/home/yapper/Downloads/hackathon/chunks'
    split_json(file_path, output_dir)