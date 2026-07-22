
import json
def load_airports(file_path):
    with open(file_path, 'r') as f:
        airports = json.load(f)
    return airports