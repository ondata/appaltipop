import sys
import logging
from pathlib import Path
import json
import jsonlines

if __name__ == "__main__":

    if len(sys.argv) == 1:
        logging.error("Usage: {} /path/to/json/file".format(sys.argv[0]))
        exit(1)

    if len(sys.argv) == 3:
        jsonl_dir = sys.argv[2]
    else:
        jsonl_dir = "./"

    json_path = Path(sys.argv[1])

    json_files = json_path.glob("*.json") if json_path.is_dir() else [json_path]

    for json_file in json_files:
        if json_file.is_file():
            jsonl_file = Path(jsonl_dir, json_file.name).with_suffix(".jsonl")
            with open(json_file) as f:
                l = [
                    { k: v for k, v in o.items() if v is not None }
                    for o in json.load(f)
                ]
            with jsonlines.open(jsonl_file, mode='w') as writer:
                writer.write_all(l)
        else:
            logging.warning("Failed to open {}, skip".format(json_file))
