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

    if json_path.is_dir():
        for json_file in json_path.glob("*.json"):
            if json_file.is_file():
                jsonl_file = Path(jsonl_dir, json_file.name).with_suffix(".jsonl")
                with open(json_file) as f:
                    l = json.load(f)
                with jsonlines.open(jsonl_file, mode='w') as writer:
                    writer.write_all(l)
            else:
                logging.warning("Failed to open {}, skip".format(json_file))
    elif json_path.is_file():
        jsonl_file = Path(jsonl_dir, json_path.name).with_suffix(".jsonl")
        with open(json_path) as f:
            l = json.load(f)
        with jsonlines.open(jsonl_file, mode='w') as writer:
            writer.write_all(l)
    else:
        logging.error("Failed to open {}".format(json_path))
