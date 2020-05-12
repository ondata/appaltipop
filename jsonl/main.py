import sys
import logging
from pathlib import Path
import json
import jsonlines

if __name__ == "__main__":

    if len(sys.argv) == 1:
        logging.error("Usage: {} /path/to/json/file".format(sys.argv[0]))
        exit(1)

    json_path = Path(sys.argv[1])
    jsonl_path = Path("./", json_path.name).with_suffix(".jsonl")

    with open(json_path) as f:
        l = json.load(f)

    with jsonlines.open(jsonl_path, mode='w') as writer:
        writer.write_all(l)
