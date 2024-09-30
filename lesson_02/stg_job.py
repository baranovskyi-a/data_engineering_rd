import json
import os
import shutil
from typing import Tuple
import fastavro
from flask import Flask, request


PORT = 8082
app = Flask(__name__)


@app.route("/", methods=['POST'])
def write_to_stg() -> Tuple[str, int]:
    '''
        gets json files from raw_dir and saves them to stg_dir as avro
        input json example: 
            json_obj={
                "raw_dir": /path/to/my_dir/raw/sales/2022-08-09,
                "stg_dir": /path/to/my_dir/stg/sales/2022-08-09,
            }
    '''
    params = request.json
    raw_dir, stg_dir = params['raw_dir'], params['stg_dir']
    # ensure idempotency
    if os.path.isdir(stg_dir):
        shutil.rmtree(stg_dir)
    os.makedirs(stg_dir)

    # avro schema
    schema = {
        "type": "record",
        "name": "Sale",
        "fields": [
            {"name": "client", "type": "string"},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"},
        ]
    }

    # get json files list to process
    files = os.listdir(raw_dir)
    files = [f for f in files if f.endswith('.json')]

    # convert files to avro and save to stg
    for filename in files:
        raw_path = os.path.join(raw_dir, filename)
        stg_path = os.path.join(stg_dir, filename[:-5] + '.avro')
        # read raw data
        with open(raw_path, 'r', encoding='utf-8') as raw_file:
            data = json.load(raw_file)
        # write stg data
        with open(stg_path, 'wb') as stg_file:
            fastavro.writer(stg_file, schema, data)
    return f'''converted files: {str(files)}''', 201


if __name__ == "__main__":
    app.run(port=PORT)
