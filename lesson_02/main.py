import json
import os
import shutil
from typing import Tuple
import requests
from flask import Flask, request


HOST = '127.0.0.1'
PORT = 8081
REQUEST_TIMEOUT_SECONDS = 10
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
app = Flask(__name__)

@app.route("/", methods=['POST'])
def load_sales() -> Tuple[str, int]:
    '''
        loads sales from https://fake-api-vycpfa6oca-uc.a.run.app/sales and saves them to raw_dir
        input json example: 
            json_obj={
                "date": "2022-08-09",
                "raw_dir": /path/to/my_dir/raw/sales/2022-08-09
            }
    '''
    params = request.json
    date, raw_dir = params['date'], params['raw_dir']
    page = 1
    records = []
    while True:
        # request current page
        response = requests.get(
            url='https://fake-api-vycpfa6oca-uc.a.run.app/sales',
            params={'date': date, 'page': page},
            headers={'Authorization': AUTH_TOKEN},
            timeout=REQUEST_TIMEOUT_SECONDS
        )
        # break if there are no more records for current date
        if response.status_code == 404:
            break

        # append current batch
        response_json = json.loads(response.text)
        records += response_json
        page += 1

    # ensure idempotency
    if os.path.isdir(raw_dir):
        shutil.rmtree(raw_dir)
    os.makedirs(raw_dir)

    # save data
    with open(os.path.join(raw_dir, f'sales_{date}.json'), 'w', encoding='utf-8') as f:
        f.write(json.dumps(records))
    return f'''Saved {len(records)} records''', 201


if __name__ == "__main__":
    app.run(host=HOST, port=PORT)
