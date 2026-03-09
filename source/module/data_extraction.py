import requests
import pandas as pd
import time
from module.api_keys import api_keys
import os
from pathlib import Path
import logging


# create logger for this module
module_logger = logging.getLogger(__name__)

# Retreieve relevant API key
api_key = api_keys['data.gov.sg']

VERIFY = True  # False only for testing!

# https://guide.data.gov.sg/developer-guide/dataset-apis/download-dataset
META_URL_TPL = "https://api-production.data.gov.sg/v2/public/api/collections/{}/metadata"
INIT_URL_TPL = "https://api-open.data.gov.sg/v1/public/api/datasets/{}/initiate-download"
POLL_URL_TPL = "https://api-open.data.gov.sg/v1/public/api/datasets/{}/poll-download"

import os
import time
import json
import requests
import pandas as pd
from pathlib import Path

def download_child_datasets_by_collection_id(collection_id, api_key, output_dir):
    headers = {
        'X-API-KEY': api_key 
    }

    max_retries = 10

    # Ensure directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Access collection_id metadata to get childDatasets for each dataset
    response = requests.get(
        f'https://api-production.data.gov.sg/v2/public/api/collections/{collection_id}/metadata',
        headers=headers,
        timeout=30
    )

    if response.status_code != 200:
        module_logger.error(f"Error: {response.status_code}")
        return None

    data = response.json()
    child_datasets = data['data']['collectionMetadata']['childDatasets']
    module_logger.info(f'Datasets found: {len(child_datasets)} in collection_id {collection_id}')

    # For each childDataset, download the dataset
    for dataset_id in child_datasets:
        module_logger.info(f"Downloading {dataset_id}")

        all_rows = []
        offset = 0
        retry_count = 0

        while True:
            try:
                resp = requests.get(
                    'https://data.gov.sg/api/action/datastore_search',
                    params={'resource_id': dataset_id, 'limit': 10000, 'offset': offset},
                    headers=headers,
                    timeout=30
                )

                # Retry if hit 429 limit 
                if resp.status_code == 429:
    
                    retry_count += 1
                    if retry_count > max_retries:
                        module_logger.error(f"Max retries exceeded for {dataset_id}")
                        break

                    wait_time = 10
                    module_logger.info(f"Limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    
                    continue  # Retry same page

                resp.raise_for_status()
                result = resp.json().get('result', {})
                rows = result.get('records', [])

                if rows:
                    all_rows.extend(rows)
                    module_logger.info(f"Downloaded {len(rows)} rows (total: {len(all_rows)})")

                    # Next page or stop if last page
                    if len(rows) < 10000:
                        break
                    offset += 10000
                    retry_count = 0 
                    time.sleep(2) 
                else:
                    module_logger.info("No more records found")
                    break

            except requests.exceptions.RequestException as e:
                module_logger.error(f"Error: {e}")
                retry_count += 1
                if retry_count > max_retries:
                    module_logger.error("Max retries exceeded")
                    break
                time.sleep(5 * retry_count)

        # save to csv to output_dir
        if all_rows:
            df = pd.DataFrame(all_rows)
            csv_path = output_dir / f"{dataset_id}.csv"
            # index=False to avoid an extra index column
            df.to_csv(csv_path, index=False)
            module_logger.info(f"Saved as CSV: {csv_path} ({len(df)} rows)")
        else:
            module_logger.info("No data to save for this dataset.")

        time.sleep(15)
    