import duckdb
import json
import pandas as pd
def download_all_blobs_with_transfer_manager(
    bucket_name, destination_directory="", threads=4
):
    
    from google.cloud.storage import Client, transfer_manager

    storage_client = Client('project_number_xxx')
    bucket = storage_client.bucket(bucket_name)

    blob_names = [blob.name for blob in bucket.list_blobs()]

    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, threads=threads
    )

    for name, result in zip(blob_names, results):
        # The results list is either `None` or an exception for each blob in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to download {} due to exception: {}".format(name, result))
        else:
            print("Downloaded {} to {}.".format(name, destination_directory + name))
download_all_blobs_with_transfer_manager('xxxxxx', "./data", threads=8)


duckdb.query("install httpfs; load httpfs; PRAGMA enable_object_cache ; SET enable_http_metadata_cache=true ")
def Query(request):
    SQL = request.get_json().get('name')
    try :
       df = duckdb.execute(SQL).df()
    except Exception as er:
       df=pd.DataFrame(er)
    return json.dumps(df.to_json(orient="records")), 200, {'Content-Type': 'application/json'}
