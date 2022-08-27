import pyarrow.dataset as ds
import duckdb
import json
lineitem = ds.dataset("gs://duckddelta/delta2",format="parquet", partitioning="hive")
con = duckdb.connect()
def Query(request):
    SQL = request.get_json().get('name')
    df = con.execute(SQL).df()
    return json.dumps(df.to_json(orient="records")), 200, {'Content-Type': 'application/json'}
