import pyarrow.dataset as ds
import duckdb
import json
def Query(request):
    dt = ds.dataset("gs://pathtoyourtable/",format="parquet", partitioning="hive")
    con = duckdb.connect()
    SQL = request.get_json().get('name')
    df = con.execute(SQL).df()
     
    return json.dumps(df.to_json(orient="split")), 200, {'Content-Type': 'application/json'}
