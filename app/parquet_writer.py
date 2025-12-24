import os
import time
import pyarrow as pa
import pyarrow.parquet as pq

def write_parquet(events, base_dir):
    if not events:
        return None

    os.makedirs(base_dir, exist_ok=True)
    timestamp = int(time.time())
    filename = f"moves_{timestamp}.parquet"
    path = os.path.join(base_dir, filename)

    table = pa.Table.from_pylist(events)
    pq.write_table(table, path)

    return path
