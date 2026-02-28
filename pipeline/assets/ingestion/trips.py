"""@bruin
name: ingestion.trips
type: python
connection: duckdb-default

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_datetime
    type: string  # <--- 强制声明为 string，避开 pyarrow 的时区校验
  - name: dropoff_datetime
    type: string
  - name: vendor_id
    type: integer
  - name: taxi_type
    type: string
@bruin"""

import pandas as pd

def materialize():
    # 构造极简数据，全部手动转为字符串
    data = {
        "vendor_id": [1, 2],
        "pickup_datetime": ["2026-02-27 10:00:00", "2026-02-27 11:00:00"],
        "dropoff_datetime": ["2026-02-27 10:30:00", "2026-02-27 11:30:00"],
        "passenger_count": [1, 2],
        "trip_distance": [2.5, 5.0],
        "taxi_type": ["yellow", "yellow"],
        "payment_type": [1, 2], 
        "pickup_location_id": [100, 101],
        "dropoff_location_id": [200, 201],
        "fare_amount": [15.5, 20.0]
    }
    
    df = pd.DataFrame(data)
    # 确保时间列是普通的字符串对象
    df['pickup_datetime'] = df['pickup_datetime'].astype(str)
    df['dropoff_datetime'] = df['dropoff_datetime'].astype(str)
    
    return df