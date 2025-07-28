import os
import pandas as pd
from scripts.transform import clean_and_transform

def test_transform_no_nulls():
    df = pd.DataFrame({
        'sensor_id': ['A']*3,
        'timestamp': ['2022-06-01T00:00:00']*3,
        'reading_type': ['temperature']*3,
        'value': [21.0, 22.0, 23.0],
        'battery_level': [98, 97, 97]
    })
    out = clean_and_transform(df)
    assert not out['value'].isnull().any()
    assert not out['battery_level'].isnull().any()
