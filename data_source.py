# data_source.py

from typing import List, Dict, Any, Union

# Type Aliases
Record = Dict[str, Union[int, str, float]]
Table = List[Record]
DataCatalog = Dict[str, Table]

def parse_data_to_catalog(csv_data: str, table_name: str) -> DataCatalog:
    """Converts CSV-like string data into a DataCatalog (Map) of records."""
    lines = csv_data.strip().split('\n')
    if len(lines) < 1:
        return {}

    # Headers are the first line
    headers = [h.strip().lower() for h in lines[0].split(',')]
    data: Table = []

    for i in range(1, len(lines)):
        values = [v.strip() for v in lines[i].split(',')]
        if len(values) != len(headers):
            continue

        row: Record = {}
        for header, value in zip(headers, values):
            # Attempt to parse as an integer
            try:
                row[header] = int(value)
            except ValueError:
                # Attempt to parse as a float
                try:
                    row[header] = float(value)
                except ValueError:
                    # Keep as string
                    row[header] = value
        data.append(row)
        
    return {table_name: data}
