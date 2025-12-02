# ============================================================================
# data_source.py - CSV Data Parser and In-Memory Catalog
# ============================================================================

from typing import List, Dict, Any, Union

Record = Dict[str, Union[int, str, float]]
Table = List[Record]
DataCatalog = Dict[str, Table]

def parse_data_to_catalog(csv_data: str, table_name: str) -> DataCatalog:
    """
    Converts CSV-like string data into a DataCatalog of records.
    Handles type inference: int, float, or string.
    """
    lines = [line.strip() for line in csv_data.strip().split('\n') if line.strip()]
    
    if len(lines) < 2:
        raise ValueError("CSV data must contain at least a header row and one data row")

    headers = [h.strip().lower() for h in lines[0].split(',')]
    data: Table = []

    for i in range(1, len(lines)):
        values = [v.strip() for v in lines[i].split(',')]
        
        if len(values) != len(headers):
            print(f"Warning: Row {i} has {len(values)} columns, expected {len(headers)}. Skipping.")
            continue

        row: Record = {}
        for header, value in zip(headers, values):
            if not value:  # Empty cell
                row[header] = None
                continue
                
            # Type inference
            try:
                row[header] = int(value)
            except ValueError:
                try:
                    row[header] = float(value)
                except ValueError:
                    row[header] = value
        data.append(row)
        
    if not data:
        raise ValueError("No valid data rows found in CSV")
        
    return {table_name: data}
