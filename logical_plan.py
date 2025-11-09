# logical_plan.py
import re
from typing import List, Tuple, Union, Dict, Any, Optional

# Type Aliases
FieldName = str
FilterCondition = Tuple[str, FieldName, Union[int, str]]
ProjectionField = Union[FieldName, Tuple[str, FieldName, int]] # str or ('+', 'col', 100)
NodeArgs = Dict[str, Any]

# --- Core Data Structures ---

class SQLStatement:
    """Represents the Abstract Syntax Tree (AST) after parsing."""
    def __init__(self, select_fields: List[ProjectionField], table_name: str, filters: List[FilterCondition], limit: Optional[int]):
        self.select_fields = select_fields
        self.table_name = table_name
        self.filters = filters
        self.limit = limit

    def __repr__(self) -> str:
        return f"Statement(SELECT={self.select_fields}, FROM={self.table_name}, WHERE={self.filters}, LIMIT={self.limit})"

class LogicalPlan:
    """Represents the required relational algebra operations (WHAT to do)."""
    def __init__(self, operation: str, child: Optional['LogicalPlan'] = None, **kwargs: Any):
        self.operation = operation # e.g., 'Scan', 'Filter', 'Project', 'Limit'
        self.child = child         # Child plan (forms the tree)
        self.kwargs = kwargs       # Operation-specific arguments
    
    def format_tree(self, indent: int = 0) -> str:
        """Formats the plan tree for console output/JSON transfer."""
        s = '  ' * indent + f"[LPlan: {self.operation}] {self.kwargs}"
        if self.child:
            s += '\n' + self.child.format_tree(indent + 1)
        return s

# --- Planning Logic ---

def parse_sql(sql_query: str) -> SQLStatement:
    """
    (MOCK) Parses a specific structure of SQL into a structured object.
    Supports: SELECT id, age + 100 FROM t1 WHERE id < 6 LIMIT 3
    """
    query = sql_query.upper()
    
    # 1. Select Fields
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query)
    if not select_match:
        raise ValueError("Invalid SQL: SELECT clause missing.")
        
    select_fields = []
    for f in select_match.group(1).split(','):
        f = f.strip()
        if re.search(r'\s*\+\s*\d+', f):
            # Handle arithmetic projection: age + 100
            parts = [p.strip() for p in re.split(r' \+ ', f)]
            if len(parts) == 2 and parts[0].isalpha() and parts[1].isdigit():
                select_fields.append(('+', parts[0].lower(), int(parts[1])))
            else:
                 select_fields.append(f)
        else:
            select_fields.append(f.lower())

    # 2. Table Name
    from_match = re.search(r'FROM\s+(\w+)', query)
    table_name = from_match.group(1).strip().lower() if from_match else "unknown"

    # 3. Filters
    filters: List[FilterCondition] = []
    where_match = re.search(r'WHERE\s+(.*?)(?:\s+LIMIT|\s*$)', query)
    if where_match:
        # Simple filter: col < 6
        parts = re.split(r'\s*([<=>!]{1,2})\s*', where_match.group(1).strip())
        if len(parts) == 3:
            col, op, val_str = parts
            col = col.lower()
            if val_str.isdigit():
                filters.append((op, col, int(val_str)))
            else:
                filters.append((op, col, val_str.replace("'", ""))) # Strip quotes for string values

    # 4. Limit
    limit_match = re.search(r'LIMIT\s+(\d+)', query)
    limit = int(limit_match.group(1)) if limit_match else None

    return SQLStatement(select_fields, table_name, filters, limit)
        
def generate_logical_plan(statement: SQLStatement) -> LogicalPlan:
    """Converts the parsed statement into a tree of logical operators."""
    
    plan = LogicalPlan('Scan', table=statement.table_name)

    if statement.filters:
        plan = LogicalPlan('Filter', child=plan, condition=statement.filters[0])

    plan = LogicalPlan('Project', child=plan, fields=statement.select_fields)

    if statement.limit is not None:
        plan = LogicalPlan('Limit', child=plan, count=statement.limit)

    return plan
