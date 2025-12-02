# ============================================================================
# logical_plan.py - SQL Parser and Logical Plan Generator
# ============================================================================

import re
from typing import List, Tuple, Union, Dict, Any, Optional

FieldName = str
FilterCondition = Tuple[str, FieldName, Union[int, str, float]]
ProjectionField = Union[FieldName, Tuple[str, FieldName, Union[int, float]]]

class SQLStatement:
    """Represents the Abstract Syntax Tree (AST) after parsing."""
    def __init__(self, select_fields: List[ProjectionField], table_name: str, 
                 filters: List[FilterCondition], limit: Optional[int]):
        self.select_fields = select_fields
        self.table_name = table_name
        self.filters = filters
        self.limit = limit

    def __repr__(self) -> str:
        return (f"SQLStatement(SELECT={self.select_fields}, FROM={self.table_name}, "
                f"WHERE={self.filters}, LIMIT={self.limit})")

class LogicalPlan:
    """Represents the required relational algebra operations (WHAT to do)."""
    def __init__(self, operation: str, child: Optional['LogicalPlan'] = None, **kwargs: Any):
        self.operation = operation
        self.child = child
        self.kwargs = kwargs
    
    def format_tree(self, indent: int = 0) -> str:
        """Formats the plan tree for display."""
        args_str = ', '.join(f"{k}={repr(v)}" for k, v in self.kwargs.items())
        s = '  ' * indent + f"[{self.operation}] {args_str}"
        if self.child:
            s += '\n' + self.child.format_tree(indent + 1)
        return s

def parse_sql(sql_query: str) -> SQLStatement:
    """
    Parses a restricted SQL syntax into a structured statement.
    Supports: SELECT col1, col2+N FROM table WHERE col op value LIMIT n
    """
    query = sql_query.upper()
    original_query = sql_query  # Keep for case-sensitive extraction
    
    # 1. Extract SELECT fields
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE)
    if not select_match:
        raise ValueError("Invalid SQL: Missing SELECT clause")
        
    select_fields = []
    raw_fields = select_match.group(1).split(',')
    
    for field in raw_fields:
        field = field.strip()
        
        # Check for arithmetic: col + number or col - number
        arith_match = re.match(r'(\w+)\s*([+\-*/])\s*([\d.]+)', field, re.IGNORECASE)
        if arith_match:
            col_name = arith_match.group(1).lower()
            operator = arith_match.group(2)
            try:
                value = float(arith_match.group(3)) if '.' in arith_match.group(3) else int(arith_match.group(3))
            except ValueError:
                raise ValueError(f"Invalid arithmetic value in SELECT: {arith_match.group(3)}")
            select_fields.append((operator, col_name, value))
        else:
            select_fields.append(field.lower())

    # 2. Extract table name
    from_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if not from_match:
        raise ValueError("Invalid SQL: Missing FROM clause")
    table_name = from_match.group(1).lower()

    # 3. Extract WHERE filters
    filters: List[FilterCondition] = []
    where_match = re.search(r'WHERE\s+(.*?)(?:\s+LIMIT|\s*$)', query, re.IGNORECASE)
    if where_match:
        condition_str = where_match.group(1).strip()
        # Parse: col op value
        parts = re.split(r'\s*(<=|>=|<>|!=|<|>|=)\s*', condition_str)
        if len(parts) == 3:
            col, op, val_str = parts
            col = col.strip().lower()
            op = op.strip()
            val_str = val_str.strip()
            
            # Type inference for value
            if val_str.startswith("'") and val_str.endswith("'"):
                value = val_str[1:-1]  # String literal
            elif val_str.replace('.', '', 1).replace('-', '', 1).isdigit():
                value = float(val_str) if '.' in val_str else int(val_str)
            else:
                value = val_str
                
            filters.append((op, col, value))

    # 4. Extract LIMIT
    limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
    limit = int(limit_match.group(1)) if limit_match else None

    return SQLStatement(select_fields, table_name, filters, limit)

def generate_logical_plan(statement: SQLStatement) -> LogicalPlan:
    """Converts the parsed statement into a tree of logical operators."""
    
    # Bottom-up construction
    plan = LogicalPlan('Scan', table=statement.table_name)

    if statement.filters:
        plan = LogicalPlan('Filter', child=plan, condition=statement.filters[0])

    plan = LogicalPlan('Project', child=plan, fields=statement.select_fields)

    if statement.limit is not None:
        plan = LogicalPlan('Limit', child=plan, count=statement.limit)

    return plan

