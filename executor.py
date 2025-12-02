# ============================================================================
# executor.py - Query Execution Engine
# ============================================================================

from typing import List, Dict, Any, Tuple, Union
from physical_plan import PhysicalPlan

def _check_condition(record: Dict[str, Any], condition: Tuple[str, str, Any]) -> bool:
    """
    Evaluates a WHERE condition against a record.
    Handles type-safe comparisons (numeric vs string).
    """
    op, col, predicate_val = condition
    record_val = record.get(col)
    
    if record_val is None:
        return False
    
    # Determine comparison type
    is_numeric = isinstance(predicate_val, (int, float))

    if is_numeric:
        # Numeric comparison
        try:
            actual_val = float(record_val)
            pred_val = float(predicate_val)
        except (ValueError, TypeError):
            return False  # Cannot convert to number
        
        if op in ('<', 'LT'):
            return actual_val < pred_val
        elif op in ('>', 'GT'):
            return actual_val > pred_val
        elif op in ('<=', 'LE'):
            return actual_val <= pred_val
        elif op in ('>=', 'GE'):
            return actual_val >= pred_val
        elif op in ('=', '==', 'EQ'):
            return abs(actual_val - pred_val) < 1e-9  # Floating point equality
        elif op in ('!=', '<>', 'NE'):
            return abs(actual_val - pred_val) >= 1e-9
    else:
        # String comparison (case-insensitive)
        actual_str = str(record_val).lower()
        pred_str = str(predicate_val).lower()
        
        if op in ('=', '==', 'EQ'):
            return actual_str == pred_str
        elif op in ('!=', '<>', 'NE'):
            return actual_str != pred_str
        
    return False

def _evaluate_projection(record: Dict[str, Any], 
                         fields: List[Union[str, Tuple[str, str, Union[int, float]]]]) -> Dict[str, Any]:
    """
    Evaluates the SELECT list, including arithmetic expressions.
    """
    new_record: Dict[str, Any] = {}
    
    for field in fields:
        if isinstance(field, tuple):
            # Arithmetic expression: (op, col, value)
            op, col, val = field
            record_val = record.get(col)
            
            # Generate output column name
            op_name = {'+': 'plus', '-': 'minus', '*': 'times', '/': 'div'}.get(op, op)
            output_col = f"{col}_{op_name}_{val}".replace('.', '_')

            if record_val is not None:
                try:
                    numeric_val = float(record_val)
                    
                    if op == '+':
                        new_record[output_col] = numeric_val + val
                    elif op == '-':
                        new_record[output_col] = numeric_val - val
                    elif op == '*':
                        new_record[output_col] = numeric_val * val
                    elif op == '/':
                        new_record[output_col] = numeric_val / val if val != 0 else None
                    else:
                        new_record[output_col] = None
                except (ValueError, TypeError):
                    new_record[output_col] = None
            else:
                new_record[output_col] = None
        else:
            # Simple column selection
            new_record[field] = record.get(field)
            
    return new_record

def execute(physical_plan: PhysicalPlan, data_catalog: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Recursively executes the physical plan tree.
    Returns the final result set.
    """
    
    def execute_node(p_plan: PhysicalPlan) -> List[Dict[str, Any]]:
        
        # Base case: Sequential Scan
        if p_plan.operation == 'SequentialScan':
            table_name = p_plan.kwargs.get('table', 'unknown')
            return list(data_catalog.get(table_name, []))  # Copy to avoid mutation

        # Recursive case: Get input from child
        input_data = execute_node(p_plan.child) if p_plan.child else []
        
        # Apply operator
        if p_plan.operation == 'FilterIterative':
            condition = p_plan.kwargs.get('condition')
            if condition:
                return [rec for rec in input_data if _check_condition(rec, condition)]
            return input_data

        elif p_plan.operation == 'LimitRows':
            count = p_plan.kwargs.get('count')
            if count is not None:
                return input_data[:count]
            return input_data

        elif p_plan.operation == 'ProjectEvaluate':
            fields = p_plan.kwargs.get('fields', [])
            return [_evaluate_projection(rec, fields) for rec in input_data]

        return input_data

    return execute_node(physical_plan)