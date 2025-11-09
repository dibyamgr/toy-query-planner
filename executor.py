# executor.py
from typing import List, Dict, Any, Tuple, Union
from physical_plan import PhysicalPlan # Direct Import
from data_source import DataCatalog, Record # Direct Import

# --- Helper Functions ---

def _check_condition(record: Record, condition: Tuple[str, str, Union[int, str, float]]) -> bool:
    """
    Evaluates a single WHERE condition against a record.
    CRITICAL FIX: Safely handles type casting to prevent TypeError between float/str.
    """
    op, col, predicate_val = condition
    record_val = record.get(col)
    
    if record_val is None:
        return False
    
    # 1. Determine if the predicate is a number (float/int)
    is_numeric_comparison = isinstance(predicate_val, (int, float))

    if is_numeric_comparison:
        # A. Try to cast the record's value to a float for comparison
        try:
            actual_val = float(record_val)
            float_predicate_val = float(predicate_val)
        except ValueError:
            # B. If record_val is a string (e.g., 'St. John\'s') and predicate is numeric,
            # this comparison is invalid, so we return False. This prevents the TypeError.
            return False 

        # Perform numeric comparison
        if op == '<':
            return actual_val < float_predicate_val
        elif op == '>':
            return actual_val > float_predicate_val
        elif op in ('=', '=='):
            return actual_val == float_predicate_val
        elif op == '!=':
            return actual_val != float_predicate_val
    
    else:
        # 2. String Comparison (Predicate is a string, e.g., WHERE city = 'Gander')
        if op in ('=', '=='):
            # Compare the actual record value (which might still be a string)
            return str(record_val).lower() == str(predicate_val).lower()
        elif op == '!=':
            return str(record_val).lower() != str(predicate_val).lower()
        
    return False

def _evaluate_projection(record: Record, fields: List[Union[str, Tuple[str, str, float]]]) -> Record:
    """Evaluates the SELECT list expressions, handling type conversion for arithmetic."""
    new_record: Record = {}
    
    for field in fields:
        if isinstance(field, tuple):
            # Arithmetic: ('+', 'avg_temp_c', 32.0)
            op, col, val = field
            
            record_val = record.get(col)
            output_col_name = f"{col}_{op.replace('*', 'times').replace('+', 'plus')}_{val}" # Basic alias for now

            if record_val is not None:
                try:
                    # CRITICAL FIX: Cast to float for calculation
                    numeric_record_val = float(record_val) 

                    if op == '+':
                        new_record[output_col_name] = numeric_record_val + val
                    # Add more operations if supported (e.g., '-', '*', '/')
                    else:
                        new_record[output_col_name] = None
                except ValueError:
                    # Column exists but isn't numeric 
                    new_record[output_col_name] = None 
            else:
                 new_record[output_col_name] = None
        else:
            # Simple column selection
            new_record[field] = record.get(field)
            
    return new_record

# --- Execution Engine ---

def execute(physical_plan: PhysicalPlan, data_catalog: DataCatalog) -> List[Record]:
    """Recursively executes the physical plan tree, returning final records."""
    
    def execute_node(p_plan: PhysicalPlan) -> List[Record]:
        
        # 1. Base Case: Sequential Scan (Data Source)
        if p_plan.operation == 'SequentialScan':
            table_name = p_plan.kwargs.get('table', 'unknown')
            return data_catalog.get(table_name, [])

        # 2. Recursive Case: Get input data from child node
        input_data = execute_node(p_plan.child) if p_plan.child else []
        result = input_data

        # 3. Execution Logic per Operator
        if p_plan.operation == 'FilterIterative':
            condition = p_plan.kwargs.get('condition')
            if condition:
                result = [record for record in input_data if _check_condition(record, condition)]

        elif p_plan.operation == 'LimitRows':
            count = p_plan.kwargs.get('count')
            if count is not None:
                result = input_data[:count]

        elif p_plan.operation == 'ProjectEvaluate':
            fields = p_plan.kwargs.get('fields', [])
            result = [_evaluate_projection(record, fields) for record in input_data]

        return result

    return execute_node(physical_plan)