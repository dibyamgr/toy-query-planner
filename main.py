# main.py
import json
import sys
from typing import List, Dict, Any

# Assuming all modules are in the same folder structure and are imported directly.
# This structure is common when running a main script that orchestrates other modules
# in the same directory.
try:
    # Changed to direct module import names (not relative imports with '.')
    from logical_plan import parse_sql, generate_logical_plan, LogicalPlan
    from optimizer import optimize
    from physical_plan import generate_physical_plan, PhysicalPlan
    from executor import execute
except ImportError as e:
    # This check now provides better context on the missing module
    print("\n" + "="*80)
    print("!!! FATAL ERROR: Could not import internal modules. !!!")
    print("This usually means Python cannot find one of the required files (logical_plan.py, optimizer.py, etc.)")
    print(f"Details: {e}")
    print("Ensure all six .py files are saved in the SAME directory and that their internal imports (e.g., in executor.py) use the direct module name (e.g., 'from physical_plan import...') instead of relative imports (e.g., 'from .physical_plan import...').")
    print("="*80)
    sys.exit(1)


def print_section_header(title: str, query: str = "") -> None:
    """Prints a styled section header for the console demo."""
    print("\n" + "="*80)
    print(f" {title} ".center(80, '-'))
    if query:
        print(f" SQL: {query} ".center(80, ' '))
    print("="*80)

def print_plan_tree(title: str, plan: LogicalPlan | PhysicalPlan) -> None:
    """Prints a formatted plan tree for clear visualization."""
    print(f"\n--- {title} Plan Tree (Top-Down Execution Flow) ---")
    print(plan.format_tree(indent=0))
    print("-" * 30)


def run_sql_api(sql_query: str) -> List[Dict[str, Any]]:
    """
    The main public API function that executes the full query planning pipeline.
    """
    
    print_section_header("START: QUERY PLANNER PIPELINE", sql_query)

    try:
        # --- Stage 1: Parsing (parse_sql) ---
        print("\n--- 1. PARSING: SQL -> Statement (AST) ---")
        statement = parse_sql(sql_query)
        print(f"   -> Statement: {repr(statement)}")
        
        # --- Stage 2: Logical Plan Generation (logical_plan) ---
        print("\n--- 2. LOGICAL PLANNING: Statement -> Relational Algebra Tree ---")
        logical_plan_initial = generate_logical_plan(statement)
        print_plan_tree("Initial Logical", logical_plan_initial)
        
        # --- Stage 3: Optimization (optimize) ---
        print("\n--- 3. OPTIMIZATION: Applying Rules to Logical Plan ---")
        logical_plan_optimized = optimize(logical_plan_initial)
        # Only print the optimized tree if an optimization actually occurred
        if logical_plan_optimized != logical_plan_initial:
            print_plan_tree("Optimized Logical", logical_plan_optimized)
        
        # --- Stage 4: Physical Plan Generation (physical_plan) ---
        print("\n--- 4. PHYSICAL PLANNING: Logical Operators -> Execution Strategy ---")
        physical_plan = generate_physical_plan(logical_plan_optimized)
        print_plan_tree("Physical Execution", physical_plan)
        
        # --- Stage 5: Execution (execute) ---
        print("\n--- 5. EXECUTION: Running the Physical Plan (Bottom-Up) ---")
        records = execute(physical_plan)
        
        # --- Final Result ---
        print_section_header("END: QUERY RESULT")
        print(json.dumps(records, indent=2))
        print("="*80)
        return records

    except Exception as e:
        print(f"\n!!! CRITICAL EXECUTION ERROR: {e} !!!")
        print("="*80)
        return []

# --- Demo Execution ---
if __name__ == "__main__":
    SQL_QUERY = "SELECT id, name, age + 100 FROM t1 WHERE id < 6 LIMIT 3"
    
    print("DEMO: Running Query Planner with sample SQL.")
    run_sql_api(SQL_QUERY)
    
    print("\n\n--- DEMO COMPLETE ---")
    print("To run this professional structure, save all files in a directory and run 'python main.py' in your terminal.")
