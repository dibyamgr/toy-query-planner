# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import json

# IMPORTANT: All internal modules must use direct imports (e.g., from logical_plan import...)
try:
    from data_source import parse_data_to_catalog 
    from logical_plan import parse_sql, generate_logical_plan
    from optimizer import optimize
    from physical_plan import generate_physical_plan
    from executor import execute
    from logical_plan import LogicalPlan, SQLStatement 
    from physical_plan import PhysicalPlan
except ImportError as e:
    print(f"FATAL ERROR: Failed to import planner modules in app.py. Ensure all files are in the same directory and use DIRECT imports. Details: {e}")
    # Exit here to prevent server startup without core logic
    exit(1)


app = Flask(__name__)
# Enable CORS to allow the HTML file (running locally) to call the Flask API
CORS(app) 

# --- Helper Functions for Serialization ---

def format_plan_tree(plan) -> str:
    """Formats the plan tree for JSON serialization."""
    if hasattr(plan, 'format_tree'):
        return plan.format_tree()
    return str(plan)

# --- The Core API Endpoint ---

@app.route('/run_query', methods=['POST'])
def run_query_api():
    """
    Receives SQL query and CSV data, runs the full pipeline, and returns
    structured results for each stage.
    """
    try:
        data = request.get_json()
        sql_query = data.get('sql_query', '')
        csv_data = data.get('csv_data', '')
        table_name = data.get('table_name', 't1')
        
        # Dictionary to store results of each stage
        pipeline_results = {
            'log': [],
            'stages': {}
        }
        
        def pipeline_log(message, stage_name):
            pipeline_results['log'].append({'stage': stage_name, 'message': message})

        # 0. Data Preparation
        data_catalog = parse_data_to_catalog(csv_data, table_name)
        pipeline_log(f"Data prepared: Loaded {len(data_catalog.get(table_name, []))} records into mock table '{table_name}'.", 'data')
        
        
        # 1. PARSING (SQL -> AST)
        statement = parse_sql(sql_query)
        pipeline_log(f"Statement (AST):\n{repr(statement)}", 'parse')
        
        # 2. LOGICAL PLAN GENERATION (AST -> Initial Logical Plan)
        logical_plan_initial = generate_logical_plan(statement)
        pipeline_results['stages']['logical'] = format_plan_tree(logical_plan_initial)
        pipeline_log("Generated Initial Logical Plan.", 'logical')
        
        # 3. OPTIMIZATION (Initial Logical Plan -> Optimized Logical Plan)
        opt_output = optimize(logical_plan_initial)
        logical_plan_optimized = opt_output['plan']
        pipeline_results['stages']['optimize'] = format_plan_tree(logical_plan_optimized)
        pipeline_log(f"Optimization applied: {opt_output['message']}", 'optimize')

        # 4. PHYSICAL PLAN GENERATION (Optimized Logical Plan -> Physical Plan)
        physical_plan = generate_physical_plan(logical_plan_optimized)
        pipeline_results['stages']['physical'] = format_plan_tree(physical_plan)
        pipeline_log("Generated Physical Plan (Execution Strategy).", 'physical')

        # 5. EXECUTION (Physical Plan + Data -> Records)
        final_records = execute(physical_plan, data_catalog)
        
        # 6. FINAL RESULT
        pipeline_results['stages']['execute'] = json.dumps(final_records, indent=2)
        pipeline_log(f"Execution complete. Returned {len(final_records)} final records.", 'execute')

        return jsonify(pipeline_results)

    except Exception as e:
        # Catch and report any runtime errors cleanly
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e), 
            'log': [{'stage': 'error', 'message': f"Critical Error: {str(e)} (Check Flask console for trace)"}]
        }), 500

if __name__ == '__main__':
    print("Starting Flask server for Query Planner Demo...")
    print("API available at http://127.0.0.1:5000/run_query")
    app.run(debug=True)
