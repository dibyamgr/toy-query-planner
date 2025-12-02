# ============================================================================
# app.py - Flask API Server for Query Planner
# ============================================================================

from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import traceback

try:
    from data_source import parse_data_to_catalog 
    from logical_plan import parse_sql, generate_logical_plan
    from optimizer import optimize
    from physical_plan import generate_physical_plan
    from executor import execute
except ImportError as e:
    print(f"FATAL ERROR: Failed to import planner modules. Details: {e}")
    exit(1)

app = Flask(__name__)
CORS(app)

def format_plan_tree(plan) -> str:
    """Formats the plan tree for JSON serialization."""
    if hasattr(plan, 'format_tree'):
        return plan.format_tree()
    return str(plan)

@app.route('/run_query', methods=['POST'])
def run_query_api():
    """
    Receives SQL query and CSV data, runs the full pipeline, and returns
    structured results for each stage.
    """
    try:
        data = request.get_json()
        sql_query = data.get('sql_query', '').strip()
        csv_data = data.get('csv_data', '').strip()
        table_name = data.get('table_name', 't1').strip()
        
        if not sql_query:
            return jsonify({'error': 'SQL query is required', 'log': []}), 400
        if not csv_data:
            return jsonify({'error': 'CSV data is required', 'log': []}), 400
        
        pipeline_results = {
            'log': [],
            'stages': {}
        }
        
        def pipeline_log(message, stage_name):
            pipeline_results['log'].append({'stage': stage_name, 'message': message})

        # 0. Data Preparation
        data_catalog = parse_data_to_catalog(csv_data, table_name)
        row_count = len(data_catalog.get(table_name, []))
        pipeline_log(f"Data loaded: {row_count} records into table '{table_name}'", 'data')
        
        # 1. PARSING (SQL -> AST)
        statement = parse_sql(sql_query)
        pipeline_log(f"SQL parsed successfully\nAST: {repr(statement)}", 'parse')
        
        # 2. LOGICAL PLAN GENERATION (AST -> Initial Logical Plan)
        logical_plan_initial = generate_logical_plan(statement)
        pipeline_results['stages']['logical'] = format_plan_tree(logical_plan_initial)
        pipeline_log("Initial Logical Plan generated", 'logical')
        
        # 3. OPTIMIZATION (Initial Logical Plan -> Optimized Logical Plan)
        opt_output = optimize(logical_plan_initial)
        logical_plan_optimized = opt_output['plan']
        pipeline_results['stages']['optimize'] = format_plan_tree(logical_plan_optimized)
        pipeline_log(f"Optimization: {opt_output['message']}", 'optimize')

        # 4. PHYSICAL PLAN GENERATION (Optimized Logical Plan -> Physical Plan)
        physical_plan = generate_physical_plan(logical_plan_optimized)
        pipeline_results['stages']['physical'] = format_plan_tree(physical_plan)
        pipeline_log("Physical Plan generated (execution strategy selected)", 'physical')

        # 5. EXECUTION (Physical Plan + Data -> Records)
        final_records = execute(physical_plan, data_catalog)
        
        # 6. FINAL RESULT
        pipeline_results['stages']['execute'] = json.dumps(final_records, indent=2)
        pipeline_log(f"Execution complete: {len(final_records)} rows returned", 'execute')

        return jsonify(pipeline_results)

    except ValueError as e:
        return jsonify({
            'error': f"Query Error: {str(e)}", 
            'log': [{'stage': 'error', 'message': str(e)}]
        }), 400
    except Exception as e:
        traceback.print_exc()
        return jsonify({
            'error': f"System Error: {str(e)}", 
            'log': [{'stage': 'error', 'message': f"Internal error: {str(e)}"}]
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'Query Planner API is running'})

if __name__ == '__main__':
    print("=" * 70)
    print("Starting Flask Query Planner API Server")
    print("=" * 70)
    print("API Endpoints:")
    print("  - POST /run_query   : Execute SQL query")
    print("  - GET  /health      : Health check")
    print("\nServer running at: http://127.0.0.1:5000")
    print("=" * 70)
    app.run(debug=True, host='127.0.0.1', port=5000)
