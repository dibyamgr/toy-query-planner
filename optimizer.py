# optimizer.py
from typing import Dict, Any
from logical_plan import LogicalPlan # Direct Import

def optimize(logical_plan: LogicalPlan) -> Dict[str, Any]:
    """
    Applies optimization rules to the logical plan.

    Optimization Rule: Limit Pushdown (Push Limit node down past Project node).
    
    Why: By limiting the rows *before* performing expensive projections (like 
    complex calculations), we save computation time.
    """
    plan = logical_plan
    message = "No major optimizations applied."

    # Check for Limit(Project(X)) pattern
    if (plan.operation == 'Limit' and 
        plan.child and 
        plan.child.operation == 'Project'):
        
        limit_count = plan.kwargs.get('count')
        project_plan = plan.child
        
        # New structure: Project -> Limit -> X
        optimized_plan = LogicalPlan(
            'Project', 
            child=LogicalPlan(
                'Limit',
                child=project_plan.child, # X (Filter/Scan)
                count=limit_count
            ),
            fields=project_plan.kwargs.get('fields')
        )
        
        plan = optimized_plan
        message = "Applied Limit Pushdown: Moved Limit below Project. Reduces unnecessary row calculations."

    return {'plan': plan, 'message': message}
