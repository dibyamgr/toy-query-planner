# ============================================================================
# optimizer.py - Rule-Based Query Optimizer
# ============================================================================

from typing import Dict, Any

def optimize(logical_plan) -> Dict[str, Any]:
    """
    Applies optimization rules to the logical plan.
    
    Current Rule: Limit Pushdown
    - Pattern: Limit(Project(X)) 
    - Transform: Project(Limit(X))
    - Benefit: Reduces rows processed by expensive projections
    """
    plan = logical_plan
    message = "No optimization applied (no applicable pattern found)"

    # Rule 1: Limit Pushdown
    if (plan.operation == 'Limit' and 
        plan.child and 
        plan.child.operation == 'Project'):
        
        limit_count = plan.kwargs.get('count')
        project_plan = plan.child
        project_fields = project_plan.kwargs.get('fields')
        
        # New structure: Project(Limit(child))
        from logical_plan import LogicalPlan
        optimized_plan = LogicalPlan(
            'Project',
            child=LogicalPlan(
                'Limit',
                child=project_plan.child,
                count=limit_count
            ),
            fields=project_fields
        )
        
        plan = optimized_plan
        message = (f"✓ Limit Pushdown Applied: Moved LIMIT {limit_count} below PROJECT. "
                   f"This reduces tuple processing in projection stage.")

    return {'plan': plan, 'message': message}

