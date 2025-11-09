# physical_plan.py
from typing import Optional, Dict, Any, Union
from logical_plan import LogicalPlan # Direct Import

class PhysicalPlan:
    """Represents the physical execution strategy (HOW to do it)."""
    def __init__(self, operation: str, child: Optional['PhysicalPlan'] = None, **kwargs: Any):
        self.operation = operation # e.g., 'SequentialScan', 'FilterIterative', 'ProjectEvaluate', 'LimitRows'
        self.child = child         # Child plan
        self.kwargs = kwargs       # Operation-specific arguments
    
    def __repr__(self, indent: int = 0) -> str:
        """Formats the plan tree for console output."""
        # Clean up kwargs for display
        args_repr = ', '.join(f"{k}={v!r}" for k, v in self.kwargs.items())
        
        # Determine the operator name for clarity
        op_name = self.operation.replace('Iterative', '').replace('Rows', '')

        # Use indentation to show hierarchy
        s = '  ' * indent + f"-> {op_name}({args_repr})"
        if self.child:
            s += '\n' + self.child.__repr__(indent + 1)
        return s
        
    def format_tree(self, indent: int = 0) -> str:
        """Alias for __repr__ for external formatting calls (like in app.py)."""
        return self.__repr__(indent)


def generate_physical_plan(logical_plan: LogicalPlan) -> PhysicalPlan:
    """Recursively converts a LogicalPlan tree into a PhysicalPlan tree."""
    def map_logical_to_physical(l_plan: Optional[LogicalPlan]) -> Optional[PhysicalPlan]:
        if l_plan is None:
            return None
        
        new_child = map_logical_to_physical(l_plan.child)
        kwargs = l_plan.kwargs

        if l_plan.operation == 'Scan':
            return PhysicalPlan('SequentialScan', child=new_child, table=kwargs.get('table'))
        
        elif l_plan.operation == 'Filter':
            # Store the predicate value exactly as it is (it's handled safely in executor.py)
            condition = kwargs.get('condition')
            return PhysicalPlan('FilterIterative', child=new_child, condition=condition)
        
        elif l_plan.operation == 'Project':
            return PhysicalPlan('ProjectEvaluate', child=new_child, fields=kwargs.get('fields'))
        
        elif l_plan.operation == 'Limit':
            return PhysicalPlan('LimitRows', child=new_child, count=kwargs.get('count'))
        
        raise ValueError(f"Unknown logical operator: {l_plan.operation}")

    physical_plan = map_logical_to_physical(logical_plan)
    if physical_plan is None:
        raise Exception("Physical plan generation failed (resulted in None).")
    return physical_plan