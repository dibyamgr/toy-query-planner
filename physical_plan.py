# ============================================================================
# physical_plan.py - Physical Plan Generator
# ============================================================================

from typing import Optional, Dict, Any

class PhysicalPlan:
    """Represents the physical execution strategy (HOW to do it)."""
    def __init__(self, operation: str, child: Optional['PhysicalPlan'] = None, **kwargs: Any):
        self.operation = operation
        self.child = child
        self.kwargs = kwargs
    
    def __repr__(self, indent: int = 0) -> str:
        """Formats the plan tree for console output."""
        args_repr = ', '.join(f"{k}={v!r}" for k, v in self.kwargs.items())
        s = '  ' * indent + f"→ {self.operation}({args_repr})"
        if self.child:
            s += '\n' + self.child.__repr__(indent + 1)
        return s
        
    def format_tree(self, indent: int = 0) -> str:
        """Alias for __repr__."""
        return self.__repr__(indent)

def generate_physical_plan(logical_plan) -> PhysicalPlan:
    """Recursively converts a LogicalPlan tree into a PhysicalPlan tree."""
    def map_to_physical(l_plan) -> Optional[PhysicalPlan]:
        if l_plan is None:
            return None
        
        child_physical = map_to_physical(l_plan.child)
        kwargs = l_plan.kwargs

        # Map logical operators to physical algorithms
        if l_plan.operation == 'Scan':
            return PhysicalPlan('SequentialScan', child=child_physical, 
                                table=kwargs.get('table'))
        
        elif l_plan.operation == 'Filter':
            return PhysicalPlan('FilterIterative', child=child_physical, 
                                condition=kwargs.get('condition'))
        
        elif l_plan.operation == 'Project':
            return PhysicalPlan('ProjectEvaluate', child=child_physical, 
                                fields=kwargs.get('fields'))
        
        elif l_plan.operation == 'Limit':
            return PhysicalPlan('LimitRows', child=child_physical, 
                                count=kwargs.get('count'))
        
        raise ValueError(f"Unknown logical operator: {l_plan.operation}")

    return map_to_physical(logical_plan)