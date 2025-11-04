"""
Conditional Edge Logic for Agent Graph
Determines routing between nodes based on state.
"""

from .state import ConversationState


def should_refine_sql(state: ConversationState) -> str:
    """
    Determine if SQL needs refinement.
    
    Args:
        state: Current conversation state
        
    Returns:
        Next node name
    """
    # Check if validation passed
    if state.get('is_valid', False):
        return "execute"
    
    # Check if we've exceeded max attempts
    if state.get('generation_attempt', 0) >= state.get('max_attempts', 3):
        return "end_with_error"
    
    # Check if we have validation errors that can be fixed
    errors = state.get('validation_errors', [])
    if errors:
        return "refine"
    
    # If warnings only, proceed to execution
    warnings = state.get('validation_warnings', [])
    if warnings and not errors:
        return "execute"
    
    return "end_with_error"


def should_continue_after_generation(state: ConversationState) -> str:
    """
    Determine next step after SQL generation.
    
    Args:
        state: Current conversation state
        
    Returns:
        Next node name
    """
    # Check if generation succeeded
    if state.get('generated_sql'):
        return "validate"
    
    # Check if we can retry
    if state.get('generation_attempt', 0) < state.get('max_attempts', 3):
        return "generate"
    
    return "end_with_error"


def should_continue_after_execution(state: ConversationState) -> str:
    """
    Determine next step after execution.
    
    Args:
        state: Current conversation state
        
    Returns:
        Next node name
    """
    # Check if execution succeeded
    if state.get('query_results') is not None:
        return "format"
    
    # Check for execution error
    if state.get('execution_error'):
        # Check if we can refine and retry
        if state.get('generation_attempt', 0) < state.get('max_attempts', 3):
            return "refine"
        return "end_with_error"
    
    return "end_with_error"


def route_from_understanding(state: ConversationState) -> str:
    """
    Route after understanding query.
    
    Args:
        state: Current conversation state
        
    Returns:
        Next node name
    """
    # Always proceed to generation after understanding
    return "generate"


def route_from_refinement(state: ConversationState) -> str:
    """
    Route after SQL refinement.
    
    Args:
        state: Current conversation state
        
    Returns:
        Next node name
    """
    # After refinement, validate again
    return "validate"


def should_end(state: ConversationState) -> bool:
    """
    Check if workflow should end.
    
    Args:
        state: Current conversation state
        
    Returns:
        True if should end, False otherwise
    """
    # End if we have a formatted response
    if state.get('formatted_response'):
        return True
    
    # End if there's a fatal error
    if state.get('error_message') and state.get('generation_attempt', 0) >= state.get('max_attempts', 3):
        return True
    
    return False