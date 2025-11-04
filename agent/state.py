"""
Agent State Management
Defines the state structure for the multi-agent graph workflow.
"""

from typing import TypedDict, Optional, List, Dict, Any
from enum import Enum


class QueryComplexity(str, Enum):
    """Query complexity levels"""
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"


class AgentStatus(str, Enum):
    """Agent execution status"""
    INITIALIZED = "initialized"
    UNDERSTANDING = "understanding"
    GENERATING = "generating"
    VALIDATING = "validating"
    REFINING = "refining"
    EXECUTING = "executing"
    FORMATTING = "formatting"
    SUCCESS = "success"
    FAILED = "failed"


class ConversationState(TypedDict, total=False):
    """
    State object that flows through the agent graph.
    Uses TypedDict for type hints but allows flexibility.
    """
    # Input
    user_query: str
    conversation_history: List[Dict[str, str]]
    
    # Context
    schema_context: str
    available_tables: List[str]
    available_columns: Dict[str, List[str]]
    relationships: Dict[str, List[Dict[str, str]]]
    
    # Query Understanding
    query_intent: Dict[str, Any]
    detected_entities: Dict[str, List[str]]  # tables, columns mentioned
    expected_output_type: str  # "list", "count", "aggregate", "comparison"
    
    # SQL Generation
    generated_sql: Optional[str]
    sql_explanation: Optional[str]
    generation_attempt: int
    max_attempts: int
    
    # Validation
    is_valid: bool
    validation_errors: List[str]
    validation_warnings: List[str]
    complexity: QueryComplexity
    
    # Refinement
    refinement_feedback: Optional[str]
    needs_refinement: bool
    
    # Execution
    query_results: Optional[List[Dict[str, Any]]]
    execution_time: Optional[float]
    row_count: Optional[int]
    execution_error: Optional[str]
    
    # Output Formatting
    formatted_response: Optional[str]
    result_summary: Optional[str]
    
    # Status & Control Flow
    status: AgentStatus
    error_message: Optional[str]
    retry_count: int
    
    # Metadata
    session_id: str
    timestamp: str


class ConversationMessage(TypedDict):
    """Single conversation message"""
    role: str  # "user" or "assistant"
    content: str
    timestamp: str
    metadata: Optional[Dict[str, Any]]


def create_initial_state(
    user_query: str,
    schema_context: str,
    available_tables: List[str],
    available_columns: Dict[str, List[str]],
    relationships: Dict[str, List[Dict[str, str]]],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    session_id: Optional[str] = None
) -> ConversationState:
    """
    Create initial state for agent execution.
    
    Args:
        user_query: Natural language query from user
        schema_context: Formatted database schema
        available_tables: List of table names
        available_columns: Dict mapping table names to column lists
        relationships: Foreign key relationships
        conversation_history: Previous conversation messages
        session_id: Unique session identifier
        
    Returns:
        Initialized ConversationState
    """
    from datetime import datetime
    import uuid
    
    return ConversationState(
        # Input
        user_query=user_query,
        conversation_history=conversation_history or [],
        
        # Context
        schema_context=schema_context,
        available_tables=available_tables,
        available_columns=available_columns,
        relationships=relationships,
        
        # Query Understanding
        query_intent={},
        detected_entities={"tables": [], "columns": []},
        expected_output_type="list",
        
        # SQL Generation
        generated_sql=None,
        sql_explanation=None,
        generation_attempt=0,
        max_attempts=3,
        
        # Validation
        is_valid=False,
        validation_errors=[],
        validation_warnings=[],
        complexity=QueryComplexity.SIMPLE,
        
        # Refinement
        refinement_feedback=None,
        needs_refinement=False,
        
        # Execution
        query_results=None,
        execution_time=None,
        row_count=None,
        execution_error=None,
        
        # Output Formatting
        formatted_response=None,
        result_summary=None,
        
        # Status & Control Flow
        status=AgentStatus.INITIALIZED,
        error_message=None,
        retry_count=0,
        
        # Metadata
        session_id=session_id or str(uuid.uuid4()),
        timestamp=datetime.now().isoformat()
    )