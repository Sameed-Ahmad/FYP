"""
LangGraph-based Agent Workflow
Orchestrates the multi-agent system with error recovery.
"""

from langgraph.graph import StateGraph, END
from typing import Dict, Any
import logging

from .state import ConversationState, create_initial_state
from .nodes import (
    UnderstandQueryNode,
    GenerateSQLNode,
    ValidateSQLNode,
    RefineSQLNode,
    ExecuteSQLNode,
    FormatResponseNode
)
from .edges import (
    should_refine_sql,
    should_continue_after_generation,
    should_continue_after_execution,
    route_from_understanding,
    route_from_refinement
)
from .sql_generator import SQLGenerator
from .validator import QueryValidator
from .query_executor import QueryExecutor

logger = logging.getLogger(__name__)


class NLToSQLGraph:
    """
    Multi-agent graph for Natural Language to SQL conversion.
    
    Graph flow:
    START → understand → generate → validate → [refine if needed] → execute → format → END
    """
    
    def __init__(
        self,
        sql_generator: SQLGenerator,
        validator: QueryValidator,
        executor: QueryExecutor,
        schema_manager
    ):
        """
        Initialize the agent graph.
        
        Args:
            sql_generator: SQL generation component
            validator: Query validation component
            executor: Query execution component
            schema_manager: Database schema manager
        """
        self.sql_generator = sql_generator
        self.validator = validator
        self.executor = executor
        self.schema_manager = schema_manager
        
        # Initialize nodes
        self.understand_node = UnderstandQueryNode()
        self.generate_node = GenerateSQLNode(sql_generator)
        self.validate_node = ValidateSQLNode(validator, schema_manager)
        self.refine_node = RefineSQLNode(sql_generator)
        self.execute_node = ExecuteSQLNode(executor)
        self.format_node = FormatResponseNode()
        
        # Build graph
        self.graph = self._build_graph()
        self.compiled_graph = self.graph.compile()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        
        # Create graph with our state schema
        workflow = StateGraph(ConversationState)
        
        # Add nodes
        workflow.add_node("understand", self.understand_node)
        workflow.add_node("generate", self.generate_node)
        workflow.add_node("validate", self.validate_node)
        workflow.add_node("refine", self.refine_node)
        workflow.add_node("execute", self.execute_node)
        workflow.add_node("format", self.format_node)
        
        # Set entry point
        workflow.set_entry_point("understand")
        
        # Add edges with conditional routing
        
        # understand → generate (always)
        workflow.add_edge("understand", "generate")
        
        # generate → validate (if SQL generated) OR retry OR end
        workflow.add_conditional_edges(
            "generate",
            should_continue_after_generation,
            {
                "validate": "validate",
                "generate": "generate",
                "end_with_error": END
            }
        )
        
        # validate → execute (if valid) OR refine (if errors) OR end
        workflow.add_conditional_edges(
            "validate",
            should_refine_sql,
            {
                "execute": "execute",
                "refine": "refine",
                "end_with_error": END
            }
        )
        
        # refine → validate (always re-validate after refinement)
        workflow.add_edge("refine", "validate")
        
        # execute → format (if success) OR refine (if error) OR end
        workflow.add_conditional_edges(
            "execute",
            should_continue_after_execution,
            {
                "format": "format",
                "refine": "refine",
                "end_with_error": END
            }
        )
        
        # format → END (always)
        workflow.add_edge("format", END)
        
        return workflow
    
    def invoke(
        self,
        user_query: str,
        conversation_history: list = None,
        session_id: str = None
    ) -> Dict[str, Any]:
        """
        Execute the agent graph for a user query.
        
        Args:
            user_query: Natural language query
            conversation_history: Previous conversation messages
            session_id: Unique session identifier
            
        Returns:
            Final state after execution
        """
        try:
            # Get schema context
            schema_context = self.schema_manager.get_schema_context()
            available_tables = self.schema_manager.get_table_names()
            available_columns = self.schema_manager.get_available_columns_dict()
            relationships = self.schema_manager.get_relationships()
            
            # Create initial state
            initial_state = create_initial_state(
                user_query=user_query,
                schema_context=schema_context,
                available_tables=available_tables,
                available_columns=available_columns,
                relationships=relationships,
                conversation_history=conversation_history,
                session_id=session_id
            )
            
            logger.info(f"Starting graph execution for query: {user_query[:50]}...")
            
            # Execute graph
            final_state = self.compiled_graph.invoke(initial_state)
            
            logger.info(f"Graph execution completed with status: {final_state.get('status')}")
            
            return final_state
            
        except Exception as e:
            logger.error(f"Graph execution error: {str(e)}")
            return {
                'status': 'failed',
                'error_message': str(e),
                'formatted_response': f"I encountered an error processing your query: {str(e)}"
            }
    
    async def ainvoke(
        self,
        user_query: str,
        conversation_history: list = None,
        session_id: str = None
    ) -> Dict[str, Any]:
        """
        Async version of invoke (for future async support).
        
        Args:
            user_query: Natural language query
            conversation_history: Previous conversation messages
            session_id: Unique session identifier
            
        Returns:
            Final state after execution
        """
        # For now, just call sync version
        # Can be enhanced for true async execution later
        return self.invoke(user_query, conversation_history, session_id)
    
    def stream(
        self,
        user_query: str,
        conversation_history: list = None,
        session_id: str = None
    ):
        """
        Stream intermediate results during execution.
        
        Args:
            user_query: Natural language query
            conversation_history: Previous conversation messages
            session_id: Unique session identifier
            
        Yields:
            Intermediate states during execution
        """
        # Get schema context
        schema_context = self.schema_manager.get_schema_context()
        available_tables = self.schema_manager.get_table_names()
        available_columns = self.schema_manager.get_available_columns_dict()
        relationships = self.schema_manager.get_relationships()
        
        # Create initial state
        initial_state = create_initial_state(
            user_query=user_query,
            schema_context=schema_context,
            available_tables=available_tables,
            available_columns=available_columns,
            relationships=relationships,
            conversation_history=conversation_history,
            session_id=session_id
        )
        
        # Stream execution
        for state in self.compiled_graph.stream(initial_state):
            yield state
    
    def get_graph_visualization(self) -> str:
        """
        Get ASCII visualization of the graph structure.
        
        Returns:
            ASCII representation of graph
        """
        return """
Agent Graph Flow:
==================

    START
      ↓
[understand_query]  ← Analyze intent & extract entities
      ↓
[generate_sql]      ← Convert NL to SQL using Gemini
      ↓
      ├─→ (retry if failed, max 3 attempts)
      ↓
[validate_sql]      ← Security & schema validation
      ↓
      ├─→ [refine_sql] → (back to validate)
      ↓             ↑
      |             └─ (if validation errors)
      ↓
[execute_sql]       ← Run query against database
      ↓
      ├─→ [refine_sql] (if execution error)
      ↓
[format_response]   ← Format results conversationally
      ↓
     END

Recovery Mechanisms:
- Failed generation → Retry up to 3 times
- Validation errors → Refine and re-validate
- Execution errors → Refine and retry
- Max attempts exceeded → Return error message
"""