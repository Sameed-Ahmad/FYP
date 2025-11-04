"""
Graph Node Implementations
Each node represents a step in the agent workflow.
"""

from typing import Dict, Any
import re
from .state import ConversationState, AgentStatus, QueryComplexity
from .sql_generator import SQLGenerator
from .validator import QueryValidator
from .query_executor import QueryExecutor
import logging

logger = logging.getLogger(__name__)


class UnderstandQueryNode:
    """Understand user intent and extract entities"""
    
    def __call__(self, state: ConversationState) -> Dict[str, Any]:
        """
        Analyze user query to extract intent and entities.
        
        Args:
            state: Current conversation state
            
        Returns:
            Updated state with intent and entities
        """
        query = state['user_query'].lower()
        
        # Detect intent
        intent = {}
        
        if any(word in query for word in ['show', 'list', 'display', 'get', 'give me']):
            intent['action'] = 'retrieve'
        elif any(word in query for word in ['count', 'how many', 'total number']):
            intent['action'] = 'count'
        elif any(word in query for word in ['top', 'best', 'highest', 'most']):
            intent['action'] = 'top_n'
            # Extract number
            match = re.search(r'top\s+(\d+)', query)
            intent['limit'] = int(match.group(1)) if match else 10
        elif any(word in query for word in ['compare', 'difference', 'vs', 'versus']):
            intent['action'] = 'compare'
        elif any(word in query for word in ['average', 'avg', 'mean']):
            intent['action'] = 'aggregate'
            intent['function'] = 'avg'
        elif any(word in query for word in ['sum', 'total']):
            intent['action'] = 'aggregate'
            intent['function'] = 'sum'
        else:
            intent['action'] = 'retrieve'
        
        # Detect aggregation needs
        intent['needs_aggregation'] = any(word in query for word in 
            ['count', 'sum', 'average', 'avg', 'total', 'min', 'max'])
        
        # Detect grouping needs
        intent['needs_grouping'] = any(word in query for word in 
            ['by', 'per', 'each', 'every', 'category'])
        
        # Detect ordering needs
        intent['needs_ordering'] = any(word in query for word in 
            ['top', 'bottom', 'best', 'worst', 'highest', 'lowest', 'latest'])
        
        # Detect entities (tables and columns mentioned)
        detected_entities = {
            'tables': [],
            'columns': []
        }
        
        available_tables = state.get('available_tables', [])
        available_columns = state.get('available_columns', {})
        
        # Find mentioned tables
        for table in available_tables:
            # Check singular and plural forms
            table_variations = [
                table.lower(),
                table.lower().rstrip('s'),  # Remove trailing 's'
                table.lower() + 's'  # Add 's'
            ]
            if any(variation in query for variation in table_variations):
                detected_entities['tables'].append(table)
        
        # Find mentioned columns
        for table, columns in available_columns.items():
            for column in columns:
                if column.lower() in query:
                    detected_entities['columns'].append(column)
        
        # Determine expected output type
        if intent['action'] == 'count':
            expected_output = 'count'
        elif intent['action'] == 'aggregate':
            expected_output = 'aggregate'
        elif intent['action'] == 'compare':
            expected_output = 'comparison'
        elif intent.get('needs_grouping'):
            expected_output = 'grouped_list'
        else:
            expected_output = 'list'
        
        return {
            'query_intent': intent,
            'detected_entities': detected_entities,
            'expected_output_type': expected_output,
            'status': AgentStatus.UNDERSTANDING
        }


class GenerateSQLNode:
    """Generate SQL from natural language using Gemini"""
    
    def __init__(self, sql_generator: SQLGenerator):
        self.sql_generator = sql_generator
    
    def __call__(self, state: ConversationState) -> Dict[str, Any]:
        """
        Generate SQL query from user input.
        
        Args:
            state: Current conversation state
            
        Returns:
            Updated state with generated SQL
        """
        try:
            # Build enhanced prompt context
            context = self._build_generation_context(state)
            
            # Generate SQL
            sql, explanation = self.sql_generator.generate_sql(
                state['user_query'],
                additional_context=context
            )
            
            return {
                'generated_sql': sql,
                'sql_explanation': explanation,
                'generation_attempt': state['generation_attempt'] + 1,
                'status': AgentStatus.GENERATING,
                'error_message': None
            }
            
        except Exception as e:
            logger.error(f"SQL generation error: {str(e)}")
            return {
                'generated_sql': None,
                'error_message': f"Failed to generate SQL: {str(e)}",
                'status': AgentStatus.FAILED
            }
    
    def _build_generation_context(self, state: ConversationState) -> str:
        """Build additional context for SQL generation"""
        context_parts = []
        
        intent = state.get('query_intent', {})
        entities = state.get('detected_entities', {})
        
        # Add intent hints
        if intent:
            context_parts.append(f"Query Intent: {intent.get('action', 'retrieve')}")
            if intent.get('limit'):
                context_parts.append(f"Limit results to: {intent['limit']}")
        
        # Add entity hints
        if entities.get('tables'):
            context_parts.append(f"Mentioned tables: {', '.join(entities['tables'])}")
        if entities.get('columns'):
            context_parts.append(f"Mentioned columns: {', '.join(entities['columns'])}")
        
        # Add output expectations
        output_type = state.get('expected_output_type', 'list')
        if output_type == 'count':
            context_parts.append("Expected output: Single count value")
        elif output_type == 'comparison':
            context_parts.append("Expected output: Only requested columns for comparison")
        elif output_type in ['list', 'grouped_list']:
            context_parts.append("Expected output: Specific columns only (avoid SELECT *)")
        
        # Add refinement feedback if exists
        if state.get('refinement_feedback'):
            context_parts.append(f"\nPrevious attempt feedback:\n{state['refinement_feedback']}")
        
        return "\n".join(context_parts)


class ValidateSQLNode:
    """Validate generated SQL for security and correctness"""
    
    def __init__(self, validator: QueryValidator, schema_manager):
        self.validator = validator
        self.schema_manager = schema_manager
    
    def __call__(self, state: ConversationState) -> Dict[str, Any]:
        """
        Validate SQL query.
        
        Args:
            state: Current conversation state
            
        Returns:
            Updated state with validation results
        """
        sql = state.get('generated_sql')
        
        if not sql:
            return {
                'is_valid': False,
                'validation_errors': ['No SQL query generated'],
                'status': AgentStatus.FAILED
            }
        
        try:
            # Basic security validation
            is_valid = self.validator.validate(sql)
            complexity = self.validator.get_query_complexity(sql)
            
            # Schema-aware validation
            schema_errors = self._validate_against_schema(sql, state)
            
            # Intent-based validation
            intent_warnings = self._validate_against_intent(sql, state)
            
            all_errors = schema_errors
            all_warnings = intent_warnings
            
            final_valid = is_valid and len(all_errors) == 0
            
            return {
                'is_valid': final_valid,
                'validation_errors': all_errors,
                'validation_warnings': all_warnings,
                'complexity': complexity,
                'status': AgentStatus.VALIDATING
            }
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            return {
                'is_valid': False,
                'validation_errors': [str(e)],
                'validation_warnings': [],
                'status': AgentStatus.VALIDATING
            }
    
    def _validate_against_schema(self, sql: str, state: ConversationState) -> list:
        """Validate SQL against database schema"""
        errors = []
        sql_lower = sql.lower()
        
        # Extract table names from SQL
        from_match = re.search(r'from\s+(\w+)', sql_lower)
        join_matches = re.findall(r'join\s+(\w+)', sql_lower)
        
        mentioned_tables = []
        if from_match:
            mentioned_tables.append(from_match.group(1))
        mentioned_tables.extend(join_matches)
        
        # Check if tables exist
        available_tables = [t.lower() for t in state.get('available_tables', [])]
        for table in mentioned_tables:
            if table not in available_tables:
                errors.append(f"Table '{table}' does not exist in schema")
        
        return errors
    
    def _validate_against_intent(self, sql: str, state: ConversationState) -> list:
        """Validate SQL matches user intent"""
        warnings = []
        intent = state.get('query_intent', {})
        sql_lower = sql.lower()
        
        # Check for SELECT *
        if 'select *' in sql_lower and intent.get('action') != 'retrieve':
            warnings.append("Query uses SELECT * but specific columns were requested")
        
        # Check for LIMIT
        if intent.get('action') == 'top_n' and 'limit' not in sql_lower:
            warnings.append(f"Query should include LIMIT {intent.get('limit', 10)}")
        
        # Check for COUNT
        if intent.get('action') == 'count' and 'count(' not in sql_lower:
            warnings.append("Query should use COUNT() for counting")
        
        # Check for ORDER BY with top_n
        if intent.get('action') == 'top_n' and 'order by' not in sql_lower:
            warnings.append("Query should include ORDER BY for top N results")
        
        return warnings


class RefineSQLNode:
    """Refine SQL based on validation feedback"""
    
    def __init__(self, sql_generator: SQLGenerator):
        self.sql_generator = sql_generator
    
    def __call__(self, state: ConversationState) -> Dict[str, Any]:
        """
        Refine SQL based on feedback.
        
        Args:
            state: Current conversation state
            
        Returns:
            Updated state with refined SQL
        """
        try:
            feedback = self._build_refinement_feedback(state)
            
            refined_sql = self.sql_generator.refine_query(
                original_query=state['generated_sql'],
                feedback=feedback,
                natural_language_query=state['user_query']
            )
            
            return {
                'generated_sql': refined_sql,
                'refinement_feedback': feedback,
                'needs_refinement': False,
                'generation_attempt': state['generation_attempt'] + 1,
                'status': AgentStatus.REFINING
            }
            
        except Exception as e:
            logger.error(f"Refinement error: {str(e)}")
            return {
                'error_message': f"Failed to refine SQL: {str(e)}",
                'status': AgentStatus.FAILED
            }
    
    def _build_refinement_feedback(self, state: ConversationState) -> str:
        """Build feedback message for refinement"""
        feedback_parts = ["Issues found with the query:"]
        
        errors = state.get('validation_errors', [])
        warnings = state.get('validation_warnings', [])
        
        if errors:
            feedback_parts.append("\nErrors:")
            for error in errors:
                feedback_parts.append(f"  - {error}")
        
        if warnings:
            feedback_parts.append("\nWarnings:")
            for warning in warnings:
                feedback_parts.append(f"  - {warning}")
        
        feedback_parts.append("\nPlease fix these issues and regenerate the SQL.")
        
        return "\n".join(feedback_parts)


class ExecuteSQLNode:
    """Execute validated SQL query"""
    
    def __init__(self, executor: QueryExecutor):
        self.executor = executor
    
    def __call__(self, state: ConversationState) -> Dict[str, Any]:
        """
        Execute SQL query against database.
        
        Args:
            state: Current conversation state
            
        Returns:
            Updated state with execution results
        """
        try:
            result = self.executor.execute(
                state['generated_sql'],
                format_type='dict'
            )
            
            if result.get('success'):
                return {
                    'query_results': result['data'],
                    'execution_time': result['execution_time'],
                    'row_count': result['row_count'],
                    'execution_error': None,
                    'status': AgentStatus.EXECUTING
                }
            else:
                return {
                    'query_results': None,
                    'execution_error': result.get('error', 'Unknown execution error'),
                    'status': AgentStatus.FAILED
                }
                
        except Exception as e:
            logger.error(f"Execution error: {str(e)}")
            return {
                'query_results': None,
                'execution_error': str(e),
                'status': AgentStatus.FAILED
            }


class FormatResponseNode:
    """Format results into conversational response"""
    
    def __call__(self, state: ConversationState) -> Dict[str, Any]:
        """
        Format query results for user.
        
        Args:
            state: Current conversation state
            
        Returns:
            Updated state with formatted response
        """
        results = state.get('query_results', [])
        intent = state.get('query_intent', {})
        
        if not results:
            formatted = "No results found for your query."
        else:
            formatted = self._format_based_on_intent(results, intent, state)
        
        # Add summary
        summary = self._create_summary(results, state)
        
        return {
            'formatted_response': formatted,
            'result_summary': summary,
            'status': AgentStatus.SUCCESS
        }
    
    def _format_based_on_intent(self, results: list, intent: dict, state: ConversationState) -> str:
        """Format results based on query intent"""
        action = intent.get('action', 'retrieve')
        
        if action == 'count':
            # Single count value
            count = results[0].get('count', len(results))
            return f"Count: {count}"
        
        elif action == 'top_n':
            # Top N results
            limit = intent.get('limit', len(results))
            formatted_lines = [f"Top {limit} results:"]
            for i, row in enumerate(results[:limit], 1):
                # Only show requested columns
                row_str = ', '.join([f"{k}: {v}" for k, v in row.items()])
                formatted_lines.append(f"{i}. {row_str}")
            return '\n'.join(formatted_lines)
        
        elif action == 'compare':
            # Comparison results - show only relevant columns
            formatted_lines = ["Comparison:"]
            for row in results:
                row_str = ' | '.join([f"{k}: {v}" for k, v in row.items()])
                formatted_lines.append(f"  {row_str}")
            return '\n'.join(formatted_lines)
        
        else:
            # General list
            formatted_lines = []
            for i, row in enumerate(results[:20], 1):  # Limit display to 20
                row_str = ', '.join([f"{k}: {v}" for k, v in row.items()])
                formatted_lines.append(f"{i}. {row_str}")
            
            if len(results) > 20:
                formatted_lines.append(f"\n... and {len(results) - 20} more results")
            
            return '\n'.join(formatted_lines)
    
    def _create_summary(self, results: list, state: ConversationState) -> str:
        """Create a summary of results"""
        row_count = len(results)
        exec_time = state.get('execution_time', 0)
        
        summary = f"Retrieved {row_count} row(s) in {exec_time:.2f}s"
        return summary