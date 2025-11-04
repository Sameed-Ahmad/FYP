"""
Conversation Manager
Handles conversation context, history, and multi-turn interactions.
"""

from typing import List, Dict, Optional
from datetime import datetime
import uuid


class ConversationManager:
    """
    Manages conversation state and history for Power BI-style chatbot.
    """
    
    def __init__(self, max_history: int = 10):
        """
        Initialize conversation manager.
        
        Args:
            max_history: Maximum number of conversation turns to keep
        """
        self.max_history = max_history
        self.sessions: Dict[str, List[Dict]] = {}
        self.context_memory: Dict[str, Dict] = {}
    
    def create_session(self) -> str:
        """
        Create a new conversation session.
        
        Returns:
            Session ID
        """
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = []
        self.context_memory[session_id] = {
            'last_tables': [],
            'last_columns': [],
            'last_intent': None,
            'created_at': datetime.now().isoformat()
        }
        return session_id
    
    def add_message(
        self,
        session_id: str,
        role: str,
        content: str,
        metadata: Optional[Dict] = None
    ):
        """
        Add a message to conversation history.
        
        Args:
            session_id: Session identifier
            role: Message role ('user' or 'assistant')
            content: Message content
            metadata: Optional metadata (SQL, execution time, etc.)
        """
        if session_id not in self.sessions:
            self.sessions[session_id] = []
        
        message = {
            'role': role,
            'content': content,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        self.sessions[session_id].append(message)
        
        # Keep only recent history
        if len(self.sessions[session_id]) > self.max_history * 2:  # *2 for user+assistant
            self.sessions[session_id] = self.sessions[session_id][-(self.max_history * 2):]
    
    def get_history(self, session_id: str) -> List[Dict]:
        """
        Get conversation history for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            List of conversation messages
        """
        return self.sessions.get(session_id, [])
    
    def get_context_summary(self, session_id: str) -> str:
        """
        Get a summary of recent conversation context.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Context summary string
        """
        history = self.get_history(session_id)
        
        if not history:
            return "No previous conversation."
        
        # Get last 3 exchanges
        recent = history[-6:] if len(history) >= 6 else history
        
        summary_parts = ["Recent conversation:"]
        for msg in recent:
            role = msg['role'].capitalize()
            content = msg['content'][:100] + "..." if len(msg['content']) > 100 else msg['content']
            summary_parts.append(f"{role}: {content}")
        
        return "\n".join(summary_parts)
    
    def update_context(
        self,
        session_id: str,
        tables: List[str] = None,
        columns: List[str] = None,
        intent: str = None
    ):
        """
        Update conversation context memory.
        
        Args:
            session_id: Session identifier
            tables: Tables mentioned in recent query
            columns: Columns mentioned in recent query
            intent: Intent of recent query
        """
        if session_id not in self.context_memory:
            self.context_memory[session_id] = {}
        
        if tables:
            self.context_memory[session_id]['last_tables'] = tables
        if columns:
            self.context_memory[session_id]['last_columns'] = columns
        if intent:
            self.context_memory[session_id]['last_intent'] = intent
    
    def get_context(self, session_id: str) -> Dict:
        """
        Get conversation context memory.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Context dictionary
        """
        return self.context_memory.get(session_id, {})
    
    def resolve_references(self, session_id: str, query: str) -> str:
        """
        Resolve references like "them", "it", "those" using context.
        
        Args:
            session_id: Session identifier
            query: User query with potential references
            
        Returns:
            Query with resolved references
        """
        context = self.get_context(session_id)
        query_lower = query.lower()
        
        # Reference patterns
        if any(ref in query_lower for ref in ['them', 'those', 'these']):
            if context.get('last_tables'):
                # Replace "them" with last mentioned table
                query = query.replace('them', context['last_tables'][0])
                query = query.replace('those', context['last_tables'][0])
                query = query.replace('these', context['last_tables'][0])
        
        if 'it' in query_lower and context.get('last_tables'):
            query = query.replace('it', context['last_tables'][0])
        
        return query
    
    def format_conversational_response(
        self,
        state: Dict,
        include_sql: bool = False
    ) -> str:
        """
        Format agent response in a conversational style.
        
        Args:
            state: Agent state after execution
            include_sql: Whether to include SQL in response
            
        Returns:
            Formatted conversational response
        """
        response_parts = []
        
        # Main response
        formatted_response = state.get('formatted_response', '')
        if formatted_response:
            response_parts.append(formatted_response)
        
        # Add summary
        summary = state.get('result_summary', '')
        if summary:
            response_parts.append(f"\n📊 {summary}")
        
        # Add SQL if requested
        if include_sql and state.get('generated_sql'):
            response_parts.append(f"\n🔍 SQL Query:")
            response_parts.append(f"```sql\n{state['generated_sql']}\n```")
        
        # Add explanation if available
        if state.get('sql_explanation'):
            response_parts.append(f"\n💡 {state['sql_explanation']}")
        
        # Add warnings if any
        warnings = state.get('validation_warnings', [])
        if warnings:
            response_parts.append("\n⚠️  Notes:")
            for warning in warnings:
                response_parts.append(f"  • {warning}")
        
        return "\n".join(response_parts)
    
    def clear_session(self, session_id: str):
        """
        Clear conversation history for a session.
        
        Args:
            session_id: Session identifier
        """
        if session_id in self.sessions:
            self.sessions[session_id] = []
        if session_id in self.context_memory:
            self.context_memory[session_id] = {
                'last_tables': [],
                'last_columns': [],
                'last_intent': None,
                'created_at': datetime.now().isoformat()
            }
    
    def get_all_sessions(self) -> List[str]:
        """
        Get list of all active session IDs.
        
        Returns:
            List of session IDs
        """
        return list(self.sessions.keys())
    
    def session_exists(self, session_id: str) -> bool:
        """
        Check if a session exists.
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if session exists
        """
        return session_id in self.sessions