"""
Natural Language to SQL Conversational Agent
Power BI-style chatbot with multi-agent architecture.
"""

import sys
import logging
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown
from prompt_toolkit import PromptSession
from prompt_toolkit.history import InMemoryHistory

from config.settings import Settings
from database.connection import DatabaseConnection
from database.schema_manager import SchemaManager
from agent.graph import NLToSQLGraph
from agent.sql_generator import SQLGenerator
from agent.validator import QueryValidator
from agent.query_executor import QueryExecutor
from agent.conversation_manager import ConversationManager
from utils.helpers import QueryCache

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ConversationalNLToSQLAgent:
    """
    Conversational Natural Language to SQL Agent.
    Uses multi-agent graph architecture for robust query processing.
    """
    
    def __init__(self):
        """Initialize the conversational agent"""
        self.console = Console()
        self.settings = Settings()
        
        self.console.print("\n[bold blue]🚀 Initializing NL-to-SQL Agent...[/bold blue]\n")
        
        # Initialize components
        self._init_database()
        self._init_agent_components()
        self._init_graph()
        
        # Initialize conversation manager
        self.conversation_manager = ConversationManager(max_history=10)
        self.current_session = None
        
        # Initialize cache
        self.cache = QueryCache(maxsize=50)
        
        self.console.print("\n[bold green]✓ Agent initialized successfully![/bold green]\n")
    
    def _init_database(self):
        """Initialize database connection and schema"""
        try:
            self.console.print("📊 Connecting to database...", end=" ")
            self.db_connection = DatabaseConnection(self.settings)
            
            # Test connection
            if not self.db_connection.test_connection():
                raise Exception("Database connection failed")
            
            self.console.print("[green]✓[/green]")
            
            # Load schema
            self.console.print("📋 Loading database schema...", end=" ")
            schema_dict = self.db_connection.get_schema_info()
            self.schema_manager = SchemaManager(schema_dict)
            self.console.print("[green]✓[/green]")
            
            self.console.print(f"[dim]   Connected to: {self.settings.db_name}[/dim]")
            self.console.print(f"[dim]   Tables found: {len(self.schema_manager.get_table_names())}[/dim]")
            
        except Exception as e:
            self.console.print("[red]✗[/red]")
            self.console.print(f"[red]✗ Database initialization failed: {str(e)}[/red]")
            raise
    
    def _init_agent_components(self):
        """Initialize agent components"""
        try:
            self.console.print("🤖 Initializing AI components...", end=" ")
            
            # SQL Generator
            schema_context = self.schema_manager.get_schema_context()
            self.sql_generator = SQLGenerator(
                api_key=self.settings.google_api_key,
                schema_context=schema_context
            )
            
            # Validator
            self.validator = QueryValidator()
            
            # Executor
            self.executor = QueryExecutor(self.db_connection)
            
            self.console.print("[green]✓[/green]")
            self.console.print("[dim]   Using: Google Gemini 2.0 Flash Experimental[/dim]")
            
        except Exception as e:
            self.console.print("[red]✗[/red]")
            self.console.print(f"[red]✗ Component initialization failed: {str(e)}[/red]")
            raise
    
    def _init_graph(self):
        """Initialize the agent graph"""
        try:
            self.console.print("🔗 Building multi-agent graph...", end=" ")
            
            self.graph = NLToSQLGraph(
                sql_generator=self.sql_generator,
                validator=self.validator,
                executor=self.executor,
                schema_manager=self.schema_manager
            )
            
            self.console.print("[green]✓[/green]")
            self.console.print("[dim]   Pipeline: Understand → Generate → Validate → Execute → Format[/dim]")
            
        except Exception as e:
            self.console.print("[red]✗[/red]")
            self.console.print(f"[red]✗ Graph initialization failed: {str(e)}[/red]")
            raise
    
    def start_session(self) -> str:
        """
        Start a new conversation session.
        
        Returns:
            Session ID
        """
        session_id = self.conversation_manager.create_session()
        self.current_session = session_id
        return session_id
    
    def process_query(
        self,
        user_query: str,
        session_id: Optional[str] = None,
        show_sql: bool = False
    ) -> dict:
        """
        Process a natural language query.
        
        Args:
            user_query: Natural language query
            session_id: Session identifier (creates new if None)
            show_sql: Whether to include SQL in response
            
        Returns:
            Result dictionary with formatted response
        """
        # Create session if needed
        if not session_id:
            session_id = self.start_session()
        
        # Sanitize input
        user_query = user_query.strip()
        
        if not user_query:
            return {'formatted_response': 'Please enter a query.', 'exit': False}
        
        # Check for special commands
        if user_query.lower() in ['quit', 'exit', 'bye', 'q']:
            return {'formatted_response': '👋 Goodbye! Have a great day!', 'exit': True}
        
        if user_query.lower() == 'schema':
            schema_info = self.schema_manager.get_schema_context()
            return {'formatted_response': schema_info, 'exit': False}
        
        if user_query.lower() == 'clear':
            self.cache.clear()
            if session_id:
                self.conversation_manager.clear_session(session_id)
            return {'formatted_response': '🧹 Cache and conversation history cleared!', 'exit': False}
        
        if user_query.lower() in ['help', '?']:
            return {'formatted_response': self._get_help_text(), 'exit': False}
        
        if user_query.lower() == 'tables':
            tables = self.schema_manager.get_table_names()
            tables_list = "\n".join([f"  • {table}" for table in tables])
            return {
                'formatted_response': f"📋 Available Tables:\n{tables_list}",
                'exit': False
            }
        
        try:
            # Check cache
            cache_key = f"{session_id}:{user_query.lower()}"
            cached_result = self.cache.get(cache_key)
            if cached_result:
                self.console.print("[dim]💾 Retrieved from cache[/dim]")
                return cached_result
            
            # Resolve references using conversation context
            resolved_query = self.conversation_manager.resolve_references(
                session_id, user_query
            )
            
            if resolved_query != user_query:
                self.console.print(f"[dim]🔄 Resolved to: {resolved_query}[/dim]")
            
            # Get conversation history
            history = self.conversation_manager.get_history(session_id)
            
            # Execute graph
            self.console.print("[dim]🤔 Processing your query...[/dim]")
            final_state = self.graph.invoke(
                user_query=resolved_query,
                conversation_history=history,
                session_id=session_id
            )
            
            # Check for errors
            if final_state.get('error_message'):
                error_msg = final_state['error_message']
                response = f"❌ I encountered an error:\n{error_msg}\n\n💡 Please try rephrasing your question or check 'help' for examples."
            else:
                # Format response
                response = self.conversation_manager.format_conversational_response(
                    final_state,
                    include_sql=show_sql
                )
            
            # Update conversation history
            self.conversation_manager.add_message(
                session_id=session_id,
                role='user',
                content=user_query
            )
            
            self.conversation_manager.add_message(
                session_id=session_id,
                role='assistant',
                content=response,
                metadata={
                    'sql': final_state.get('generated_sql'),
                    'execution_time': final_state.get('execution_time'),
                    'row_count': final_state.get('row_count')
                }
            )
            
            # Update context
            entities = final_state.get('detected_entities', {})
            intent = final_state.get('query_intent', {})
            self.conversation_manager.update_context(
                session_id=session_id,
                tables=entities.get('tables', []),
                columns=entities.get('columns', []),
                intent=intent.get('action')
            )
            
            # Cache result
            result = {
                'formatted_response': response,
                'state': final_state,
                'exit': False
            }
            self.cache.set(cache_key, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Query processing error: {str(e)}")
            error_response = f"❌ I encountered an error: {str(e)}\n\n💡 Please try rephrasing your question."
            return {'formatted_response': error_response, 'exit': False}
    
    def run_interactive(self):
        """Run interactive CLI mode"""
        self.console.print(Panel.fit(
            "[bold blue]💬 Natural Language to SQL Chatbot[/bold blue]\n"
            "[dim]Conversational Power BI-style Agent[/dim]\n\n"
            "Ask questions in plain English about your database.\n"
            "Type [bold]'help'[/bold] for commands, [bold]'quit'[/bold] to exit.",
            border_style="blue",
            padding=(1, 2)
        ))
        
        # Start session
        session_id = self.start_session()
        self.console.print(f"[dim]📝 Session: {session_id[:8]}...[/dim]")
        
        # Show available tables
        tables = self.schema_manager.get_table_names()
        self.console.print(f"[dim]📊 Database has {len(tables)} tables[/dim]\n")
        
        # Create prompt session with history
        session = PromptSession(history=InMemoryHistory())
        
        # Show example
        self.console.print("[dim]💡 Try: 'Show me all customers' or 'Top 5 products by price'[/dim]\n")
        
        while True:
            try:
                # Get user input
                user_input = session.prompt("\n💬 You: ", default="")
                
                if not user_input.strip():
                    continue
                
                # Process query
                result = self.process_query(
                    user_input,
                    session_id=session_id,
                    show_sql=False  # Toggle with 'show sql' command
                )
                
                # Display response
                self.console.print(f"\n🤖 Assistant:\n{result['formatted_response']}")
                
                # Check for exit
                if result.get('exit'):
                    break
                    
            except KeyboardInterrupt:
                self.console.print("\n[yellow]⚠️  Interrupted. Type 'quit' to exit.[/yellow]")
                continue
            except EOFError:
                break
            except Exception as e:
                self.console.print(f"\n[red]❌ Error: {str(e)}[/red]")
                logger.exception("Interactive mode error")
        
        self.console.print("\n[blue]👋 Thank you for using NL-to-SQL Agent! Goodbye![/blue]\n")
    
    def run_single_query(self, query: str, show_sql: bool = True):
        """
        Run a single query and exit.
        
        Args:
            query: Natural language query
            show_sql: Whether to show SQL query
        """
        session_id = self.start_session()
        result = self.process_query(query, session_id=session_id, show_sql=show_sql)
        
        self.console.print("\n" + "="*70)
        self.console.print(result['formatted_response'])
        self.console.print("="*70 + "\n")
    
    def _get_help_text(self) -> str:
        """Get help text"""
        return """
📚 Available Commands:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • help / ?    - Show this help message
  • schema      - Display full database schema
  • tables      - List all available tables
  • clear       - Clear cache and conversation history
  • quit / exit - Exit the application

💡 Example Queries:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Simple Queries:
    • "Show me all customers"
    • "List all products"
    • "Display orders"
  
  Aggregations:
    • "How many customers are there?"
    • "What's the total sales amount?"
    • "Average product price by category"
  
  Top N Queries:
    • "Top 5 most expensive products"
    • "Show me the 10 latest orders"
    • "Best selling products"
  
  Comparisons:
    • "Compare product prices"
    • "Show customer names and cities"
    • "List employees with their titles"
  
  Counts:
    • "Count orders from last month"
    • "How many products in each category?"
    • "Number of customers per country"

🎯 Pro Tips:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ✓ Ask naturally - like talking to a colleague
  ✓ Use follow-ups - "them", "it", "those" work!
  ✓ Be specific - "customer names" gets just names
  ✓ The agent learns - it remembers recent context
  ✓ Get precise data - no unnecessary columns

🔍 Smart Features:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Auto-retry on errors (up to 3 attempts)
  • Schema-aware validation
  • Conversation context memory
  • Smart column selection
  • Results caching
"""


def main():
    """Main entry point"""
    try:
        agent = ConversationalNLToSQLAgent()
        
        # Check command line arguments
        if len(sys.argv) > 1:
            # Check for flags
            show_sql = '--sql' in sys.argv or '-s' in sys.argv
            
            # Remove flags from query
            query_parts = [arg for arg in sys.argv[1:] if arg not in ['--sql', '-s']]
            
            if query_parts:
                query = ' '.join(query_parts)
                agent.run_single_query(query, show_sql=show_sql)
            else:
                agent.run_interactive()
        else:
            # Interactive mode
            agent.run_interactive()
            
    except KeyboardInterrupt:
        print("\n\n👋 Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {str(e)}")
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()