#!/usr/bin/env python3
"""
Natural Language to SQL AI Agent
Main application entry point
"""

import sys
import logging
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt

from config.settings import settings
from database.connection import DatabaseConnection
from database.schema_manager import SchemaManager
from agent.embeddings import GeminiEmbeddings
from agent.sql_generator import SQLGenerator
from agent.query_executor import QueryExecutor
from utils.helpers import log_query, sanitize_input, QueryCache, format_error_message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

console = Console()


class NLToSQLAgent:
    """Main AI Agent class orchestrating the NL to SQL pipeline"""
    
    def __init__(self):
        console.print("[bold blue]Initializing NL to SQL Agent...[/bold blue]")
        
        try:
            # Initialize components
            self.db_connection = DatabaseConnection()
            self.query_cache = QueryCache(max_size=50)
            
            # Test database connection
            if not self.db_connection.test_connection():
                raise Exception("Database connection failed")
            
            # Get schema information
            schema_info = self.db_connection.get_schema_info()
            self.schema_manager = SchemaManager(schema_info)
            schema_context = self.schema_manager.get_schema_context()
            
            # Initialize AI components
            self.embeddings = GeminiEmbeddings()
            self.sql_generator = SQLGenerator(schema_context)
            self.query_executor = QueryExecutor(self.db_connection)
            
            console.print("[bold green]✓ Agent initialized successfully![/bold green]\n")
            
        except Exception as e:
            console.print(f"[bold red]✗ Initialization failed: {str(e)}[/bold red]")
            sys.exit(1)
    
    def process_query(self, natural_language_query: str) -> dict:
        """
        Process a natural language query through the full pipeline
        
        Args:
            natural_language_query: User's question in natural language
            
        Returns:
            Dictionary containing results and metadata
        """
        try:
            # Sanitize input
            query = sanitize_input(natural_language_query)
            
            # Check cache
            cached_result = self.query_cache.get(query)
            if cached_result:
                return cached_result
            
            # Generate SQL (silently)
            sql_info = self.sql_generator.generate_sql(query)
            
            # Execute query (silently)
            results = self.query_executor.execute(sql_info["sql"])
            
            # Combine results
            output = {
                **results,
                "sql": sql_info["sql"],
                "explanation": sql_info["explanation"],
                "original_query": query
            }
            
            # Cache successful results
            if output["success"]:
                self.query_cache.set(query, output)
            
            # Log query (only if logging enabled in settings)
            if settings.enable_query_logging:
                log_query(query, output)
            
            return output
            
        except Exception as e:
            logger.error(f"Query processing failed: {str(e)}")
            return {
                "success": False,
                "error": format_error_message(e),
                "original_query": natural_language_query
            }
    
    def display_results(self, results: dict):
        """Display query results in a formatted way"""
        
        if not results["success"]:
            console.print(Panel(
                f"[bold red]Error:[/bold red] {results['error']}",
                title="Query Failed",
                border_style="red"
            ))
            return
        
        # Display SQL query
        console.print(Panel(
            results["sql"],
            title="Generated SQL",
            border_style="blue"
        ))
        
        # Display explanation
        if results.get("explanation"):
            console.print(Panel(
                results["explanation"],
                title="Query Explanation",
                border_style="cyan"
            ))
        
        # Display metadata
        metadata = results.get("metadata", {})
        console.print(f"\n[dim]Rows: {metadata.get('row_count', 0)} | "
                     f"Time: {metadata.get('execution_time', 'N/A')} | "
                     f"Complexity: {metadata.get('complexity', 'N/A')}[/dim]\n")
        
        # Display results in table
        if results.get("results"):
            self._display_table(results["results"])
        else:
            console.print("[yellow]No results found[/yellow]")
    
    def _display_table(self, data: list):
        """Display data as a rich table"""
        if not data:
            return
        
        table = Table(show_header=True, header_style="bold magenta")
        
        # Add columns
        columns = list(data[0].keys())
        for col in columns:
            table.add_column(col)
        
        # Add rows (limit to first 20 for display)
        for row in data[:20]:
            table.add_row(*[str(row.get(col, "")) for col in columns])
        
        if len(data) > 20:
            console.print(f"[dim]Showing first 20 of {len(data)} rows[/dim]")
        
        console.print(table)
    
    def show_schema(self):
        """Display database schema"""
        console.print(Panel(
            self.schema_manager.get_schema_context(),
            title="Database Schema",
            border_style="green"
        ))
    
    def interactive_mode(self):
        """Run agent in interactive CLI mode"""
        console.print(Panel.fit(
            "[bold cyan]Natural Language to SQL AI Agent[/bold cyan]\n"
            "Ask questions about your data in plain English!\n\n"
            "Commands:\n"
            "  • Type your question to query the database\n"
            "  • 'schema' - Show database schema\n"
            "  • 'clear' - Clear query cache\n"
            "  • 'quit' or 'exit' - Exit the application",
            border_style="cyan"
        ))
        
        while True:
            try:
                query = Prompt.ask("\n[bold green]Your question[/bold green]")
                
                if not query:
                    continue
                
                query_lower = query.lower().strip()
                
                if query_lower in ['quit', 'exit', 'q']:
                    console.print("[yellow]Goodbye![/yellow]")
                    break
                
                elif query_lower == 'schema':
                    self.show_schema()
                
                elif query_lower == 'clear':
                    self.query_cache.clear()
                    console.print("[green]Cache cleared![/green]")
                
                else:
                    results = self.process_query(query)
                    self.display_results(results)
                    
            except KeyboardInterrupt:
                console.print("\n[yellow]Interrupted. Type 'quit' to exit.[/yellow]")
            except Exception as e:
                console.print(f"[red]Error: {str(e)}[/red]")


def main():
    """Main entry point"""
    agent = NLToSQLAgent()
    
    # Check if query provided as command line argument
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        results = agent.process_query(query)
        agent.display_results(results)
    else:
        # Interactive mode
        agent.interactive_mode()


if __name__ == "__main__":
    main()