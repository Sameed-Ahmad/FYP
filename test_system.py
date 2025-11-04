"""
Comprehensive System Testing
Tests all components of the multi-agent NL-to-SQL system.
"""

import sys
import os
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn


console = Console()


def print_test_header(test_name: str):
    """Print test section header"""
    console.print(f"\n[bold blue]{'='*70}[/bold blue]")
    console.print(f"[bold blue]{test_name}[/bold blue]")
    console.print(f"[bold blue]{'='*70}[/bold blue]\n")


def print_success(message: str):
    """Print success message"""
    console.print(f"[green]✓ {message}[/green]")


def print_error(message: str):
    """Print error message"""
    console.print(f"[red]✗ {message}[/red]")


def print_info(message: str):
    """Print info message"""
    console.print(f"[dim]  {message}[/dim]")


def test_imports():
    """Test 1: Import all required modules"""
    print_test_header("TEST 1: Module Imports")
    
    errors = []
    
    try:
        print_info("Checking core imports...")
        import google.generativeai as genai
        print_success("google-generativeai imported")
    except ImportError as e:
        errors.append(f"google-generativeai: {str(e)}")
        print_error(f"google-generativeai: {str(e)}")
    
    try:
        import langgraph
        from langgraph.graph import StateGraph, END
        print_success("langgraph imported")
    except ImportError as e:
        errors.append(f"langgraph: {str(e)}")
        print_error(f"langgraph: {str(e)}")
    
    try:
        from sqlalchemy import create_engine
        print_success("sqlalchemy imported")
    except ImportError as e:
        errors.append(f"sqlalchemy: {str(e)}")
        print_error(f"sqlalchemy: {str(e)}")
    
    try:
        import psycopg2
        print_success("psycopg2 imported")
    except ImportError as e:
        errors.append(f"psycopg2: {str(e)}")
        print_error(f"psycopg2: {str(e)}")
    
    try:
        from rich.console import Console
        from prompt_toolkit import PromptSession
        print_success("rich and prompt_toolkit imported")
    except ImportError as e:
        errors.append(f"UI libraries: {str(e)}")
        print_error(f"UI libraries: {str(e)}")
    
    # Test project imports
    print_info("\nChecking project modules...")
    
    try:
        from config.settings import Settings
        print_success("config.settings imported")
    except ImportError as e:
        errors.append(f"config.settings: {str(e)}")
        print_error(f"config.settings: {str(e)}")
    
    try:
        from database.connection import DatabaseConnection
        from database.schema_manager import SchemaManager
        print_success("database modules imported")
    except ImportError as e:
        errors.append(f"database modules: {str(e)}")
        print_error(f"database modules: {str(e)}")
    
    try:
        from agent.sql_generator import SQLGenerator
        from agent.validator import QueryValidator
        from agent.query_executor import QueryExecutor
        print_success("agent core modules imported")
    except ImportError as e:
        errors.append(f"agent core: {str(e)}")
        print_error(f"agent core: {str(e)}")
    
    try:
        from agent.state import ConversationState, create_initial_state
        from agent.nodes import UnderstandQueryNode
        from agent.edges import should_refine_sql
        from agent.graph import NLToSQLGraph
        from agent.conversation_manager import ConversationManager
        print_success("agent graph modules imported")
    except ImportError as e:
        errors.append(f"agent graph: {str(e)}")
        print_error(f"agent graph: {str(e)}")
    
    if errors:
        print_error(f"\n{len(errors)} import error(s) found")
        return False
    else:
        print_success("\n✓ All imports successful!")
        return True


def test_environment():
    """Test 2: Environment configuration"""
    print_test_header("TEST 2: Environment Configuration")
    
    try:
        from config.settings import Settings
        settings = Settings()
        
        # Check required variables
        if settings.google_api_key:
            print_success(f"GOOGLE_API_KEY is set ({settings.google_api_key[:10]}...)")
        else:
            print_error("GOOGLE_API_KEY is not set")
            return False
        
        print_success(f"DB_HOST: {settings.db_host}")
        print_success(f"DB_PORT: {settings.db_port}")
        print_success(f"DB_NAME: {settings.db_name}")
        print_success(f"DB_USER: {settings.db_user}")
        print_success(f"DB_PASSWORD: {'*' * len(settings.db_password)}")
        
        print_success("\n✓ Environment configuration valid!")
        return True
        
    except Exception as e:
        print_error(f"Environment configuration error: {str(e)}")
        return False


def test_database_connection():
    """Test 3: Database connectivity"""
    print_test_header("TEST 3: Database Connection")
    
    try:
        from config.settings import Settings
        from database.connection import DatabaseConnection
        
        settings = Settings()
        db = DatabaseConnection(settings)
        
        print_info("Testing connection...")
        if db.test_connection():
            print_success("Database connection successful")
        else:
            print_error("Database connection failed")
            return False
        
        print_info("Loading schema...")
        schema = db.get_schema_info()
        
        if schema:
            print_success(f"Schema loaded: {len(schema)} tables found")
            
            # Show tables
            table = Table(title="Available Tables")
            table.add_column("Table Name", style="cyan")
            table.add_column("Columns", style="green")
            
            for table_name, table_info in list(schema.items())[:5]:
                table.add_row(
                    table_name,
                    str(len(table_info.get('columns', [])))
                )
            
            if len(schema) > 5:
                table.add_row("...", "...")
            
            console.print(table)
            print_success("\n✓ Database connection test passed!")
            return True
        else:
            print_error("No tables found in database")
            return False
            
    except Exception as e:
        print_error(f"Database connection error: {str(e)}")
        return False


def test_gemini_api():
    """Test 4: Google Gemini API connectivity"""
    print_test_header("TEST 4: Google Gemini API")
    
    try:
        import google.generativeai as genai
        from config.settings import Settings
        
        settings = Settings()
        genai.configure(api_key=settings.google_api_key)
        
        print_info("Testing text generation...")
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        response = model.generate_content("Say 'Hello, World!' in exactly those words.")
        
        if response and response.text:
            print_success(f"Text generation works: '{response.text[:50]}...'")
        else:
            print_error("Text generation returned empty response")
            return False
        
        print_info("Testing embeddings...")
        result = genai.embed_content(
            model="models/text-embedding-004",
            content="Test embedding"
        )
        
        if result and 'embedding' in result:
            print_success(f"Embeddings work: vector dimension = {len(result['embedding'])}")
        else:
            print_error("Embeddings failed")
            return False
        
        print_success("\n✓ Gemini API test passed!")
        return True
        
    except Exception as e:
        print_error(f"Gemini API error: {str(e)}")
        print_info("Check your API key at: https://makersuite.google.com/app/apikey")
        return False


def test_sql_generation():
    """Test 5: SQL generation"""
    print_test_header("TEST 5: SQL Generation")
    
    try:
        from config.settings import Settings
        from database.connection import DatabaseConnection
        from database.schema_manager import SchemaManager
        from agent.sql_generator import SQLGenerator
        
        settings = Settings()
        db = DatabaseConnection(settings)
        schema_dict = db.get_schema_info()
        schema_manager = SchemaManager(schema_dict)
        
        schema_context = schema_manager.get_schema_context()
        generator = SQLGenerator(
            api_key=settings.google_api_key,
            schema_context=schema_context
        )
        
        test_queries = [
            "Show me all customers",
            "Count the total number of products",
            "Get the top 5 most expensive products"
        ]
        
        for query in test_queries:
            print_info(f"\nQuery: '{query}'")
            sql, explanation = generator.generate_sql(query)
            
            if sql:
                print_success(f"Generated SQL: {sql[:100]}...")
                print_info(f"Explanation: {explanation[:80]}...")
            else:
                print_error(f"Failed to generate SQL for: {query}")
                return False
        
        print_success("\n✓ SQL generation test passed!")
        return True
        
    except Exception as e:
        print_error(f"SQL generation error: {str(e)}")
        return False


def test_validation():
    """Test 6: Query validation"""
    print_test_header("TEST 6: Query Validation")
    
    try:
        from agent.validator import QueryValidator
        
        validator = QueryValidator()
        
        # Test valid query
        valid_sql = "SELECT * FROM customers WHERE city = 'London'"
        print_info(f"Testing valid query: {valid_sql}")
        
        if validator.validate(valid_sql):
            print_success("Valid query accepted")
        else:
            print_error("Valid query rejected")
            return False
        
        # Test invalid queries
        invalid_queries = [
            "DROP TABLE customers",
            "DELETE FROM orders",
            "UPDATE products SET price = 0"
        ]
        
        for sql in invalid_queries:
            print_info(f"\nTesting invalid query: {sql}")
            try:
                validator.validate(sql)
                print_error("Invalid query was accepted (should be rejected)")
                return False
            except Exception:
                print_success("Invalid query correctly rejected")
        
        print_success("\n✓ Validation test passed!")
        return True
        
    except Exception as e:
        print_error(f"Validation error: {str(e)}")
        return False


def test_graph_workflow():
    """Test 7: Multi-agent graph workflow"""
    print_test_header("TEST 7: Multi-Agent Graph Workflow")
    
    try:
        from config.settings import Settings
        from database.connection import DatabaseConnection
        from database.schema_manager import SchemaManager
        from agent.sql_generator import SQLGenerator
        from agent.validator import QueryValidator
        from agent.query_executor import QueryExecutor
        from agent.graph import NLToSQLGraph
        
        print_info("Initializing graph components...")
        
        settings = Settings()
        db = DatabaseConnection(settings)
        schema_dict = db.get_schema_info()
        schema_manager = SchemaManager(schema_dict)
        
        schema_context = schema_manager.get_schema_context()
        sql_generator = SQLGenerator(
            api_key=settings.google_api_key,
            schema_context=schema_context
        )
        
        validator = QueryValidator()
        executor = QueryExecutor(db)
        
        print_success("Components initialized")
        
        print_info("Building graph...")
        graph = NLToSQLGraph(
            sql_generator=sql_generator,
            validator=validator,
            executor=executor,
            schema_manager=schema_manager
        )
        print_success("Graph built successfully")
        
        # Test graph execution
        test_query = "Show me the first 3 customers"
        print_info(f"\nExecuting test query: '{test_query}'")
        
        result = graph.invoke(
            user_query=test_query,
            conversation_history=[],
            session_id="test-session"
        )
        
        if result.get('formatted_response'):
            print_success("Graph execution successful")
            print_info(f"Status: {result.get('status')}")
            print_info(f"SQL: {result.get('generated_sql', 'N/A')[:80]}...")
            
            if result.get('query_results'):
                print_success(f"Retrieved {len(result['query_results'])} rows")
        else:
            print_error("Graph execution produced no response")
            return False
        
        print_success("\n✓ Graph workflow test passed!")
        return True
        
    except Exception as e:
        print_error(f"Graph workflow error: {str(e)}")
        import traceback
        print_info(traceback.format_exc())
        return False


def test_end_to_end():
    """Test 8: End-to-end agent test"""
    print_test_header("TEST 8: End-to-End Agent Test")
    
    try:
        from app import ConversationalNLToSQLAgent
        
        print_info("Initializing full agent...")
        agent = ConversationalNLToSQLAgent()
        print_success("Agent initialized")
        
        # Create session
        session_id = agent.start_session()
        print_success(f"Session created: {session_id[:8]}...")
        
        # Test queries
        test_queries = [
            "Show me all customers",
            "How many products are there?",
            "Get the top 3 most expensive products"
        ]
        
        for query in test_queries:
            print_info(f"\nQuery: '{query}'")
            result = agent.process_query(query, session_id=session_id)
            
            if result.get('formatted_response'):
                print_success("Query processed successfully")
                # Show first 100 chars of response
                response_preview = result['formatted_response'][:100]
                print_info(f"Response: {response_preview}...")
            else:
                print_error(f"Query failed: {query}")
                return False
        
        print_success("\n✓ End-to-end test passed!")
        return True
        
    except Exception as e:
        print_error(f"End-to-end test error: {str(e)}")
        import traceback
        print_info(traceback.format_exc())
        return False


def run_all_tests():
    """Run all system tests"""
    console.print(Panel.fit(
        "[bold blue]🧪 NL-to-SQL Agent System Tests[/bold blue]\n"
        "[dim]Testing multi-agent graph architecture[/dim]",
        border_style="blue",
        padding=(1, 2)
    ))
    
    tests = [
        ("Module Imports", test_imports),
        ("Environment Config", test_environment),
        ("Database Connection", test_database_connection),
        ("Gemini API", test_gemini_api),
        ("SQL Generation", test_sql_generation),
        ("Query Validation", test_validation),
        ("Graph Workflow", test_graph_workflow),
        ("End-to-End", test_end_to_end)
    ]
    
    results = []
    start_time = datetime.now()
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            console.print(f"\n[red]Test '{test_name}' crashed: {str(e)}[/red]")
            results.append((test_name, False))
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Print summary
    console.print("\n" + "="*70)
    console.print("[bold]TEST SUMMARY[/bold]")
    console.print("="*70 + "\n")
    
    summary_table = Table(show_header=True, header_style="bold magenta")
    summary_table.add_column("Test", style="cyan", width=30)
    summary_table.add_column("Status", width=15)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "[green]✓ PASSED[/green]" if result else "[red]✗ FAILED[/red]"
        summary_table.add_row(test_name, status)
    
    console.print(summary_table)
    
    console.print(f"\n[bold]Results: {passed}/{total} tests passed[/bold]")
    console.print(f"[dim]Duration: {duration:.2f} seconds[/dim]\n")
    
    if passed == total:
        console.print(Panel.fit(
            "[bold green]🎉 ALL TESTS PASSED![/bold green]\n"
            "[dim]Your NL-to-SQL agent is ready to use![/dim]\n\n"
            "Run: [bold cyan]python app.py[/bold cyan]",
            border_style="green",
            padding=(1, 2)
        ))
        return 0
    else:
        console.print(Panel.fit(
            f"[bold red]❌ {total - passed} TEST(S) FAILED[/bold red]\n"
            "[dim]Please fix the errors above before using the agent.[/dim]",
            border_style="red",
            padding=(1, 2)
        ))
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())