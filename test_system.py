#!/usr/bin/env python3
"""
System Test Script
Tests all components of the NL to SQL Agent (FREE Gemini version)
"""

import sys
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def test_imports():
    """Test if all required modules can be imported"""
    console.print("\n[bold cyan]Testing Imports...[/bold cyan]")
    
    tests = {
        "config.settings": "Configuration module",
        "database.connection": "Database connection",
        "database.schema_manager": "Schema manager",
        "agent.embeddings": "Gemini embeddings",
        "agent.sql_generator": "SQL generator",
        "agent.query_executor": "Query executor",
        "agent.validator": "Query validator",
        "utils.helpers": "Utility helpers"
    }
    
    results = []
    all_passed = True
    
    for module, description in tests.items():
        try:
            __import__(module)
            results.append((description, "✓ Pass", "green"))
        except Exception as e:
            results.append((description, f"✗ Fail: {str(e)}", "red"))
            all_passed = False
    
    # Display results
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="white")
    
    for desc, status, color in results:
        table.add_row(desc, f"[{color}]{status}[/{color}]")
    
    console.print(table)
    return all_passed


def test_environment():
    """Test environment configuration"""
    console.print("\n[bold cyan]Testing Environment Configuration...[/bold cyan]")
    
    from config.settings import settings
    
    checks = {
        "Google API Key": settings.google_api_key[:10] + "..." if settings.google_api_key else None,
        "Database Host": settings.db_host,
        "Database Name": settings.db_name,
        "Database User": settings.db_user
    }
    
    results = []
    all_passed = True
    
    for name, value in checks.items():
        if value and value != "...":
            results.append((name, "✓ Configured", "green"))
        else:
            results.append((name, "✗ Not configured", "red"))
            all_passed = False
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Setting", style="cyan")
    table.add_column("Status", style="white")
    
    for name, status, color in results:
        table.add_row(name, f"[{color}]{status}[/{color}]")
    
    console.print(table)
    return all_passed


def test_database_connection():
    """Test database connectivity"""
    console.print("\n[bold cyan]Testing Database Connection...[/bold cyan]")
    
    try:
        from database.connection import DatabaseConnection
        
        db = DatabaseConnection()
        
        # Test connection
        if not db.test_connection():
            console.print("[red]✗ Database connection failed[/red]")
            return False
        
        console.print("[green]✓ Database connected successfully[/green]")
        
        # Get schema info
        schema_info = db.get_schema_info()
        console.print(f"[green]✓ Found {len(schema_info)} tables[/green]")
        
        # List tables
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Table Name", style="cyan")
        table.add_column("Columns", style="white")
        
        for table_name, info in schema_info.items():
            col_count = len(info['columns'])
            table.add_row(table_name, str(col_count))
        
        console.print(table)
        
        return True
        
    except Exception as e:
        console.print(f"[red]✗ Database test failed: {str(e)}[/red]")
        return False


def test_google_gemini():
    """Test Google Gemini (both text generation and embeddings)"""
    console.print("\n[bold cyan]Testing Google Gemini...[/bold cyan]")
    
    text_gen_passed = False
    embeddings_passed = False
    
    # Test 1: Text generation with Gemini 2.0 Flash
    try:
        import google.generativeai as genai
        from config.settings import settings
        
        genai.configure(api_key=settings.google_api_key)
        
        console.print("[yellow]Testing text generation (Gemini 2.0 Flash)...[/yellow]")
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        response = model.generate_content(
            "Say 'test successful'",
            generation_config=genai.types.GenerationConfig(
                temperature=0.1,
                max_output_tokens=20
            )
        )
        console.print("[green]✓ Gemini 2.0 Flash text generation working[/green]")
        console.print(f"[dim]Response: {response.text[:50]}[/dim]")
        text_gen_passed = True
        
    except Exception as e:
        console.print(f"[red]✗ Text generation failed: {str(e)}[/red]")
        console.print("[yellow]Trying fallback model (gemini-1.5-flash)...[/yellow]")
        
        # Try fallback model
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = model.generate_content("Say 'test successful'")
            console.print("[green]✓ Gemini 1.5 Flash working (fallback)[/green]")
            text_gen_passed = True
        except Exception as e2:
            console.print(f"[red]✗ Fallback also failed: {str(e2)}[/red]")
    
    # Test 2: Embeddings
    try:
        console.print("[yellow]Testing embeddings...[/yellow]")
        result = genai.embed_content(
            model="models/text-embedding-004",
            content="test embedding"
        )
        
        embedding_dim = len(result['embedding'])
        console.print("[green]✓ Gemini embeddings working[/green]")
        console.print(f"[dim]Embedding dimension: {embedding_dim}[/dim]")
        embeddings_passed = True
        
    except Exception as e:
        console.print(f"[red]✗ Embeddings failed: {str(e)}[/red]")
        console.print("[yellow]Note: Embeddings are optional for basic functionality[/yellow]")
    
    # Overall result
    if not text_gen_passed:
        console.print("[red]✗ Text generation is required and failed[/red]")
        console.print("[yellow]Check your API key at: https://makersuite.google.com/app/apikey[/yellow]")
        console.print("[yellow]Make sure it's correctly set in .env file[/yellow]")
        return False
    
    if not embeddings_passed:
        console.print("[yellow]⚠ Embeddings failed but text generation works[/yellow]")
        console.print("[yellow]System will work but without semantic search features[/yellow]")
    
    return text_gen_passed  # Only require text generation to pass


def test_sql_generation():
    """Test SQL generation"""
    console.print("\n[bold cyan]Testing SQL Generation...[/bold cyan]")
    
    try:
        from database.connection import DatabaseConnection
        from database.schema_manager import SchemaManager
        from agent.sql_generator import SQLGenerator
        
        # Get schema
        db = DatabaseConnection()
        schema_info = db.get_schema_info()
        schema_manager = SchemaManager(schema_info)
        schema_context = schema_manager.get_schema_context()
        
        # Initialize SQL generator
        sql_gen = SQLGenerator(schema_context)
        
        # Test query
        test_query = "Show me all data from the first table"
        console.print(f"[yellow]Testing query: '{test_query}'[/yellow]")
        result = sql_gen.generate_sql(test_query)
        
        console.print("[green]✓ SQL generation successful[/green]")
        console.print(f"[dim]Generated SQL:[/dim]")
        console.print(Panel(result["sql"], border_style="blue"))
        
        return True
        
    except Exception as e:
        console.print(f"[red]✗ SQL generation test failed: {str(e)}[/red]")
        return False


def test_query_validation():
    """Test query validation"""
    console.print("\n[bold cyan]Testing Query Validation...[/bold cyan]")
    
    try:
        from agent.validator import QueryValidator
        
        validator = QueryValidator()
        
        test_cases = [
            ("SELECT * FROM products", True, "Valid SELECT query"),
            ("SELECT * FROM products WHERE id = 1", True, "SELECT with WHERE"),
            ("SELECT COUNT(*) FROM orders", True, "SELECT with COUNT"),
            ("DROP TABLE products", False, "Dangerous DROP statement"),
            ("DELETE FROM users", False, "Dangerous DELETE statement"),
            ("INSERT INTO products VALUES (1)", False, "Dangerous INSERT statement"),
            ("UPDATE products SET price = 0", False, "Dangerous UPDATE statement"),
        ]
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Test Case", style="cyan", width=40)
        table.add_column("Expected", style="white", width=10)
        table.add_column("Result", style="white", width=15)
        
        all_passed = True
        for query, should_pass, description in test_cases:
            try:
                is_valid, error_msg = validator.validate(query)
                passed = (is_valid == should_pass)
                
                if not passed:
                    all_passed = False
                    console.print(f"[red]Failed test: {description}[/red]")
                    console.print(f"[red]Query: {query}[/red]")
                    console.print(f"[red]Expected: {should_pass}, Got: {is_valid}[/red]")
                    if error_msg:
                        console.print(f"[red]Error: {error_msg}[/red]")
                
                result_color = "green" if passed else "red"
                result_text = "✓ Pass" if passed else "✗ Fail"
                expected_text = "Valid" if should_pass else "Invalid"
                
                table.add_row(
                    description,
                    expected_text,
                    f"[{result_color}]{result_text}[/{result_color}]"
                )
            except Exception as e:
                console.print(f"[red]Exception in test '{description}': {str(e)}[/red]")
                all_passed = False
                table.add_row(
                    description,
                    "Valid" if should_pass else "Invalid",
                    f"[red]✗ Error: {str(e)[:20]}[/red]"
                )
        
        console.print(table)
        
        if all_passed:
            console.print("[green]✓ All validation tests passed[/green]")
        else:
            console.print("[red]✗ Some validation tests failed[/red]")
        
        return all_passed
        
    except Exception as e:
        console.print(f"[red]✗ Validation test failed: {str(e)}[/red]")
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")
        return False


def test_end_to_end():
    """Test complete end-to-end query"""
    console.print("\n[bold cyan]Testing End-to-End Query Execution...[/bold cyan]")
    
    try:
        from app import NLToSQLAgent
        
        console.print("[yellow]Initializing agent...[/yellow]")
        agent = NLToSQLAgent()
        
        # Test query
        test_query = "How many tables are in the database?"
        console.print(f"[yellow]Executing: '{test_query}'[/yellow]")
        
        result = agent.process_query(test_query)
        
        if result["success"]:
            console.print("[green]✓ End-to-end test successful[/green]")
            console.print(f"[dim]SQL: {result['sql']}[/dim]")
            console.print(f"[dim]Results: {len(result.get('results', []))} rows[/dim]")
            return True
        else:
            console.print(f"[red]✗ Query failed: {result.get('error', 'Unknown error')}[/red]")
            return False
        
    except Exception as e:
        console.print(f"[red]✗ End-to-end test failed: {str(e)}[/red]")
        return False


def main():
    """Run all tests"""
    console.print(Panel.fit(
        "[bold cyan]NL to SQL Agent - System Test[/bold cyan]\n"
        "[green]100% FREE Version with Google Gemini[/green]\n"
        "Running comprehensive system tests...",
        border_style="cyan"
    ))
    
    tests = [
        ("Imports", test_imports),
        ("Environment", test_environment),
        ("Database", test_database_connection),
        ("Google Gemini", test_google_gemini),
        ("SQL Generation", test_sql_generation),
        ("Query Validation", test_query_validation),
        ("End-to-End", test_end_to_end)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            console.print(f"[red]✗ {test_name} test crashed: {str(e)}[/red]")
            results[test_name] = False
    
    # Summary
    console.print("\n" + "="*60)
    console.print("[bold cyan]Test Summary[/bold cyan]")
    console.print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "[green]✓ PASS[/green]" if passed_test else "[red]✗ FAIL[/red]"
        console.print(f"{test_name:.<40} {status}")
    
    console.print("="*60)
    console.print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        console.print("\n[bold green]🎉 All tests passed! System is ready to use.[/bold green]")
        console.print("[dim]Run: python app.py[/dim]")
        return 0
    else:
        console.print(f"\n[bold red]⚠ {total - passed} test(s) failed. Please review errors above.[/bold red]")
        return 1


if __name__ == "__main__":
    sys.exit(main())