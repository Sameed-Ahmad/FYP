"""
Test the multi-agent graph system
"""

from app import ConversationalNLToSQLAgent


def test_graph():
    """Test graph execution"""
    print("Initializing agent...")
    agent = ConversationalNLToSQLAgent()
    
    print("\n" + "="*60)
    print("Testing Multi-Agent Graph System")
    print("="*60 + "\n")
    
    # Test queries
    test_queries = [
        "Show me all customers",
        "Get the top 5 most expensive products",
        "How many orders are there?",
        "Show me customer names from London"
    ]
    
    session_id = agent.start_session()
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: {query}")
        print("-" * 60)
        
        result = agent.process_query(query, session_id=session_id, show_sql=True)
        print(result['formatted_response'])
        print("\n")
    
    print("="*60)
    print("All tests completed!")
    print("="*60)


if __name__ == "__main__":
    test_graph()