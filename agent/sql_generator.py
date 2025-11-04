"""
Enhanced SQL Generator with Schema Intelligence
Converts natural language to SQL using Google Gemini with smart column selection.
"""

import google.generativeai as genai
import re
from typing import Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class SQLGenerator:
    """Enhanced SQL generator with intelligent column selection"""
    
    def __init__(self, api_key: str, schema_context: str):
        """
        Initialize SQL generator.
        
        Args:
            api_key: Google Gemini API key
            schema_context: Database schema information
        """
        genai.configure(api_key=api_key)
        
        # Use Gemini 2.0 Flash Experimental for best results
        self.model = genai.GenerativeModel(
            'gemini-2.0-flash-exp',
            generation_config={
                'temperature': 0.1,  # Low temperature for deterministic output
                'max_output_tokens': 1000,
            }
        )
        
        self.schema_context = schema_context
        self.system_prompt = self._build_system_prompt()
    
    def _build_system_prompt(self) -> str:
        """Build comprehensive system prompt"""
        return f"""You are an expert SQL query generator for PostgreSQL databases. Your task is to convert natural language queries into precise, efficient SQL queries.

DATABASE SCHEMA:
{self.schema_context}

CRITICAL RULES FOR COLUMN SELECTION:
1. **ONLY select columns that are explicitly requested or necessary**
2. If user asks for "names" → SELECT ONLY name columns
3. If user asks for "top N" → SELECT requested columns + ordering column
4. If user asks to "compare A and B" → SELECT ONLY A and B columns
5. AVOID SELECT * unless user explicitly asks for "all information"
6. For counts → Use COUNT(*) or COUNT(column), don't select all columns
7. For aggregations → SELECT only grouped columns and aggregate results

EXAMPLES OF CORRECT COLUMN SELECTION:
- "Show me customer names" → SELECT customer_name FROM customers
- "Top 5 expensive products" → SELECT product_name, price FROM products ORDER BY price DESC LIMIT 5
- "Compare product prices" → SELECT product_name, price FROM products
- "Count customers" → SELECT COUNT(*) as customer_count FROM customers
- "Average price by category" → SELECT category, AVG(price) as avg_price FROM products GROUP BY category

SQL GENERATION RULES:
1. Use proper JOINs with explicit ON conditions
2. Follow foreign key relationships from schema
3. Use meaningful aliases (e.g., c for customers, p for products)
4. Include ORDER BY for "top", "best", "highest", "latest" queries
5. Use LIMIT for "top N" or "first N" requests
6. Use GROUP BY for aggregations by category
7. Use HAVING for filtered aggregates
8. Handle NULL values appropriately
9. Use DISTINCT when avoiding duplicates
10. Always use exact table and column names from schema

SECURITY CONSTRAINTS:
- ONLY generate SELECT queries
- NO INSERT, UPDATE, DELETE, DROP, or any modification queries
- NO UNION or multiple statements
- NO stored procedures or dynamic SQL

OUTPUT FORMAT:
Return your response in this exact format:

```sql
[Your SQL query here]
```

Explanation: [Brief explanation of what the query does and why you selected specific columns]

Now generate a SQL query for the following request:"""
    
    def generate_sql(
        self,
        natural_language_query: str,
        additional_context: str = None
    ) -> Tuple[str, str]:
        """
        Generate SQL from natural language.
        
        Args:
            natural_language_query: User's question in plain English
            additional_context: Additional context (intent, entities, etc.)
            
        Returns:
            Tuple of (sql_query, explanation)
            
        Raises:
            Exception: If generation fails
        """
        try:
            # Build full prompt
            prompt_parts = [self.system_prompt]
            
            if additional_context:
                prompt_parts.append(f"\nAdditional Context:\n{additional_context}\n")
            
            prompt_parts.append(f"\nUser Query: {natural_language_query}")
            
            full_prompt = "\n".join(prompt_parts)
            
            logger.debug(f"Generating SQL for: {natural_language_query}")
            
            # Generate SQL
            response = self.model.generate_content(full_prompt)
            
            if not response or not response.text:
                raise Exception("Empty response from Gemini API")
            
            # Extract SQL and explanation
            sql = self._extract_sql(response.text)
            explanation = self._extract_explanation(response.text)
            
            if not sql:
                raise Exception("Could not extract SQL from response")
            
            logger.debug(f"Generated SQL: {sql}")
            
            return sql, explanation
            
        except Exception as e:
            logger.error(f"SQL generation failed: {str(e)}")
            raise Exception(f"Failed to generate SQL: {str(e)}")
    
    def refine_query(
        self,
        original_query: str,
        feedback: str,
        natural_language_query: str
    ) -> str:
        """
        Refine SQL query based on validation feedback.
        
        Args:
            original_query: The SQL query that failed validation
            feedback: Validation errors/warnings
            natural_language_query: Original natural language query
            
        Returns:
            Refined SQL query
            
        Raises:
            Exception: If refinement fails
        """
        try:
            refinement_prompt = f"""
The following SQL query has issues:

```sql
{original_query}
```

Feedback:
{feedback}

Original user request: {natural_language_query}

Please generate a corrected SQL query that addresses all the issues mentioned in the feedback.
Remember the column selection rules:
- Only select columns that are explicitly needed
- Avoid SELECT * unless all columns are requested
- For "name" queries, only return name columns
- For "top N" queries, only return relevant columns plus ordering column

Return ONLY the corrected SQL query in a code block.
"""
            
            response = self.model.generate_content(refinement_prompt)
            
            if not response or not response.text:
                raise Exception("Empty response from Gemini API during refinement")
            
            refined_sql = self._extract_sql(response.text)
            
            if not refined_sql:
                raise Exception("Could not extract refined SQL from response")
            
            logger.debug(f"Refined SQL: {refined_sql}")
            
            return refined_sql
            
        except Exception as e:
            logger.error(f"SQL refinement failed: {str(e)}")
            raise Exception(f"Failed to refine SQL: {str(e)}")
    
    def _extract_sql(self, response_text: str) -> Optional[str]:
        """
        Extract SQL query from markdown code blocks.
        
        Args:
            response_text: Raw response from Gemini
            
        Returns:
            Extracted SQL query or None
        """
        # Try to find SQL in code blocks
        sql_pattern = r'```sql\s*(.*?)\s*```'
        matches = re.findall(sql_pattern, response_text, re.DOTALL | re.IGNORECASE)
        
        if matches:
            return matches[0].strip()
        
        # Try generic code blocks
        code_pattern = r'```\s*(.*?)\s*```'
        matches = re.findall(code_pattern, response_text, re.DOTALL)
        
        if matches:
            return matches[0].strip()
        
        # If no code blocks, try to find SELECT statement
        select_pattern = r'(SELECT\s+.*?;)'
        matches = re.findall(select_pattern, response_text, re.DOTALL | re.IGNORECASE)
        
        if matches:
            return matches[0].strip()
        
        # Last resort: check if entire response looks like SQL
        if response_text.strip().upper().startswith('SELECT'):
            # Remove any trailing explanation
            sql_lines = []
            for line in response_text.split('\n'):
                if line.strip().upper().startswith('EXPLANATION'):
                    break
                sql_lines.append(line)
            return '\n'.join(sql_lines).strip()
        
        return None
    
    def _extract_explanation(self, response_text: str) -> str:
        """
        Extract explanation from response.
        
        Args:
            response_text: Raw response from Gemini
            
        Returns:
            Explanation text
        """
        # Look for "Explanation:" marker
        explanation_pattern = r'Explanation:\s*(.*?)(?:\n\n|$)'
        matches = re.findall(explanation_pattern, response_text, re.DOTALL | re.IGNORECASE)
        
        if matches:
            return matches[0].strip()
        
        # If no explicit explanation, try to extract text after code block
        parts = response_text.split('```')
        if len(parts) > 2:
            explanation = parts[2].strip()
            if explanation:
                return explanation
        
        return "Query generated to fulfill the request."