import google.generativeai as genai
import logging
from typing import Dict, Any
from config.settings import settings

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class SQLGenerator:
    """Generates SQL queries from natural language using Google Gemini (FREE!)"""
    
    def __init__(self, schema_context: str):
        genai.configure(api_key=settings.google_api_key)
        # Using Gemini 2.0 Flash - Latest and fastest FREE model!
        # Alternative models: 'gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-2.0-flash-exp'
        self.model = genai.GenerativeModel('gemini-2.0-flash-exp')
        self.schema_context = schema_context
    
    def generate_sql(self, natural_language_query: str) -> Dict[str, Any]:
        """
        Convert natural language to SQL query using Gemini
        
        Args:
            natural_language_query: User's question in natural language
            
        Returns:
            Dictionary containing SQL query and explanation
        """
        prompt = f"""You are an expert SQL query generator. Given a natural language question and database schema, generate a valid PostgreSQL query.

DATABASE SCHEMA:
{self.schema_context}

RULES:
1. Generate ONLY SELECT queries (no INSERT, UPDATE, DELETE, DROP)
2. Use proper JOIN syntax when needed
3. Include appropriate WHERE clauses for filtering
4. Use aggregate functions (COUNT, SUM, AVG, etc.) when appropriate
5. Add ORDER BY and LIMIT clauses when relevant
6. Return ONLY valid SQL, no explanations in the query itself
7. Use table and column names exactly as they appear in the schema
8. Be case-insensitive for table and column matching

USER QUESTION: {natural_language_query}

Please respond in this exact format:
```sql
[YOUR SQL QUERY HERE]
```

EXPLANATION:
[Brief explanation of what the query does]"""

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=1000,
                )
            )
            
            content = response.text
            
            # Extract SQL query from markdown code block
            sql_query = self._extract_sql(content)
            explanation = self._extract_explanation(content)
            
            logger.info(f"Generated SQL: {sql_query}")
            
            return {
                "sql": sql_query,
                "explanation": explanation,
                "original_query": natural_language_query
            }
            
        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            raise Exception(f"Failed to generate SQL query: {str(e)}")
    
    def _extract_sql(self, content: str) -> str:
        """Extract SQL query from markdown code block"""
        lines = content.split('\n')
        sql_lines = []
        in_code_block = False
        
        for line in lines:
            if line.strip().startswith('```sql') or line.strip().startswith('```'):
                in_code_block = not in_code_block
                continue
            if in_code_block:
                sql_lines.append(line)
        
        # If no code block found, try to find SQL directly
        if not sql_lines:
            for line in lines:
                upper_line = line.strip().upper()
                if upper_line.startswith('SELECT') or upper_line.startswith('WITH'):
                    sql_lines.append(line)
                    # Get the rest of the query
                    idx = lines.index(line)
                    for remaining_line in lines[idx+1:]:
                        stripped = remaining_line.strip()
                        if stripped and not stripped.upper().startswith('EXPLANATION'):
                            sql_lines.append(remaining_line)
                        elif stripped.upper().startswith('EXPLANATION'):
                            break
                    break
        
        return '\n'.join(sql_lines).strip()
    
    def _extract_explanation(self, content: str) -> str:
        """Extract explanation from response"""
        lines = content.split('\n')
        explanation_lines = []
        in_explanation = False
        skip_code_block = False
        
        for line in lines:
            if '```' in line:
                skip_code_block = not skip_code_block
                continue
            
            if 'EXPLANATION' in line.upper():
                in_explanation = True
                continue
                
            if not skip_code_block and in_explanation:
                if line.strip():
                    explanation_lines.append(line)
        
        # If no explanation found after EXPLANATION marker, get text after code block
        if not explanation_lines:
            after_code = False
            for line in lines:
                if '```' in line:
                    after_code = not after_code
                    continue
                if after_code and line.strip() and not line.strip().upper().startswith('SELECT'):
                    explanation_lines.append(line)
        
        explanation = '\n'.join(explanation_lines).strip()
        return explanation if explanation else "Query generated successfully"
    
    def refine_query(self, sql_query: str, feedback: str) -> str:
        """
        Refine an existing SQL query based on feedback
        
        Args:
            sql_query: Existing SQL query
            feedback: User feedback for refinement
            
        Returns:
            Refined SQL query
        """
        prompt = f"""Given this SQL query:
```sql
{sql_query}
```

User feedback: {feedback}

Generate an improved SQL query based on the feedback. 

DATABASE SCHEMA:
{self.schema_context}

Return only the SQL query in a code block."""

        try:
            response = self.model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=1000,
                )
            )
            
            refined_sql = self._extract_sql(response.text)
            return refined_sql
            
        except Exception as e:
            logger.error(f"Error refining query: {str(e)}")
            return sql_query