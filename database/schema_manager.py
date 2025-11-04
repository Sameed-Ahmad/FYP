"""
Enhanced Schema Manager with Intelligence
Provides schema context with column usage recommendations.
"""

from typing import Dict, List, Optional
import json


class SchemaManager:
    """Enhanced schema manager with intelligent context"""
    
    def __init__(self, schema_dict: Dict):
        """
        Initialize with schema information.
        
        Args:
            schema_dict: Schema dictionary from DatabaseConnection
        """
        self.schema = schema_dict
        self._build_indexes()
    
    def _build_indexes(self):
        """Build quick lookup indexes"""
        self.table_columns = {}
        self.column_to_tables = {}
        self.primary_keys = {}
        self.foreign_keys = {}
        
        for table_name, table_info in self.schema.items():
            columns = [col['name'] for col in table_info['columns']]
            self.table_columns[table_name] = columns
            
            # Index columns to tables
            for col in columns:
                if col not in self.column_to_tables:
                    self.column_to_tables[col] = []
                self.column_to_tables[col].append(table_name)
            
            # Index primary keys
            pk_cols = [col['name'] for col in table_info['columns'] if col.get('primary_key')]
            if pk_cols:
                self.primary_keys[table_name] = pk_cols
            
            # Index foreign keys
            fk_list = []
            for col in table_info['columns']:
                if col.get('foreign_key'):
                    fk_list.append(col['foreign_key'])
            if fk_list:
                self.foreign_keys[table_name] = fk_list
    
    def get_schema_context(self, include_sample_queries: bool = True) -> str:
        """
        Format schema as comprehensive context for AI.
        
        Args:
            include_sample_queries: Whether to include example queries
            
        Returns:
            Formatted schema context string
        """
        context = ["DATABASE SCHEMA INFORMATION\n" + "=" * 50 + "\n"]
        
        for table_name, table_info in self.schema.items():
            context.append(f"\nTable: {table_name}")
            context.append("-" * 40)
            
            # Columns with enhanced metadata
            context.append("Columns:")
            for col in table_info['columns']:
                col_desc = f"  - {col['name']} ({col['type']})"
                
                flags = []
                if col.get('primary_key'):
                    flags.append("PRIMARY KEY")
                if col.get('nullable') is False:
                    flags.append("NOT NULL")
                if col.get('foreign_key'):
                    fk = col['foreign_key']
                    flags.append(f"FK → {fk['referred_table']}.{fk['referred_column']}")
                
                if flags:
                    col_desc += f" [{', '.join(flags)}]"
                
                context.append(col_desc)
            
            # Usage hints
            if 'id' in [c['name'].lower() for c in table_info['columns']]:
                context.append(f"\n  💡 Use {table_name} for identifying specific records")
            
            if any('name' in c['name'].lower() for c in table_info['columns']):
                context.append(f"  💡 Use name columns for display/search operations")
        
        # Relationships summary
        if any(self.foreign_keys.values()):
            context.append("\n" + "=" * 50)
            context.append("TABLE RELATIONSHIPS")
            context.append("=" * 50)
            
            for table, fks in self.foreign_keys.items():
                for fk in fks:
                    context.append(
                        f"\n{table}.{fk['constrained_columns'][0]} → "
                        f"{fk['referred_table']}.{fk['referred_column']}"
                    )
        
        # Query guidelines
        context.append("\n" + "=" * 50)
        context.append("QUERY GUIDELINES")
        context.append("=" * 50)
        context.append("""
1. SELECTION RULES:
   - For "get name" queries: SELECT ONLY the name column
   - For "top N" queries: Use ORDER BY + LIMIT N
   - For comparisons: SELECT only requested columns
   - For counts: Use COUNT(*) or COUNT(column)

2. JOIN REQUIREMENTS:
   - Always use proper INNER/LEFT JOIN with ON conditions
   - Follow foreign key relationships shown above
   - Use table aliases for readability (e.g., c.name)

3. OUTPUT OPTIMIZATION:
   - Only include columns mentioned in the query
   - For "show me X" → SELECT only X
   - For "compare A and B" → SELECT A, B
   - Avoid SELECT * unless explicitly requested

4. AGGREGATIONS:
   - Use GROUP BY for categorized results
   - Use HAVING for filtered aggregates
   - Include proper COUNT, SUM, AVG, MIN, MAX

5. ORDERING:
   - "top N" → ORDER BY DESC LIMIT N
   - "bottom N" → ORDER BY ASC LIMIT N
   - "latest" → ORDER BY date_column DESC
""")
        
        if include_sample_queries:
            context.append("\nSAMPLE QUERIES:")
            context.append("  Q: 'Get customer names' → SELECT customer_name FROM customers")
            context.append("  Q: 'Top 5 products' → SELECT product_name, price FROM products ORDER BY price DESC LIMIT 5")
            context.append("  Q: 'Count orders' → SELECT COUNT(*) as order_count FROM orders")
        
        return "\n".join(context)
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names"""
        return list(self.schema.keys())
    
    def get_table_columns(self, table_name: str) -> List[Dict]:
        """
        Get columns for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column dictionaries
        """
        return self.schema.get(table_name, {}).get('columns', [])
    
    def get_column_info(self, table_name: str, column_name: str) -> Optional[Dict]:
        """Get detailed info for a specific column"""
        columns = self.get_table_columns(table_name)
        for col in columns:
            if col['name'].lower() == column_name.lower():
                return col
        return None
    
    def get_relationships(self) -> Dict[str, List[Dict]]:
        """Get all foreign key relationships"""
        return self.foreign_keys
    
    def find_tables_with_column(self, column_name: str) -> List[str]:
        """Find which tables contain a given column"""
        return self.column_to_tables.get(column_name.lower(), [])
    
    def get_join_path(self, table1: str, table2: str) -> Optional[List[Dict]]:
        """
        Find join path between two tables.
        
        Args:
            table1: Source table
            table2: Target table
            
        Returns:
            List of join information dictionaries
        """
        # Simple direct relationship check
        if table1 in self.foreign_keys:
            for fk in self.foreign_keys[table1]:
                if fk['referred_table'] == table2:
                    return [{
                        'from_table': table1,
                        'from_column': fk['constrained_columns'][0],
                        'to_table': table2,
                        'to_column': fk['referred_column']
                    }]
        
        # Reverse check
        if table2 in self.foreign_keys:
            for fk in self.foreign_keys[table2]:
                if fk['referred_table'] == table1:
                    return [{
                        'from_table': table2,
                        'from_column': fk['constrained_columns'][0],
                        'to_table': table1,
                        'to_column': fk['referred_column']
                    }]
        
        return None
    
    def suggest_columns_for_intent(self, intent: str, mentioned_tables: List[str]) -> List[str]:
        """
        Suggest relevant columns based on query intent.
        
        Args:
            intent: Query intent (e.g., "get_name", "count", "compare")
            mentioned_tables: Tables mentioned in query
            
        Returns:
            List of suggested column names
        """
        suggestions = []
        
        for table in mentioned_tables:
            columns = self.table_columns.get(table, [])
            
            if intent in ["get_name", "show_name", "list_name"]:
                # Suggest name-like columns
                suggestions.extend([c for c in columns if 'name' in c.lower()])
            
            elif intent in ["count", "total"]:
                # Suggest primary key or first column
                if table in self.primary_keys:
                    suggestions.extend(self.primary_keys[table])
                elif columns:
                    suggestions.append(columns[0])
            
            elif intent in ["compare", "difference"]:
                # Suggest numeric or name columns
                for col in columns:
                    col_info = self.get_column_info(table, col)
                    if col_info and ('int' in col_info['type'].lower() or 
                                     'numeric' in col_info['type'].lower() or
                                     'name' in col.lower()):
                        suggestions.append(col)
        
        return suggestions
    
    def to_json(self) -> str:
        """Export schema as JSON"""
        return json.dumps(self.schema, indent=2)
    
    def get_available_columns_dict(self) -> Dict[str, List[str]]:
        """Get dictionary mapping tables to their columns"""
        return self.table_columns.copy()