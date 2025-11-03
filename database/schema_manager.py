from typing import Dict, Any
import json


class SchemaManager:
    """Manages database schema information and formatting for AI context"""
    
    def __init__(self, schema_info: Dict[str, Any]):
        self.schema_info = schema_info
    
    def get_schema_context(self) -> str:
        """
        Format schema information for AI context
        
        Returns:
            Formatted string describing database schema
        """
        context_parts = ["DATABASE SCHEMA:\n"]
        
        for table_name, table_info in self.schema_info.items():
            context_parts.append(f"\nTable: {table_name}")
            
            # Add columns
            context_parts.append("Columns:")
            for col in table_info["columns"]:
                nullable = "NULL" if col["nullable"] else "NOT NULL"
                context_parts.append(f"  - {col['name']}: {col['type']} ({nullable})")
            
            # Add primary keys
            if table_info["primary_keys"]:
                pk_list = ", ".join(table_info["primary_keys"])
                context_parts.append(f"Primary Key: {pk_list}")
            
            # Add foreign keys
            if table_info["foreign_keys"]:
                context_parts.append("Foreign Keys:")
                for fk in table_info["foreign_keys"]:
                    fk_cols = ", ".join(fk["columns"])
                    ref_cols = ", ".join(fk["referred_columns"])
                    context_parts.append(
                        f"  - {fk_cols} -> {fk['referred_table']}({ref_cols})"
                    )
        
        return "\n".join(context_parts)
    
    def get_table_names(self) -> list:
        """Get list of all table names"""
        return list(self.schema_info.keys())
    
    def get_table_columns(self, table_name: str) -> list:
        """Get column names for a specific table"""
        if table_name in self.schema_info:
            return [col["name"] for col in self.schema_info[table_name]["columns"]]
        return []
    
    def get_relationships(self) -> Dict[str, list]:
        """Get all foreign key relationships"""
        relationships = {}
        for table_name, table_info in self.schema_info.items():
            if table_info["foreign_keys"]:
                relationships[table_name] = table_info["foreign_keys"]
        return relationships
    
    def to_json(self) -> str:
        """Export schema as JSON string"""
        return json.dumps(self.schema_info, indent=2)