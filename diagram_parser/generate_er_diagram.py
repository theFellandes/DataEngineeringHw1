#!/usr/bin/env python3
import re
import argparse
from graphviz import Digraph


def parse_sql_schema(sql_text):
    """
    Parses a SQL file to extract table definitions and foreign key relationships.

    Returns:
      tables: A dictionary mapping table names to a dict with raw content and foreign keys.
      foreign_keys: A list of tuples (child_table, fk_columns, parent_table, parent_columns).
    """
    tables = {}
    foreign_keys = []

    # Regex to match CREATE TABLE statements.
    # It captures the table name (including schema, if any) and the contents between parentheses.
    table_pattern = re.compile(
        r"CREATE\s+TABLE\s+([\w\.]+)\s*\((.*?)\);",
        re.DOTALL | re.IGNORECASE
    )

    for match in table_pattern.finditer(sql_text):
        full_table_name = match.group(1).strip()
        # Remove schema if present (e.g., dbo.books -> books)
        table_name = full_table_name.split('.')[-1]
        contents = match.group(2)
        tables[table_name] = {
            "raw": contents,
            "fks": []  # to store foreign keys in this table
        }

        # Regex to capture foreign key definitions within the table definition.
        fk_pattern = re.compile(
            r"FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+([\w\.]+)\s*\(([^)]+)\)",
            re.IGNORECASE
        )
        for fk_match in fk_pattern.finditer(contents):
            fk_columns = fk_match.group(1).strip()
            ref_full_table = fk_match.group(2).strip()
            ref_table = ref_full_table.split('.')[-1]
            ref_columns = fk_match.group(3).strip()
            tables[table_name]["fks"].append((fk_columns, ref_table, ref_columns))
            foreign_keys.append((table_name, fk_columns, ref_table, ref_columns))

    return tables, foreign_keys


def create_er_diagram(tables, foreign_keys, output_file="er_diagram"):
    """
    Generates an ER Diagram using Graphviz and renders it to an output file (PNG).
    """
    dot = Digraph(comment="Entity Relationship Diagram", format="png")

    # Add nodes for each table
    for table in tables:
        # For a more detailed label, you could include columns; here we just use the table name.
        dot.node(table, table)

    # Add edges for each foreign key relationship
    for child_table, fk_cols, parent_table, parent_cols in foreign_keys:
        # The edge label shows which columns reference which columns.
        label = f"{fk_cols} → {parent_cols}"
        dot.edge(child_table, parent_table, label=label)

    # Render the diagram; the output file will have a .png extension.
    dot.render(output_file, view=True)
    print(f"ER diagram saved as {output_file}.png")


def main():
    parser = argparse.ArgumentParser(description="Generate an ER Diagram from a SQL schema file")
    parser.add_argument("sql_file", help="Path to the .sql file containing DDL statements")
    parser.add_argument("--output", default="er_diagram", help="Base name for the output file (default: er_diagram)")
    args = parser.parse_args()

    with open(args.sql_file, "r") as f:
        sql_text = f.read()

    tables, foreign_keys = parse_sql_schema(sql_text)

    print("Parsed Tables:")
    for table, info in tables.items():
        print(f" - {table}")
    print("Parsed Foreign Keys:")
    for fk in foreign_keys:
        print(f" - {fk[0]}: {fk[1]} -> {fk[2]}: {fk[3]}")

    create_er_diagram(tables, foreign_keys, output_file=args.output)


if __name__ == "__main__":
    main()
