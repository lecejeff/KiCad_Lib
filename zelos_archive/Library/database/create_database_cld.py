#!/usr/bin/env python3
import os
import csv
import json
import sqlite3
import argparse
from pathlib import Path
import codecs

class KiCADDatabaseCreator:
    def __init__(self, folder_path: str, database_name: str):
        self.folder_path = Path(folder_path)
        self.database_name = database_name
        self.db_path = Path(f"{database_name}.sqlite3")
        self.kicad_dbl_path = Path(f"{database_name}.kicad_dbl")
        self.tables = {}

    def read_csv_files(self):
        """Read all CSV files from the specified folder."""
        for csv_file in self.folder_path.glob('*.csv'):
            table_name = csv_file.stem
            with codecs.open(csv_file, 'r', encoding='ISO-8859-3') as f:
                csv_reader = csv.DictReader(f)
                headers = csv_reader.fieldnames
                if not headers or 'ZPN' not in headers:
                    print(f"Skipping {csv_file}: No ZPN column found")
                    continue
                
                rows = []
                for row in csv_reader:
                    # Handle quoted strings
                    processed_row = {
                        k: v.strip('"') if isinstance(v, str) else v
                        for k, v in row.items()
                    }
                    rows.append(processed_row)
                
                self.tables[table_name] = {
                    'headers': headers,
                    'rows': rows
                }

    def create_sqlite_database(self):
        """Create SQLite database and populate with CSV data."""
        if self.db_path.exists():
            self.db_path.unlink()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for table_name, table_data in self.tables.items():
            # Create table
            columns = [f"{h.replace(' ', '_')} TEXT" for h in table_data['headers']]
            create_query = f"""
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            )
            """
            cursor.execute(create_query)

            # Insert data
            for row in table_data['rows']:
                placeholders = ','.join(['?' for _ in table_data['headers']])
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                values = [row.get(h, '') for h in table_data['headers']]
                cursor.execute(insert_query, values)

        conn.commit()
        conn.close()

    def create_kicad_dbl(self):
        """Create KiCAD database library file."""
        kicad_dbl = {
            "meta": {
                "version": 0
            },
            "name": self.database_name,
            "description": f"Database generated from {self.folder_path}",
            "source": {
                "type": "odbc",
                "dsn": "",
                "username": "",
                "password": "",
                "timeout_seconds": 2,
                "connection_string": f"DRIVER={{SQLite3 ODBC Driver}};DATABASE=${{CWD}}/{self.db_path.name}"
            },
            "libraries": []
        }

        # Create library entry for each table
        for table_name, table_data in self.tables.items():
            library = {
                "name": table_name,
                "table": table_name,
                "key": "ZPN",
                "symbols": next((h for h in table_data['headers'] if h.lower() == 'symbol'), ""),
                "footprints": next((h for h in table_data['headers'] if h.lower() == 'footprint'), ""),
                "fields": []
            }

            # Add fields for each column
            for header in table_data['headers']:
                # Skip ZPN (key) and Symbol/Footprint columns as they're handled separately
                if header.lower() not in ['zpn', 'symbol', 'footprint']:
                    field = {
                        "column": header,
                        "name": header,
                        "visible_on_add": True,
                        "visible_in_chooser": True,
                        "show_name": True,
                        "inherit_properties": True
                    }
                    library["fields"].append(field)

            # Add standard properties
            library["properties"] = {
                "description": "Description",
                "footprint_filters": "Footprint Filters",
                "keywords": "Keywords",
                "exclude_from_bom": "No BOM",
                "exclude_from_board": "Schematic Only"
            }

            kicad_dbl["libraries"].append(library)

        # Write the KiCAD database library file
        with open(self.kicad_dbl_path, 'w', encoding='utf-8') as f:
            json.dump(kicad_dbl, f, indent=4)

def main():
    parser = argparse.ArgumentParser(description='Create KiCAD database from CSV files')
    parser.add_argument('folder_path', help='Path to folder containing CSV files')
    parser.add_argument('database_name', help='Name for the output database (without extension)')
    
    args = parser.parse_args()

    creator = KiCADDatabaseCreator(args.folder_path, args.database_name)
    
    print("Reading CSV files...")
    creator.read_csv_files()
    
    print("Creating SQLite database...")
    creator.create_sqlite_database()
    
    print("Creating KiCAD database library file...")
    creator.create_kicad_dbl()
    
    print(f"Done! Created {creator.db_path} and {creator.kicad_dbl_path}")

if __name__ == "__main__":
    main()