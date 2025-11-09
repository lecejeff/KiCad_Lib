import os
import sqlite3
import json
import csv
import sys

def create_database(csv_folder, db_name):
    db_path = f"{db_name}.sqlite3"
    kicad_dbl_path = f"{db_name}.kicad_dbl"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for file in os.listdir(csv_folder):
        if file.endswith(".csv"):
            table_name = os.path.splitext(file)[0]
            file_path = os.path.join(csv_folder, file)
            
            with open(file_path, "r", encoding="ISO-8859-3") as f:
                reader = csv.reader(f)
                headers = next(reader)
                headers = [h.strip('"') for h in headers]  # Remove quotes from headers
                
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join([f'"{h}" TEXT' for h in headers])},
                    PRIMARY KEY("ZPN")
                )
                """)
                
                for row in reader:
                    row = [col.strip('"') for col in row]  # Remove quotes from values
                    cursor.execute(f"INSERT OR IGNORE INTO {table_name} VALUES ({', '.join(['?' for _ in row])})", row)
    
    conn.commit()
    conn.close()
    
    kicad_dbl_content = {
        "meta": {"version": 0},
        "name": "My Database Library",
        "description": "A database of components",
        "source": {
            "type": "odbc",
            "dsn": "",
            "username": "",
            "password": "",
            "timeout_seconds": 2,
            "connection_string": f"DRIVER=SQLite3;DATABASE={db_path}"
        },
        "libraries": []
    }
    
    for file in os.listdir(csv_folder):
        if file.endswith(".csv"):
            table_name = os.path.splitext(file)[0]
            kicad_dbl_content["libraries"].append({
                "name": table_name,
                "table": table_name,
                "key": "ZPN",
                "symbols": "Symbols",
                "footprints": "Footprints",
                "fields": [{
                    "column": "MPN",
                    "name": "MPN",
                    "visible_on_add": False,
                    "visible_in_chooser": True,
                    "show_name": True,
                    "inherit_properties": True
                }, {
                    "column": "Value",
                    "name": "Value",
                    "visible_on_add": True,
                    "visible_in_chooser": True,
                    "show_name": False
                }],
                "properties": {
                    "description": "Description",
                    "footprint_filters": "Footprint Filters",
                    "keywords": "Keywords",
                    "exclude_from_bom": "No BOM",
                    "exclude_from_board": "Schematic Only"
                }
            })
    
    with open(kicad_dbl_path, "w", encoding="utf-8") as f:
        json.dump(kicad_dbl_content, f, indent=4)
    
    print(f"Database and KiCad DBL file created: {db_path}, {kicad_dbl_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <csv_folder> <database_name>")
        sys.exit(1)
    
    csv_folder = sys.argv[1]
    db_name = sys.argv[2]
    create_database(csv_folder, db_name)
