import sqlite3
import csv

DB_NAME = "transit_telemetry.db"
OUTPUT_FILE = "telemetry_dump.csv"

def extract_ledger():
    print(f"Tapping the vault: {DB_NAME}...")
    
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        # Pulling the entire table. As this scales, we'll add a WHERE clause to slice by time.
        cursor.execute("SELECT * FROM rt_telemetry")
        rows = cursor.fetchall()
        
        if not rows:
            print("Ledger is empty. Run your in_live.py loop to gather mass first.")
            return
            
        # Dynamically rip the column headers from the schema
        headers = [description[0] for description in cursor.description]
        
        # Write the flat file
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(headers)
            writer.writerows(rows)
            
        print(f"Extraction successful: {len(rows)} records written to {OUTPUT_FILE}.")
        
    except sqlite3.Error as e:
        print(f"Database read failure: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    extract_ledger()