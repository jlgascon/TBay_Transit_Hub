import urllib.request
import time
import sqlite3
from google.transit import gtfs_realtime_pb2

# Configuration
FEED_URL = "http://api.nextlift.ca/gtfs-realtime/tripupdates.pb" 
DB_NAME = "transit_telemetry.db"
POLL_INTERVAL = 60  # Seconds between network sweeps

def sweep_and_store():
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n[{timestamp}] Pinging endpoint...")
    
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        req = urllib.request.Request(FEED_URL, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            feed.ParseFromString(response.read())
    except Exception as e:
        print(f"Network intercept failed: {e}")
        return

    batch_data = []
    
    # Live Aggregates for Terminal Insight
    cancellations = 0
    delayed_stops = 0
    max_delay = 0
    worst_route = "N/A"
    
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update.trip
            route_id = trip.route_id
            trip_id = trip.trip_id
            
            trip_status_code = trip.schedule_relationship
            trip_status = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(trip_status_code)
            
            if trip_status == "CANCELED":
                cancellations += 1
                batch_data.append(("ALL_STOPS", route_id, trip_id, trip_status, 0))
                continue
                
            for stop_update in entity.trip_update.stop_time_update:
                stop_id = stop_update.stop_id
                delay = stop_update.arrival.delay if stop_update.HasField('arrival') else 0
                
                stop_status_code = stop_update.schedule_relationship
                stop_status = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(stop_status_code)
                
                # Tally the damage
                if stop_status in ["CANCELED", "SKIPPED"]:
                    cancellations += 1
                if delay > 0:
                    delayed_stops += 1
                    if delay > max_delay:
                        max_delay = delay
                        worst_route = route_id
                
                batch_data.append((stop_id, route_id, trip_id, stop_status, delay))

    if batch_data:
        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO rt_telemetry (stop_id, route_id, trip_id, schedule_status, delay_seconds)
                VALUES (?, ?, ?, ?, ?)
            """, batch_data)
            conn.commit()
            
            # The Insight Block
            print(f" ├─ Ledger: {len(batch_data)} state updates committed.")
            print(f" ├─ Friction: {delayed_stops} stops currently reporting delays.")
            if max_delay > 0:
                print(f" ├─ Bleed: Route {worst_route} is lagging by {max_delay} seconds.")
            if cancellations > 0:
                print(f" └─ WARNING: {cancellations} system failures/cancellations detected.")
            else:
                print(f" └─ Status: No active cancellations.")
                
        except sqlite3.Error as e:
            print(f"Database write failure: {e}")
        finally:
            if conn:
                conn.close()
    else:
        print(" └─ Empty payload. No state changes detected.")
    # The Database Transaction
    if batch_data:
        try:
            conn = sqlite3.connect(DB_NAME)
            cursor = conn.cursor()
            
            # Executemany is highly optimized for bulk inserts in SQLite
            cursor.executemany("""
                INSERT INTO rt_telemetry (stop_id, route_id, trip_id, schedule_status, delay_seconds)
                VALUES (?, ?, ?, ?, ?)
            """, batch_data)
            
            conn.commit()
            print(f"Logged {len(batch_data)} state updates to the ledger.")
            
        except sqlite3.Error as e:
            print(f"Database write failure: {e}")
            
        finally:
            # Ensure the connection closes even if the write fails
            if conn:
                conn.close()
    else:
        print("Empty payload. No state changes detected.")

def verify_ledger_integrity():
    print("Verifying database architecture...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Enable Write-Ahead Logging
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # Forge the table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rt_telemetry (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            poll_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            route_id TEXT NOT NULL,
            trip_id TEXT NOT NULL,
            stop_id TEXT NOT NULL,
            schedule_status TEXT NOT NULL, 
            delay_seconds INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    print("Ledger integrity verified. Table structure is locked.")

def run_aggregator():
    print("===================================================")
    print(" LIVE INGESTION ACTIVE: WRITING TO LEDGER")
    print("===================================================")
    
    verify_ledger_integrity() # Add this line here
    
    try:
        while True:
            start_time = time.time()
            sweep_and_store()
            
            execution_time = time.time() - start_time
            sleep_time = max(0, POLL_INTERVAL - execution_time)
            
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\nPipeline terminated by user. Ledger safely closed.")
if __name__ == "__main__":
    run_aggregator()