import urllib.request
import sqlite3
import time
from google.transit import gtfs_realtime_pb2

DB_NAME = "transit_telemetry.db"
# Using the specific trip updates endpoint for the network
FEED_URL = "http://api.nextlift.ca/gtfs-realtime/tripupdates.pb" 

def sweep_network():
    print(f"\n[{time.strftime('%H:%M:%S')}] Initiating full network sweep...")
    
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        # Some transit APIs block default Python user-agents. We spoof a standard browser.
        req = urllib.request.Request(FEED_URL, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            feed.ParseFromString(response.read())
    except Exception as e:
        print(f"Network intercept failed: {e}")
        return

    # Connect to your immutable ledger
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    batch_data = []
    cancellation_count = 0
    
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update.trip
            route_id = trip.route_id
            trip_id = trip.trip_id
            
            # 1. Check for a total Trip Cancellation
            trip_status_code = trip.schedule_relationship
            trip_status = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(trip_status_code)
            
            if trip_status == "CANCELED":
                cancellation_count += 1
                # Log the dead trip. "ALL_STOPS" indicates the entire route failed.
                batch_data.append(("ALL_STOPS", route_id, trip_id, trip_status, 0))
                continue # Skip processing individual stops if the whole bus is ghosted
                
            # 2. Check individual Stop Updates (for delays or skipped stops)
            for stop_update in entity.trip_update.stop_time_update:
                stop_id = stop_update.stop_id
                delay = stop_update.arrival.delay if stop_update.HasField('arrival') else 0
                
                stop_status_code = stop_update.schedule_relationship
                stop_status = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(stop_status_code)
                
                if stop_status == "SKIPPED" or stop_status == "CANCELED":
                    cancellation_count += 1
                    
                batch_data.append((stop_id, route_id, trip_id, stop_status, delay))

    # Batch insert the sweep into SQLite for performance
    if batch_data:
        cursor.executemany("""
            INSERT INTO telemetry_log (stop_id, route_id, trip_id, schedule_status, delay_seconds)
            VALUES (?, ?, ?, ?, ?)
        """, batch_data)
        conn.commit()
        
    conn.close()
    
    print(f"[{time.strftime('%H:%M:%S')}] Sweep complete. Logged {len(batch_data)} state updates.")
    if cancellation_count > 0:
        print(f"⚠️ Dissonance detected: {cancellation_count} system failures/cancellations recorded.")

def live_demo_loop(interval_seconds=60):
    print("===================================================")
    print(" TRANSIT TELEMETRY PIPELINE - LIVE DEMO ACTIVE")
    print("===================================================")
    try:
        while True:
            sweep_network()
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nPipeline terminated by user. Ledger safely closed.")

if __name__ == "__main__":
    # Polls the network every 60 seconds
    live_demo_loop(60)