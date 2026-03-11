import urllib.request
import time
from google.transit import gtfs_realtime_pb2

# Target endpoint for Thunder Bay's GTFS-RT trip updates
FEED_URL = "http://api.nextlift.ca/gtfs-realtime/tripupdates.pb" 

def dry_run_sweep():
    print(f"\n[{time.strftime('%H:%M:%S')}] Pinging endpoint...")
    
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        req = urllib.request.Request(FEED_URL, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            feed.ParseFromString(response.read())
    except Exception as e:
        print(f"Network intercept failed: {e}")
        return

    batch_data = []
    
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip = entity.trip_update.trip
            route_id = trip.route_id
            trip_id = trip.trip_id
            
            trip_status_code = trip.schedule_relationship
            trip_status = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(trip_status_code)
            
            if trip_status == "CANCELED":
                batch_data.append(("ALL_STOPS", route_id, trip_id, trip_status, 0))
                continue
                
            for stop_update in entity.trip_update.stop_time_update:
                stop_id = stop_update.stop_id
                delay = stop_update.arrival.delay if stop_update.HasField('arrival') else 0
                
                stop_status_code = stop_update.schedule_relationship
                stop_status = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(stop_status_code)
                
                batch_data.append((stop_id, route_id, trip_id, stop_status, delay))

    # The Emulated Transaction Log
    print(f"--- EMULATED SQL ({len(batch_data)} records detected) ---")
    
    # Slicing the first 5 records to prevent terminal flood
    for record in batch_data[:5]:
        print(f"INSERT INTO telemetry_log (stop_id, route_id, trip_id, schedule_status, delay_seconds) VALUES {record};")
    
    if len(batch_data) > 5:
        print(f"... plus {len(batch_data) - 5} additional statements.")
        
    print("-" * 50)

def connection_test():
    print("===================================================")
    print(" DRY RUN ACTIVE: 20-SECOND DIAGNOSTIC WINDOW")
    print("===================================================")
    
    # 4 sweeps at 5-second intervals = 20 seconds total
    for i in range(4):
        dry_run_sweep()
        if i < 3:
            time.sleep(5)
            
    print("\nDiagnostic complete. Zero state changes made.")

if __name__ == "__main__":
    connection_test()