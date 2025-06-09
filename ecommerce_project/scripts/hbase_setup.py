import happybase
import json
import glob
import os
from datetime import datetime

# --- Connect to HBase ---
connection = happybase.Connection('localhost')
connection.open()

# --- Base Data Directory ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'ecommerce_data')

# --- Create Tables Safely ---
tables = connection.tables()
if b'user_sessions' not in tables:
    connection.create_table(
        'user_sessions',
        {
            'info': dict(max_versions=1),
            'geo': dict(max_versions=1),
            'device': dict(max_versions=1),
            'views': dict(max_versions=1),
            'cart': dict(max_versions=1)
        }
    )

if b'product_metrics' not in tables:
    connection.create_table(
        'product_metrics',
        {
            'views': dict(max_versions=10),
            'cart': dict(max_versions=10),
            'purchases': dict(max_versions=10)
        }
    )

# --- Load JSON Sessions into HBase ---
def load_sessions_to_hbase(session_file):
    table = connection.table('user_sessions')
    with open(session_file, 'r') as f:
        sessions = json.load(f)
    
    for session in sessions:
        user_id = session['user_id']
        timestamp = int(datetime.fromisoformat(session['start_time'].replace('Z', '+00:00')).timestamp())
        reverse_ts = f"{2**63 - timestamp:020d}"
        row_key = f"{user_id}:{reverse_ts}"
        
        table.put(row_key, {
            b'info:session_id': session['session_id'].encode(),
            b'info:duration': str(session['duration_seconds']).encode(),
            b'info:conversion': session['conversion_status'].encode(),
            b'geo:city': session['geo_data']['city'].encode(),
            b'geo:state': session['geo_data']['state'].encode(),
            b'geo:country': session['geo_data']['country'].encode(),
            b'geo:ip': session['geo_data']['ip_address'].encode(),
            b'device:type': session['device_profile']['type'].encode(),
            b'device:os': session['device_profile']['os'].encode(),
            b'device:browser': session['device_profile']['browser'].encode(),
            b'views:data': json.dumps(session['page_views']).encode(),
            b'cart:data': json.dumps(session['cart_contents']).encode()
        })

# --- Load All Session Chunks ---
session_files = glob.glob(os.path.join(DATA_DIR, 'sessions_*.json'))
for session_file in session_files:
    load_sessions_to_hbase(session_file)
    print(f"✅ Loaded {os.path.basename(session_file)} into HBase user_sessions")

# --- Sample Query: Get Recent Sessions for a User ---
def get_user_sessions(user_id, limit=10):
    table = connection.table('user_sessions')
    rows = table.scan(row_prefix=f"{user_id}:".encode(), limit=limit)
    sessions = []
    for row_key, data in rows:
        sessions.append({
            'session_id': data.get(b'info:session_id', b'').decode(),
            'duration': int(data.get(b'info:duration', b'0').decode()),
            'conversion': data.get(b'info:conversion', b'').decode(),
            'city': data.get(b'geo:city', b'').decode()
        })
    return sessions

# --- Example Usage ---
user_sessions = get_user_sessions('user_000001')
print("\n🧾 Recent sessions for user_000001:")
for s in user_sessions:
    print(f"• Session {s['session_id']}: {s['duration']}s, {s['conversion']}, {s['city']}")

connection.close()

