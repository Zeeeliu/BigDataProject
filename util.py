import pandas as pd
import os
import json
import datetime
from hashing import hash_ssn_to_bucket

# Constants
BUCKETS_DIR = './buckets'
WAL_DIR = './wal'
MAX_WAL_OPERATIONS = 100

# Initialize the WAL counter
wal_counter = 0
wal_file_counter = 0

# Function to get the current timestamp
def get_timestamp():
    return datetime.datetime.now().strftime('%Y%m%d%H%M%S')

# Function to write to the WAL
def write_to_wal(operation, data):
    global wal_counter, wal_file_counter
    timestamp = get_timestamp()
    wal_entry = f"{timestamp},{operation},{json.dumps(data)}\n"
    wal_file_path = os.path.join(WAL_DIR, f'wal_{wal_file_counter}.txt')
    with open(wal_file_path, 'a') as wal_file:
        wal_file.write(wal_entry)
    wal_counter += 1
    if wal_counter >= MAX_WAL_OPERATIONS:
        wal_counter = 0
        wal_file_counter += 1

# Function to process the WAL and undo failed changes
def process_wal():
    wal_files = sorted([f for f in os.listdir(WAL_DIR) if f.startswith('wal_')])
    for wal_file in wal_files:
        wal_file_path = os.path.join(WAL_DIR, wal_file)
        with open(wal_file_path, 'r') as file:
            for line in file:
                timestamp, operation, data = line.strip().split(',', 2)
                data = json.loads(data)
                if operation == 'create':
                    delete_record(data['SSN'])
                elif operation == 'update':
                    update_record(data['SSN'], data['old_data'])
                elif operation == 'delete':
                    create_record(data)
        os.remove(wal_file_path)

# Function to read a record by SSN
def read_record(ssn):
    bucket = hash_ssn_to_bucket(ssn)
    bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')
    if os.path.exists(bucket_file_path):
        bucket_df = pd.read_parquet(bucket_file_path)
        record = bucket_df[bucket_df['SSN'] == ssn]
        if not record.empty:
            return record.to_dict(orient='records')[0]
    return None

# Function to create a new record
def create_record(record):
    ssn = record['SSN']
    bucket = hash_ssn_to_bucket(ssn)
    bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')
    if os.path.exists(bucket_file_path):
        bucket_df = pd.read_parquet(bucket_file_path)
        if ssn in bucket_df['SSN'].values:
            print(f"Record with SSN {ssn} already exists. Record not created")
            return
        # Find the correct position to insert the record using binary search
        insert_index = bucket_df['SSN'].searchsorted(ssn)
        # Insert the record at the correct position
        bucket_df = pd.concat([bucket_df[:insert_index], pd.DataFrame([record]), bucket_df[insert_index:]], ignore_index=True)
    else:
        bucket_df = pd.DataFrame([record])
    bucket_df.to_parquet(bucket_file_path, index=False)
    write_to_wal('create', record)
    update_secondary_indexes(record)

# Function to update a record
def update_record(ssn, updated_data):
    old_record = read_record(ssn)
    if old_record:
        bucket = hash_ssn_to_bucket(ssn)
        bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')
        bucket_df = pd.read_parquet(bucket_file_path)
        bucket_df.loc[bucket_df['SSN'] == ssn, updated_data.keys()] = updated_data.values()
        bucket_df.to_parquet(bucket_file_path, index=False)
        write_to_wal('update', {'SSN': ssn, 'old_data': old_record, 'new_data': updated_data})
        update_secondary_indexes(updated_data, old_record)
    else:
        print(f"Record with SSN {ssn} does not exist.")

# Function to delete a record
def delete_record(ssn):
    old_record = read_record(ssn)
    if old_record:
        bucket = hash_ssn_to_bucket(ssn)
        bucket_file_path = os.path.join(BUCKETS_DIR, f'bucket_{bucket}.parquet')
        bucket_df = pd.read_parquet(bucket_file_path)
        bucket_df = bucket_df[bucket_df['SSN'] != ssn]
        bucket_df.to_parquet(bucket_file_path, index=False)
        write_to_wal('delete', old_record)
        remove_from_secondary_indexes(old_record)
    else:
        print(f"Record with SSN {ssn} does not exist.")

# Function to update secondary indexes
def update_secondary_indexes(new_data, old_data=None):
    # Update state index
    state_index_file_path = os.path.join(BUCKETS_DIR, 'state_index.json')
    if os.path.exists(state_index_file_path):
        with open(state_index_file_path, 'r') as state_index_file:
            state_index = json.load(state_index_file)
    else:
        state_index = {}
    if old_data:
        state_index[old_data['State']].remove(old_data['SSN'])
    state_index.setdefault(new_data['State'], []).append(new_data['SSN'])
    with open(state_index_file_path, 'w') as state_index_file:
        json.dump(state_index, state_index_file)

    # Update occupation index
    occupation_index_file_path = os.path.join(BUCKETS_DIR, 'occupation_index.json')
    if os.path.exists(occupation_index_file_path):
        with open(occupation_index_file_path, 'r') as occupation_index_file:
            occupation_index = json.load(occupation_index_file)
    else:
        occupation_index = {}
    if old_data:
        occupation_index[old_data['Occupation']].remove(old_data['SSN'])
    occupation_index.setdefault(new_data['Occupation'], []).append(new_data['SSN'])
    with open(occupation_index_file_path, 'w') as occupation_index_file:
        json.dump(occupation_index, occupation_index_file)

# Function to remove a record from secondary indexes
def remove_from_secondary_indexes(record):
    # Remove from state index
    state_index_file_path = os.path.join(BUCKETS_DIR, 'state_index.json')
    with open(state_index_file_path, 'r') as state_index_file:
        state_index = json.load(state_index_file)
    state_index[record['State']].remove(record['SSN'])
    with open(state_index_file_path, 'w') as state_index_file:
        json.dump(state_index, state_index_file)

    # Remove from occupation index
    occupation_index_file_path = os.path.join(BUCKETS_DIR, 'occupation_index.json')
    with open(occupation_index_file_path, 'r') as occupation_index_file:
        occupation_index = json.load(occupation_index_file)
    occupation_index[record['Occupation']].remove(record['SSN'])
    with open(occupation_index_file_path, 'w') as occupation_index_file:
        json.dump(occupation_index, occupation_index_file)

# Create the necessary directories if they don't exist
os.makedirs(BUCKETS_DIR, exist_ok=True)
os.makedirs(WAL_DIR, exist_ok=True)

# Process the WAL on initialization
process_wal()

# Example usage
create_record({'SSN': '123456789', 'State': 'California', 'Occupation': 'Engineer'})
create_record({'SSN': '987654321', 'State': 'New York', 'Occupation': 'Teacher'})

# Read records
print(read_record('123456789'))
print(delete_record('123456789'))
print(read_record('123456789'))
print(read_record('987654321'))
