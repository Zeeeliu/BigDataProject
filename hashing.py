import pandas as pd
import hashlib
import os
import json

# Read the parquet file
file_path = './data.parquet'
df = pd.read_parquet(file_path)

# Create a directory to store the bucket files
buckets_dir = './buckets'
os.makedirs(buckets_dir, exist_ok=True)


# Hashing function to map SSNs to 1000 buckets
def hash_ssn_to_bucket(ssn, num_buckets=1000):
    hash_object = hashlib.md5(ssn.encode())
    hash_hex = hash_object.hexdigest()
    bucket = int(hash_hex, 16) % num_buckets
    return bucket


# Create a dictionary to store the buckets
buckets = {i: [] for i in range(1000)}

# Create dictionaries to store the secondary indexes
state_index = {}
occupation_index = {}

# Distribute records into buckets and populate secondary indexes
for index, row in df.iterrows():
    bucket = hash_ssn_to_bucket(row['SSN'])
    buckets[bucket].append(row)

    # Populate state index
    state = row['State']
    if state not in state_index:
        state_index[state] = []
    state_index[state].append(row['SSN'])

    # Populate occupation index
    occupation = row['Occupation']
    if occupation not in occupation_index:
        occupation_index[occupation] = []
    occupation_index[occupation].append(row['SSN'])

# Sort the SSNs within each bucket and save to individual files
for bucket, records in buckets.items():
    if records:
        bucket_df = pd.DataFrame(records)
        bucket_df = bucket_df.sort_values(by='SSN')
        bucket_file_path = os.path.join(buckets_dir, f'bucket_{bucket}.parquet')
        bucket_df.to_parquet(bucket_file_path)

# Save the secondary indexes to JSON files
with open(os.path.join(buckets_dir, 'state_index.json'), 'w') as state_index_file:
    json.dump(state_index, state_index_file)

with open(os.path.join(buckets_dir, 'occupation_index.json'), 'w') as occupation_index_file:
    json.dump(occupation_index, occupation_index_file)

print("Records have been distributed into buckets and sorted by SSN within each bucket.")
print("Secondary indexes for states and occupations have been created.")
