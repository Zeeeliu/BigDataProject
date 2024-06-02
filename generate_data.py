import pandas as pd
import random
import pyarrow.parquet as pq
import pyarrow as pa

# List of US states
states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
    'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
    'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
    'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire',
    'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
    'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota',
    'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia',
    'Wisconsin', 'Wyoming'
]

# List of common occupations
occupations = [
    'Accountant', 'Engineer', 'Teacher', 'Nurse', 'Doctor', 'Lawyer',
    'Salesperson', 'Manager', 'Clerk', 'Mechanic'
]

# Function to generate a random 9-digit SSN
def generate_unique_ssns(num_ssns):
    ssns = set()
    while len(ssns) < num_ssns:
        ssn = '{:09d}'.format(random.randint(0, 999999999))
        ssns.add(ssn)
    return list(ssns)

# Number of records
num_records = 1000000

# Generate unique SSNs
ssns = generate_unique_ssns(num_records)

# Generate the data
data = {
    'SSN': ssns,
    'State': [random.choice(states) for _ in range(num_records)],
    'Occupation': [random.choice(occupations) for _ in range(num_records)]
}

# Create DataFrame
df = pd.DataFrame(data)

# Print to make sure it was properly generated
print(df.head())

# Display the number of rows in the DataFrame
num_rows = len(df)
print(f"Number of rows in the DataFrame: {num_rows}")

# Convert DataFrame to Parquet
table = pa.Table.from_pandas(df)
pq.write_table(table, 'data.parquet')

print("Parquet file 'data.parquet' created successfully.")
