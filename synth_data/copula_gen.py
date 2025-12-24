import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
import pickle

# 1. Load Data
print("Loading dataset...")
cols_to_use = [
    'FL_DATE',               # Date
    'AIRLINE_CODE',          # Airline Identifier
    'ORIGIN',                # Origin Airport
    'DEST',                  # Destination Airport
    'CRS_DEP_TIME',          # Scheduled Departure Time
    'DEP_DELAY',             # KPI: Departure Delay
    'ARR_DELAY',             # KPI: Arrival Delay
    'DISTANCE',              # KPI: Distance
    'DELAY_DUE_WEATHER',     # Fact: Weather Delay
    'DELAY_DUE_CARRIER',     # Fact: Carrier Delay
    'DELAY_DUE_NAS',         # Fact: NAS Delay
    'DELAY_DUE_LATE_AIRCRAFT'# Fact: Late Aircraft Delay
]

df = pd.read_csv("data/flights_sample_3m.csv", usecols=cols_to_use).sample(10000) # Train on a sample to be fast
# Extract Temporal Features from FL_DATE
df['FL_DATE'] = pd.to_datetime(df['FL_DATE'])
df['Month'] = df['FL_DATE'].dt.month
df['DayOfWeek'] = df['FL_DATE'].dt.dayofweek # 0=Monday, 6=Sunday

# Drop FL_DATE (We will simulate current dates in the stream)
df = df.drop(columns=['FL_DATE'])

# Ensure CRS_DEP_TIME is integer (0-2400)
df['CRS_DEP_TIME'] = pd.to_numeric(df['CRS_DEP_TIME'], errors='coerce').fillna(0).astype(int)

# --- 3. Define Metadata ---
print("Detecting metadata...")
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(df)

# Explicitly set categorical columns so SDV treats them as categories
categorical_cols = ['AIRLINE_CODE', 'ORIGIN', 'DEST']
for col in categorical_cols:
    metadata.update_column(column_name=col, sdtype='categorical')

# --- 4. Train Gaussian Copula ---
print("Training Gaussian Copula")
model = GaussianCopulaSynthesizer(metadata)
model.fit(df)

print("Generating 5 synthetic rows...")
synthetic_sample = model.sample(5)
print(synthetic_sample)

output_directory = '../producer/'
output_path = output_directory + 'model.pkl'

print(f"Saving model to {output_path}...")
with open(output_path, 'wb') as f:
    pickle.dump(model, f)
