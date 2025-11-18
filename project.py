import os
import json
from datetime import datetime, timezone
from pathlib import Path

# 1. Define the FEATURE_STORE_BASE_DIR
FEATURE_STORE_BASE_DIR = Path('feature_store_data/')

# Ensure the base directory exists
FEATURE_STORE_BASE_DIR.mkdir(parents=True, exist_ok=True)
print(f"Feature store base directory set to: {FEATURE_STORE_BASE_DIR}")

# 2. write_features function implementation
def write_features(entity_id, feature_group_name, features, event_timestamp=None):
    if event_timestamp is None:
        event_timestamp = datetime.now(timezone.utc)
    
    feature_group_path = FEATURE_STORE_BASE_DIR / feature_group_name
    entity_path = feature_group_path / entity_id
    entity_path.mkdir(parents=True, exist_ok=True)
    
    # Use the timestamp to create a unique filename
    # Format timestamp to be filesystem friendly and sortable
    timestamp_str = event_timestamp.isoformat().replace(':', '-').replace('.', '_')
    filename = f"{timestamp_str}.json"
    file_path = entity_path / filename
    
    # Prepare data for saving
    data_to_save = {
        "entity_id": entity_id,
        "features": features,
        "event_timestamp": event_timestamp.isoformat()
    }
    
    with open(file_path, 'w') as f:
        json.dump(data_to_save, f, indent=4)
    
    print(f"Features for {entity_id} in group '{feature_group_name}' written to {file_path}")

# 3. read_features function implementation
def read_features(entity_id, feature_group_name, as_of=None, limit=None, historical=False):
    feature_group_path = FEATURE_STORE_BASE_DIR / feature_group_name
    entity_path = feature_group_path / entity_id
    
    if not entity_path.is_dir():
        # print(f"No features found for entity '{entity_id}' in group '{feature_group_name}'.") # Suppress for cleaner output
        return []
    
    feature_files = sorted(entity_path.glob("*.json"))
    
    all_features = []
    for f_path in feature_files:
        with open(f_path, 'r') as f:
            data = json.load(f)
            # Convert event_timestamp back to datetime object
            data['event_timestamp'] = datetime.fromisoformat(data['event_timestamp'])
            all_features.append(data)
            
    if historical:
        # Return all features in chronological order
        return sorted(all_features, key=lambda x: x['event_timestamp'])

    if as_of:
        # Filter for features before or at as_of timestamp
        filtered_features = [f for f in all_features if f['event_timestamp'] <= as_of]
        # Get the most recent feature from the filtered list
        if filtered_features:
            return [max(filtered_features, key=lambda x: x['event_timestamp'])]
        else:
            return []
    
    if limit:
        # Get the most recent 'limit' features
        # Sort by timestamp in ascending order and take the last 'limit'
        return sorted(all_features, key=lambda x: x['event_timestamp'])[-limit:]
    
    # Default to latest feature if no other options are specified
    if all_features:
        return [max(all_features, key=lambda x: x['event_timestamp'])]
    else:
        return []

# Helper to make datetime objects JSON serializable
def json_serializable_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

demonstration_outputs = {}

print("\n--- Demonstrating User Profile Features ---")

# 4. Code demonstrating the writing of user profile features
user_id = 'user_123'
feature_group = 'user_profile_features'

# Features for user_123 at different points in time (from kernel state)
features_v1 = {'age': 30, 'gender': 'Male', 'country': 'USA', 'membership_level': 'Silver', 'registration_date': '2022-05-01T00:00:00', 'last_login_date': '2022-12-25T00:00:00'}
features_v2 = {'age': 31, 'gender': 'Male', 'country': 'USA', 'membership_level': 'Gold', 'registration_date': '2022-05-01T00:00:00', 'last_login_date': '2023-03-10T00:00:00'}
features_v3 = {'age': 31, 'gender': 'Male', 'country': 'Canada', 'membership_level': 'Gold', 'registration_date': '2022-05-01T00:00:00', 'last_login_date': '2023-06-15T00:00:00'}

print("\nWriting user profile features:")
write_features(user_id, feature_group, features_v1, event_timestamp=datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc))
write_features(user_id, feature_group, features_v2, event_timestamp=datetime(2023, 3, 15, 14, 30, tzinfo=timezone.utc))
write_features(user_id, feature_group, features_v3, event_timestamp=datetime(2023, 6, 20, 9, 0, tzinfo=timezone.utc))

# 5. Code demonstrating the reading of user profile features
print("\nReading user profile features:")

read_v1_features = read_features(user_id, feature_group, as_of=datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc))
print(f"Features for '{user_id}' as of 2023-01-01 10:00:00 UTC (v1): {read_v1_features}")

read_v2_features = read_features(user_id, feature_group, as_of=datetime(2023, 3, 15, 14, 30, tzinfo=timezone.utc))
print(f"Features for '{user_id}' as of 2023-03-15 14:30:00 UTC (v2): {read_v2_features}")

read_v3_features_as_of = read_features(user_id, feature_group, as_of=datetime(2023, 6, 20, 9, 0, tzinfo=timezone.utc))
print(f"Features for '{user_id}' as of 2023-06-20 09:00:00 UTC (v3): {read_v3_features_as_of}")

latest_features = read_features(user_id, feature_group)
print(f"Latest features for '{user_id}': {latest_features}")


print("\n--- Demonstrating Item Interaction Features ---")

# 6. Code demonstrating the writing of item interaction features
item_id = 'item_ABC'
item_feature_group = 'item_interaction_features'

# Features for item_ABC at different points in time (from kernel state)
item_features_v1 = {'views_last_24h': 100, 'add_to_cart_last_24h': 5, 'purchases_last_24h': 1, 'avg_time_on_page_last_24h': 120.5}
item_features_v2 = {'views_last_24h': 250, 'add_to_cart_last_24h': 15, 'purchases_last_24h': 3, 'avg_time_on_page_last_24h': 180.0}
item_features_v3 = {'views_last_24h': 500, 'add_to_cart_last_24h': 30, 'purchases_last_24h': 8, 'avg_time_on_page_last_24h': 240.2}
item_features_v4 = {'views_last_24h': 300, 'add_to_cart_last_24h': 10, 'purchases_last_24h': 2, 'avg_time_on_page_last_24h': 150.7}

print("\nWriting item interaction features:")
write_features(item_id, item_feature_group, item_features_v1, event_timestamp=datetime(2023, 1, 10, 12, 0, tzinfo=timezone.utc))
write_features(item_id, item_feature_group, item_features_v2, event_timestamp=datetime(2023, 1, 11, 10, 0, tzinfo=timezone.utc))
write_features(item_id, item_feature_group, item_features_v3, event_timestamp=datetime(2023, 1, 12, 9, 0, tzinfo=timezone.utc))
write_features(item_id, item_feature_group, item_features_v4, event_timestamp=datetime(2023, 1, 13, 9, 0, tzinfo=timezone.utc))


# 7. Code demonstrating the reading of item interaction features
print("\nReading item interaction features:")

demonstration_outputs['item_interaction_features'] = {}

latest_item_features = read_features(item_id, item_feature_group)
print(f"Latest features for '{item_id}': {latest_item_features}")
demonstration_outputs['item_interaction_features']['latest'] = latest_item_features

historical_features_v2 = read_features(item_id, item_feature_group, historical=True)
print(f"Historical features for '{item_id}': {historical_features_v2}")
demonstration_outputs['item_interaction_features']['historical'] = historical_features_v2

print("\nConsolidated code executed successfully.")

# 8. Store the collected outputs into a JSON file
output_file_path = 'output.json'
with open(output_file_path, 'w') as f:
    json.dump(demonstration_outputs, f, indent=4, default=json_serializable_converter)

print(f"\nDemonstration output has been saved to {output_file_path}")