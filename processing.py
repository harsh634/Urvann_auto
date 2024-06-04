import pandas as pd
import re

# Replace 'your_file.csv' with the path to your CSV file
file_path = '665d8b56e75827c1bc922b5e.csv'

# List of columns you want to fetch
columns_to_fetch = [
    '_id', 'order_id', 'status', 'txn_id', 'email', 'seller_name', 'created_on',
    'created_by', 'shipping_status', 'line_item_name', 'line_item_price',
    'line_item_sku', 'line_item_total', 'line_item_quantity', 'shipping_address_phone',
    'shipping_address_zip', 'shipping_address_full_name', 'shipping_address_address',
    'shipping_address_city', 'client_substore', 'shipping_address_metafields'
]

# Read the CSV file and fetch specific columns
df = pd.read_csv(file_path, usecols=columns_to_fetch)

# Forward fill missing values
df.ffill(inplace=True)

# Function to extract the quantity from 'Set of <number>' in 'line_item_name'
def extract_set_quantity(line_item_name):
    match = re.search(r'Set of (\d+)', line_item_name)
    if match:
        return int(match.group(1))
    return 1

# Create a new column 'total_item_quantity'
df['total_item_quantity'] = df.apply(
    lambda row: extract_set_quantity(row['line_item_name']) * row['line_item_quantity'],
    axis=1
)

# Get the index of 'line_item_quantity' column
line_item_quantity_index = df.columns.get_loc('line_item_quantity')

# Insert the 'total_item_quantity' column next to 'line_item_quantity'
df.insert(line_item_quantity_index + 1, 'total_item_quantity', df.pop('total_item_quantity'))

# Concatenate 'shipping_address_address' and 'shipping_address_city' columns to create 'shipping_address_country'
df['shipping_address_country'] = df['shipping_address_address'] + ', ' + df['shipping_address_city']

# Get the index of 'shipping_address_city' column
shipping_address_city_index = df.columns.get_loc('shipping_address_city')

# Insert the 'shipping_address_country' column next to 'shipping_address_city'
df.insert(shipping_address_city_index + 1, 'shipping_address_country', df.pop('shipping_address_country'))

# Apply the filter: status = 'open' and shipping_status = 'Not Shipped'
filtered_df = df[(df['status'] == 'open') & (df['shipping_status'] == 'Not Shipped')]

# Function to merge txn_id if the shipping_address_address is the same but txn_id is different
def merge_txn_id(group):
    if len(group['txn_id'].unique()) > 1:
        new_txn_id = group['txn_id'].iloc[0] + "_merge"  # Create new txn_id with "merge" suffix
        group['txn_id'] = new_txn_id
    return group

filtered_df = filtered_df.groupby('shipping_address_address', sort=False).apply(merge_txn_id).reset_index(drop=True)

# Create a new column 'GMV' as line_item_price * line_item_quantity
filtered_df['GMV'] = filtered_df['line_item_price'] * filtered_df['line_item_quantity']

# Insert the 'GMV' column next to 'total_item_quantity'
filtered_df.insert(line_item_quantity_index + 2, 'GMV', filtered_df.pop('GMV'))

# Create the customer_details DataFrame
customer_details = filtered_df[['txn_id', 'email', 'shipping_address_full_name', 'shipping_address_address', 'shipping_address_phone', 'total_item_quantity']].drop_duplicates()

# Group by txn_id and calculate total GMV for each txn_id
gmv_per_txn = filtered_df.groupby('txn_id')['GMV'].sum()
# Create a DataFrame from the grouped series
gmv_df = gmv_per_txn.reset_index()
# Merge unique email IDs with the GMV DataFrame
unique_emails = filtered_df[['txn_id', 'email']].drop_duplicates()
gmv_df = pd.merge(unique_emails, gmv_df, on='txn_id', how='left')
# Sort the GMV DataFrame in descending order based on GMV
gmv_df = gmv_df.sort_values(by='GMV', ascending=False)

# Select specific columns for the route DataFrame and ensure unique txn_id
filtered_unique_txn = filtered_df.drop_duplicates(subset=['txn_id'])

# Select and rename columns from filtered_unique_txn
route_columns_from_filtered = filtered_unique_txn[['txn_id', 'email', 'shipping_address_full_name', 'shipping_address_address', 'shipping_address_phone', 'total_item_quantity', 'shipping_address_metafields', 'shipping_address_zip']].copy()
route_columns_from_filtered.columns = ['Unique Key', 'email', 'Name', 'Address', 'Phone Number', 'Quantity', 'Alternate', 'Pincode']

# Select and rename columns from gmv_df
route_columns_from_gmv = gmv_df[['txn_id', 'GMV']].copy()
route_columns_from_gmv.columns = ['Unique Key', 'Total GMV']

# Merge the dataframes on 'Transaction ID'
route_df = pd.merge(route_columns_from_filtered, route_columns_from_gmv, on='Unique Key', how='left')

# Add the 'order_type' column
route_df['order_type'] = 'new' 
route_df['Coordinates'] = '' 
route_df['Link'] = '' 
route_df['Driver Code'] = '' 
route_df['Rank'] = '' 
route_df['Driver Assigned'] = '' 
# Add a new column based on the condition
route_df['Mode'] = route_df['Total GMV'].apply(lambda x: 'vehicle' if x > 450 else 'bike')
route_df['Status'] = '' 
route_df['Hub Code'] = '' 
route_df['Sector Code'] = '' 
route_df['Hubwise Sector Code'] = '' 
route_df['Final Route Zone'] = '' 


# Specify the output file path
output_file_path = 'filtered_data.xlsx'

# Create a Pandas Excel writer using XlsxWriter as the engine
with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
    # Write the original filtered data to the first sheet
    filtered_df.to_excel(writer, sheet_name='Filtered Data', index=False)
    
    # Write GMV DataFrame to another sheet
    gmv_df.to_excel(writer, sheet_name='GMV Data', index=False)

    # Write the customer_details DataFrame to another sheet
    customer_details.to_excel(writer, sheet_name='Customer Details', index=False)

    # Write the route DataFrame to another sheet
    route_df.to_excel(writer, sheet_name='Route', index=False)

print("Data has been saved to Excel file with multiple sheets.")
