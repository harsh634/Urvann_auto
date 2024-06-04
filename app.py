from flask import Flask, request, send_file, jsonify, render_template
import pandas as pd
import re
import os

app = Flask(__name__)

# Function to extract the quantity from 'Set of <number>' in 'line_item_name'
def extract_set_quantity(line_item_name):
    match = re.search(r'Set of (\d+)', line_item_name)
    if match:
        return int(match.group(1))
    return 1

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            app.logger.error('No file part in request')
            return jsonify({'error': 'No file part'}), 400
        file = request.files['file']
        if file.filename == '':
            app.logger.error('No selected file')
            return jsonify({'error': 'No selected file'}), 400
        if file:
            df = pd.read_csv(file)

            columns_to_fetch = [
                '_id', 'order_id', 'status', 'txn_id', 'email', 'seller_name', 'created_on',
                'created_by', 'shipping_status', 'line_item_name', 'line_item_price',
                'line_item_sku', 'line_item_total', 'line_item_quantity', 'shipping_address_phone',
                'shipping_address_zip', 'shipping_address_full_name', 'shipping_address_address',
                'shipping_address_city', 'client_substore', 'shipping_address_metafields'
            ]

            df = df[columns_to_fetch]
            df.ffill(inplace=True)

            df['total_item_quantity'] = df.apply(
                lambda row: extract_set_quantity(row['line_item_name']) * row['line_item_quantity'],
                axis=1
            )

            line_item_quantity_index = df.columns.get_loc('line_item_quantity')
            df.insert(line_item_quantity_index + 1, 'total_item_quantity', df.pop('total_item_quantity'))
            
            df['shipping_address_country'] = df['shipping_address_address'] + ', ' + df['shipping_address_city']
            shipping_address_city_index = df.columns.get_loc('shipping_address_city')
            df.insert(shipping_address_city_index + 1, 'shipping_address_country', df.pop('shipping_address_country'))

            filtered_df = df[(df['status'] == 'open') & (df['shipping_status'] == 'Not Shipped')]

            def merge_txn_id(group):
                if len(group['txn_id'].unique()) > 1:
                    new_txn_id = group['txn_id'].iloc[0] + "_merge"
                    group['txn_id'] = new_txn_id
                return group

            filtered_df = filtered_df.groupby('shipping_address_address', sort=False).apply(merge_txn_id).reset_index(drop=True)
            filtered_df['GMV'] = filtered_df['line_item_price'] * filtered_df['line_item_quantity']
            filtered_df.insert(line_item_quantity_index + 2, 'GMV', filtered_df.pop('GMV'))

            customer_details = filtered_df[['txn_id', 'email', 'shipping_address_full_name', 'shipping_address_address', 'shipping_address_phone', 'total_item_quantity']].drop_duplicates()
            gmv_per_txn = filtered_df.groupby('txn_id')['GMV'].sum()
            gmv_df = gmv_per_txn.reset_index()
            unique_emails = filtered_df[['txn_id', 'email']].drop_duplicates()
            gmv_df = pd.merge(unique_emails, gmv_df, on='txn_id', how='left')
            gmv_df = gmv_df.sort_values(by='GMV', ascending=False)

            filtered_unique_txn = filtered_df.drop_duplicates(subset=['txn_id'])
            route_columns_from_filtered = filtered_unique_txn[['txn_id', 'email', 'shipping_address_full_name', 'shipping_address_address', 'shipping_address_phone', 'total_item_quantity', 'shipping_address_metafields', 'shipping_address_zip']].copy()
            route_columns_from_filtered.columns = ['Unique Key', 'email', 'Name', 'Address', 'Phone Number', 'Quantity', 'Alternate', 'Pincode']
            route_columns_from_gmv = gmv_df[['txn_id', 'GMV']].copy()
            route_columns_from_gmv.columns = ['Unique Key', 'Total GMV']
            route_df = pd.merge(route_columns_from_filtered, route_columns_from_gmv, on='Unique Key', how='left')

            route_df['order_type'] = 'new'
            route_df['Coordinates'] = ''
            route_df['Link'] = ''
            route_df['Driver Code'] = ''
            route_df['Rank'] = ''
            route_df['Driver Assigned'] = ''
            route_df['Mode'] = route_df['Total GMV'].apply(lambda x: 'vehicle' if x > 450 else 'bike')
            route_df['Status'] = ''
            route_df['Hub Code'] = ''
            route_df['Sector Code'] = ''
            route_df['Hubwise Sector Code'] = ''
            route_df['Final Route Zone'] = ''

            output_file_path = 'filtered_data.xlsx'
            with pd.ExcelWriter(output_file_path, engine='xlsxwriter') as writer:
                filtered_df.to_excel(writer, sheet_name='Filtered Data', index=False)
                gmv_df.to_excel(writer, sheet_name='GMV Data', index=False)
                customer_details.to_excel(writer, sheet_name='Customer Details', index=False)
                route_df.to_excel(writer, sheet_name='Route', index=False)

            app.logger.info('File processed successfully')
            return jsonify({'message': 'File processed successfully', 'file_url': '/download'})
    except Exception as e:
        app.logger.error(f'Error processing file: {str(e)}')
        return jsonify({'error': str(e)}), 500

@app.route('/download')
def download_file():
    try:
        output_file_path = 'filtered_data.xlsx'
        return send_file(output_file_path, as_attachment=True)
    except Exception as e:
        app.logger.error(f'Error sending file: {str(e)}')
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
