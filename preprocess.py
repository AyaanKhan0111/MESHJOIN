import pandas as pd

# Load the CSV file
products_file_path = "products_data.csv"
products_data = pd.read_csv(products_file_path)

# Assign new supplier IDs
supplier_name_to_id = {}
current_id = 1

for idx, row in products_data.iterrows():
    supplier_name = row['supplierName']
    if supplier_name not in supplier_name_to_id:
        supplier_name_to_id[supplier_name] = current_id
        current_id += 1
    products_data.at[idx, 'supplierID'] = supplier_name_to_id[supplier_name]

# Convert the productPrice column to numeric
# Remove the dollar sign ($) and convert productPrice to numeric
products_data['productPrice'] = products_data['productPrice'].str.replace('$', '').astype(float)

# Save the cleaned data to a new file
cleaned_file_path = "products_data_cleaned.csv"
products_data.to_csv(cleaned_file_path, index=False)

print(f"Cleaned data saved to {cleaned_file_path}")
