import boto3
import json

query_id = 'a4f70607-4143-4059-b0c0-0ca9147cd2b3'  # Updated query ID for month distribution

athena = boto3.client('athena')
result = athena.get_query_results(QueryExecutionId=query_id)
rows = result['ResultSet']['Rows']

# Get column names
column_names = [col['VarCharValue'] for col in rows[0]['Data']]
print('\nColumns:', column_names)

# Print data distribution by month
print('\nData distribution by year/month:')
for i in range(1, len(rows)):
    row_data = {}
    for j in range(len(column_names)):
        if 'VarCharValue' in rows[i]['Data'][j]:
            row_data[column_names[j]] = rows[i]['Data'][j]['VarCharValue']
        else:
            row_data[column_names[j]] = None
    print(json.dumps(row_data, indent=2)) 