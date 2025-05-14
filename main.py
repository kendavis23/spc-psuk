import functions_framework
import pandas as pd
import io
import datetime
from io import StringIO
from pandas_gbq import to_gbq
from google.cloud import storage
from google.cloud import bigquery
from updateCI import updateci

@functions_framework.http
def main(request):
    
    request_json = request.get_json(silent=True)
    request_args = request.args 
    
    bucket = "spc_financials"
    name = "SPC_Monzo.csv"

    df_monzo = read_monzo(bucket, name)         #Read monzo file
    print(df_monzo.head().to_string())

    cost_df = cost(df_monzo)
    print(cost_df.head().to_string())
    table_id = 'spc-sandbox-453019.financials.spc-cost-monzo'
    cost_df.to_gbq(table_id, if_exists='replace')

    return 'Hello World!'


def read_monzo(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob_data = blob.download_as_text()
    data = StringIO(blob_data)
    df = pd.read_csv(data)

    df.columns.values[0] = 'Date'
    df.columns.values[1] = 'Category'
    df.columns.values[2] = 'Xero'
    df.columns.values[3] = 'Description'
    df.columns.values[4] = 'Reconciled'
    df.columns.values[5] = 'Source'
    df.columns.values[6] = 'Amount'
    df.columns.values[7] = 'Balance'

    new_df = df[['Date', 'Description', 'Category', 'Amount']].copy()
    new_df['Ops_Include'] = True
    new_df['Type'] = "Transfer"

    for index, row in new_df.iterrows():
        amount = new_df.loc[index, 'Amount']
        if '(' in amount:
            new_df.loc[index, 'Type'] = "Cost"
        amount = amount.replace('(','')
        amount = amount.replace(')','')
        amount = amount.replace(',','')
        new_df.loc[index, 'Amount'] = amount

    new_df['Date'] = pd.to_datetime(new_df['Date'])
    new_df['Description'] = new_df['Description'].astype(str)
    new_df['Category'] = new_df['Category'].astype(str)
    new_df['Amount'] = new_df['Amount'].astype(float)

    new_df['Category'] = 'other'

    return new_df

def cost(monzo_df):

    cost_df = monzo_df[monzo_df['Type'] == "Cost"]
    cost_df = cost_df.reset_index(drop=True)

    client = bigquery.Client()
    ex_query = """
    SELECT Substring 
    FROM `spc-sandbox-453019.financials.config-ops-exclude` 
    WHERE File = 'monzo' AND Type = 'cost'
    """
    ex_df = client.query(ex_query).to_dataframe()

    rc_query = """
    SELECT Substring, Category 
    FROM `spc-sandbox-453019.financials.config-cost-categories`
    WHERE File = 'monzo'"""
    rc_df = client.query(rc_query).to_dataframe()

    return updateci(rc_df, ex_df, cost_df)
