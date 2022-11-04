import pandas as pd
import numpy as np
import requests
from urllib.parse import urljoin
import psycopg2
import boto3
import json

API_URL = 'https://api.ion-gov.com'
AUTHENTICATION_SERVER = 'auth.ion-gov.com'
AUTH_URL = 'auth.ion-gov.com'
REQUEST_URL = f'https://{API_URL}/graphql'

ARCHIVE_RUNS = '''
mutation updateRunStatus($inputs: ArchiveRunInput!){
  archiveRun(input: $inputs){
    run{
      id title status
    }
  }
}
'''


class IonMutation:

    def __init__(self):
        self.date = str(input('ARCHIVE TODO RUNS CREATED BEFORE WHICH DATE (FORMAT YEAR-MONTH-DAY): '))
        self.db_creds = self.grab_creds('ion/db/psql')
        self.api_creds = self.grab_creds('ion/api/creds')
        self.access_token = self.get_access_token()
        self.runs = self.get_run_ids()

    def grab_creds(self, sec_id: any):
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=sec_id)
        database_secrets = json.loads(response['SecretString'])
        return database_secrets

    def get_access_token(self):
        payload = {
            'grant_type': 'client_credentials',
            'client_id': self.api_creds['clientId'],
            'client_secret': self.api_creds['clientSecret'],
            'audience': API_URL
        }

        headers = {'content-type': 'application/x-www-form-urlencoded'}

        auth_url = urljoin(f'https://{AUTHENTICATION_SERVER}', '/auth/realms/api-keys/protocol/openid-connect/token',
                           'oauth/token')
        res = requests.post(auth_url, data=payload, headers=headers)
        if res.status_code != 200:
            raise RuntimeError('An error occurred in the API request')
        return res.json()['access_token']

        # sends query using API to graphQL

    def call_api(self, query, variables):
        """Calls ions GraphQL api."""
        headers = {
            'Authorization': f'{self.access_token}',
            'Content-Type': 'application/json'
        }
        # print(variables)
        res = requests.post(REQUEST_URL,
                            headers=headers,
                            json={'query': query, 'variables': variables})
        if res.status_code != 200:
            raise RuntimeError('An error occurred in the API request')
        return res.json()['data']

        # establishes connection to db

    def connect(self):
        return psycopg2.connect(
            database="postgres", user=self.db_creds['username'],
            password=self.db_creds['password'],
            host=self.db_creds['host'],
            port=self.db_creds['port']
        )

    def get_run_ids(self):

        conn = self.connect()

        conn.autocommit = True

        cursor = conn.cursor()

        query = "set search_path to epirussystems_com; with run_status as(SELECT r.id, r.part_inventory_id,CASE WHEN ('redline' = ANY (array_agg(rs.status))) THEN 'redline' WHEN ('hold' = ANY (array_agg(rs.status))) THEN 'hold'WHEN ('failed' = ANY (array_agg(rs.status))) THEN 'failed' WHEN ('complete' = ALL (array_agg(rs.status))) THEN 'complete' WHEN ('canceled' = ALL (array_agg(rs.status))) THEN 'canceled' WHEN ('todo' = ALL (array_agg(rs.status))) THEN 'todo' WHEN ('in_progress' = ANY (array_agg(rs.status))) THEN 'in_progress' WHEN ('canceled' = ALL (array_remove(array_agg(rs.status),'complete'))) THEN 'partial_complete' WHEN ('complete' = ANY (array_agg(rs.status))) THEN 'in_progress' ELSE 'complete' END AS run_status FROM epirussystems_com.runs r JOIN epirussystems_com.run_steps rs ON r.id = rs.run_id GROUP BY r.id) select runs.id, _etag from runs join run_status on runs.id = run_status.id where run_status = 'todo' and date(_created) < %s"
        cursor.execute(query, [self.date])

        result = cursor.fetchall()

        df1 = pd.DataFrame(result, columns=['run_id', '_etag'])

        #print(df1)

        return(df1)

    def close_runs(self):
        runs = self.runs
        for run_id in runs['run_id']:
            bm = runs['run_id'] == run_id
            dff = runs[bm]
            etag = dff['_etag'].tolist() #the tolist is needed because the etag will be an object that includes the ID
            inputt = {
                'id': run_id,
                'etag': etag[0],
                'archived': True
            }
            self.call_api(ARCHIVE_RUNS, {'inputs': inputt})
            print(f'archived run_id: {run_id}')
        print("ALL RUNS ARCHIVED :)")


def main():
    try:
        ionmutation = IonMutation()
        ionmutation.close_runs()
    except Exception as e:
        print(f'go fuck yourself asshole: {e}')


if __name__ == "__main__":
    main()
