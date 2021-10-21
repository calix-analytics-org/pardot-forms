import requests
import json

class Pardot():
    def __init__(self, credentials):
         # Initialize forms api session
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        self.s = requests.Session()
        self.s.headers = headers
        
        host = 'login.salesforce.com'
        endpoint = '/services/oauth2/token'
        business_unit = credentials['business_unit']

        #  Get access token
        resp = self.s.post(f'https://{host}{endpoint}', data = credentials)
        access_token = json.loads(resp.content)['access_token']

        print(f'Token acquired: {access_token}')

        headers = {'Authorization' : f'Bearer {access_token}',
                    'Pardot-Business-Unit-Id' : business_unit}

        self.s.headers.update(headers)

        print('Pardot authentication successful')

    def get_forms(self, query=''):
        host = 'pi.pardot.com'
        endpoint = '/api/form/version/3/do/query?'
        full_url = f'https://{host}{endpoint}{query}'
        #print(f'SENDING: {full_url}')
        resp = self.s.get(full_url)
        return resp