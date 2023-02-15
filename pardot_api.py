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
        business_unit = credentials['business_unit_id']
        combined_pw_token = f"{credentials['password']}{credentials['security_token']}" #Used below as the password param for auth to salesforce
       
        #  Get access token
        resp = self.s.post(f"https://{host}{endpoint}?grant_type={credentials['grant_type']}&client_id={credentials['client_id']}&client_secret={credentials['client_secret']}&username={credentials['username']}&password={combined_pw_token}")
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