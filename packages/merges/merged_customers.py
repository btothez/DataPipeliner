import packages.config as config
import requests
import json
import sys

class MergedCustomers():

    def __init__(self):
        self.config = config.Config()
        self.merged_dict = {}
        self.api_creds = self.config.api_creds
        self.api_urls = self.config.api_urls

    def get_merged_dict(self):
        self.authenticate()
        self.get_merges()
        return self.merged_dict

    def authenticate(self):
        resp = requests.post(self.api_urls['auth_url'],
                            data=self.api_creds,
                            headers={'Connection':'close'})

        resp_dict = json.loads(resp.text)
        resp.close()

        try:
            self.token = resp_dict['token']
        except Exception as e:
            print("Trouble getting token from api")
            print(e)
            sys.exit()

    def get_merges(self):
        resp = requests.get(self.api_urls['merged_endpoint'],
            headers={'Authorization': 'Bearer {}'.format(self.token)}
            )

        old_merged_dict = json.loads(resp.text)

        try:
            if 'error' not in old_merged_dict:

                for old_id_str in old_merged_dict:
                    old_id_int = int(old_id_str)
                    self.merged_dict[old_id_int] = old_merged_dict[old_id_str]
        except Exception as e:
            print("Error in converting merged customer ids")
            print(e)
