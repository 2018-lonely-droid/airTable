import os
from dotenv import load_dotenv
import requests
import json
import csv
import time

# Load .env file
load_dotenv()
airTableApiKey = os.getenv('AT_API_KEY')


def getBase(baseName, baseId):
    for base in baseName:
        hasMore = True
        offset = ''
        cnt = 0
        lst = []
        while hasMore:
            url = f'https://api.airtable.com/v0/{baseId}/{base}?offset={offset}&api_key={airTableApiKey}'
            headers = {
                'accept': 'application/json',
                'content-type': 'application/json'
            }
            res = json.loads(requests.request('GET', url=url, headers=headers).text)
            # print(json.dumps(res, indent=2))

            if 'records' in res:
                for x in res['records']:
                    dict = x['fields']
                    dict.update({'id': x['id']})
                    cnt += 1
                    lst.append(x['fields'])

            # If there is an error stop
            else:
                print(res)
                exit()

            # Pagination offset
            if 'offset' in res:
                offset = res['offset']
            else:
                hasMore = False

            # Sleep - Rate limit is 5 requests per second
            time.sleep(.3)

        # Base Count
        print('Base: ', base, 'Base count: ', cnt)

        # Change base name for filenames
        base_fn = base.replace('/', '_')
        base_fn = base_fn.replace(' ', '_')
        base_fn = base_fn.replace('.', '')

        # Fix code names with the real name
        base_fn = base_fn.replace('tblNBBcIQnoRrq3h8', 'Spkr_Magmt')
        base_fn = base_fn.replace('tblDfZZdRG7zpecGF', 'Spkr_Database')
        base_fn = base_fn.replace('tbll4cuWsCAXNBAbP', 'News-Updates')

        # Store as JSON
        with open(f'{base_fn}.json', 'w') as f:
            json.dump(lst, f)

        # Create CSV
        keys = lst[0].keys()
        with open(f'{base_fn}.csv', 'w', encoding='utf-8', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore')
            dict_writer.writeheader()
            dict_writer.writerows(lst)


def main():
    # Alliance CRM
    baseName = ['Organizations', 'Outreach', 'Tasks', 'Programs', 'Sponsorships', 'tbll4cuWsCAXNBAbP',
                'Relationship Owner(s)', '2021 Benefits', '2020 Benefits', '2019 Benefits', '2019 Revenue Tracker']
    baseId = 'appwaKZAEZXGumbJB'
    getBase(baseName, baseId)

    # Alliance Events Management
    baseName = ['Events', 'tblNBBcIQnoRrq3h8', 'Capacity', 'Content', 'tblDfZZdRG7zpecGF', 'Funders', 'Affiliation',
                'Program Planning', 'Program Planning 2', 'Resources', 'Outreach', 'Topics', 'Mentions', 'Inquiries',
                'Venues', 'Vendors']
    baseId = 'appIyA2lrGGZ2XaWt'
    getBase(baseName, baseId)


main()