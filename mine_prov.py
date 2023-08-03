import requests
import pandas as pd
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from io import StringIO
from os.path import exists as file_exists

# Change list of regencies on down_file
down_file = "prov/bali.txt"
text_file = "Kab-Kot.txt"
down = []
kabkot = []

url = "https://attn-barang.dephub.go.id/data/site/front/?page=pergerakan&jenis=mat&komoditas=General+Cargo+Non+Makanan&data_jenis=kab2kab&kota_asal={}&kota_tujuan={}&generate="
headers = {
'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 \
(KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'
}

urllib3.disable_warnings()

def request_retry(url, num_retries=3, success_list=[200, 404], **kwargs):
    for _ in range(num_retries):
        try:
            response = requests.get(url, **kwargs)
            if response.status_code in success_list:
                ## Return response if successful
                return response
        except requests.exceptions.ConnectionError:
            pass
    return None
    
def download(url):
    try:
        html = request_retry(url, headers=headers, verify=False).text
        df = pd.read_html(StringIO(html))[0].loc[:, ['Asal', 'Tujuan', 'Volume (ton)']]
        return df
        
    except requests.exceptions.RequestException as e:
        return e

with open(down_file) as file:
    while line := file.readline():
        down.append(line.strip('\n'))
        
with open(text_file) as file:
    while line := file.readline():
        kabkot.append(line.strip('\n'))

for kabkot_asal in down:
    url_list = []
    threads = []
    results = []
    
    for kabkot_tujuan in kabkot:
        url_list.append(url.format(kabkot_asal, kabkot_tujuan))
    
    if file_exists("{}.csv".format(kabkot_asal)):
        print('Skipping: {}...'.format(kabkot_asal))
        continue
        
    print('Downloading : {}...'.format(kabkot_asal))
    start_time = datetime.now()
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        for url in url_list:
            threads.append(executor.submit(download, url))
            
            for task in as_completed(threads):
                results.append(task.result())
                
    df = pd.concat(results)
    
    df.columns = ["Asal", "Tujuan", "Volume (ton)"]
    df.to_csv("{}.csv".format(kabkot_asal), index=None)
    
    end_time = datetime.now()
    print('Time Processing: {}'.format(end_time - start_time))
