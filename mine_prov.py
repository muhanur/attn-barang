import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from os.path import exists as file_exists

# Change list of regencies on down_file
down_file = "prov/yogyakarta.txt"
text_file = "Kab-Kot.txt"
down = []
kabkot = []

with open(down_file) as file:
    while line := file.readline():
        down.append(line.strip('\n'))
        
with open(text_file) as file:
    while line := file.readline():
        kabkot.append(line.strip('\n'))

requests_session = requests.Session() 
retries = Retry( 
	total = 3, 
	backoff_factor = 1, 
	status_forcelist = [429, 500, 502, 503, 504] 
)
requests_session.mount("https://", HTTPAdapter(max_retries=retries)) 

def download(url):
    try:
        #html = requests_session.get(url).text
        html = requests.get(url, verify=False).text
        df = pd.read_html(StringIO(html))[0].loc[:, ['Asal', 'Tujuan', 'Volume (ton)']]
        return df
        
    except Exception as e:
        return e

for kabkot_asal in down:
    urls = []
    threads = []
    results = []
    
    if file_exists("{}.csv".format(kabkot_asal)):
        print('Skipping : {}...'.format(kabkot_asal))
        continue
        
    print('Downloading : {}...'.format(kabkot_asal))
    
    for kabkot_tujuan in kabkot:
        url = "https://attn-barang.dephub.go.id/data/site/front/?page=pergerakan&jenis=mat&komoditas=General+Cargo+Non+Makanan&data_jenis=kab2kab&kota_asal={}&kota_tujuan={}&generate="
        urls.append(url.format(kabkot_asal, kabkot_tujuan))
        
    start_time = datetime.now()
    
    with ThreadPoolExecutor(max_workers=32) as executor:
        for url in urls:
            threads.append(executor.submit(download, url))
            
        for task in as_completed(threads):
            results.append(task.result())
    
    df = pd.concat(results)
    df.columns = ["Asal", "Tujuan", "Volume (ton)"]
    df.to_csv("{}.csv".format(kabkot_asal), index=None)
    
    end_time = datetime.now()
    print('Time Processing: {}'.format(end_time - start_time))
