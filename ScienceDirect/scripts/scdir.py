import requests
from multiprocessing.pool import ThreadPool
from functools import partial
from tqdm import tqdm
import time
from bs4 import BeautifulSoup
import unicodedata

SEARCH_API_KEY = "KEY"
ARTICLE_TEXT_API_KEY = "KEY"


def extract_urls(entry):
    pass


def search_request(start: int, query: str, count: int, wait_time: int, return_fields=None):
    if wait_time > 5:
        return []
    
    time.sleep(wait_time)
    r = requests.put(f"https://api.elsevier.com/content/search/sciencedirect",
             headers={"X-ELS-APIKey": SEARCH_API_KEY},
             json={
                  "qs": query,
                  "display": {
                      "show": count,
                      "offset": start
                  },
                  "filters": {
                    "openAccess": 'true',
                    "articleTypes": ["REV","CFLA"]
                  },
                })
    
    if r.status_code == 400:
        return []
    
    if r.status_code != 200:
        print(f"Retrying... {r.status_code}")
        return search_request(start, query, wait_time=wait_time+1, count=count)

    response = r.json()
    if return_fields is not None:
        response = [[i[field] for field in return_fields]for i in response['results']]
        
    return response


def find(query:str, wait_time: int, on_page_count: int, n_threads: int = 2, return_fields: tuple = ("pii", "title", "loadDate")):
    response = search_request(query=query, start=0, wait_time=wait_time, count=on_page_count)
    if not response:
        print(":(")
    
    count = int(response['resultsFound'])
    print(f"RESULTS COUNT: {count}")
    
    start_idxs = range(on_page_count, count, 100)
    pool = ThreadPool(n_threads)
    
    url_getter = partial(search_request, **{"query": query,
                                            "return_fields": return_fields,
                                            "wait_time": wait_time,
                                            "count": on_page_count})
    
    items = [[i[field] for field in return_fields]for i in response['results']]
    yield items
        
    for items in tqdm(pool.imap_unordered(url_getter, start_idxs), total=len(start_idxs)):
        if not items:
            pool.close()
            break
        yield items


def get_text_batch(piis: list, n_threads=3):
    pool = ThreadPool(n_threads)
    for item in tqdm(pool.imap_unordered(get_text, piis), total=len(piis)):
        if item[1] is None:
            break
        yield item


def get_text(pii: str, wait_time=1):
    if wait_time > 5:
        return pii, None
    
    r = requests.get(f"https://api.elsevier.com/content/article/pii/{pii}",
            headers={"X-ELS-APIKey": ARTICLE_TEXT_API_KEY},
            )
    if r.status_code != 200:
        get_text(pii, wait_time=wait_time+1)
    
    soup = BeautifulSoup(r.text, 'lxml')
    text = ' '.join([unicodedata.normalize("NFKD", i.text).replace('\n', '') for i in soup.find_all("ce:para")])
    if text == '':
        get_text(pii, wait_time=wait_time+1)
    
    return pii, text