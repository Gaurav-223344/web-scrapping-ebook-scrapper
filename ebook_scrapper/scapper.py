import httpx
import asyncio
import requests
import inspect
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool

def generate_url(_url:str, page_number:int) -> str:
    """generate url with page number

    Args:
        url (str): url to scrape
        page_number (int): page number of the url

    Returns:
        str: modified url with pagination
    """
    return _url.format(page_number=page_number)

async def fetch(_url:str):
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(_url, timeout=60)
            print(f"{_url} : ",response.status_code)
            if response.status_code == 200:
                return response.text
            else:
                return None
    except Exception as e:
        return None
    
    
  
async def create_soup(_url:str) -> BeautifulSoup:
    """get successful response using requests and then
    create soup object

    Args:
        url (str): url to create soup

    Returns:
        BeautifulSoup: soup object
    """
    try:
        response_text = await fetch(_url)
        if response_text is None:
            return response_text
        soup = BeautifulSoup(response_text, "html.parser")
        return soup
    except Exception as e:
        raise e


def get_book_articles(soup:BeautifulSoup):
    articles = soup.find_all("article", class_="post")
    return articles

def get_book_name(book_article:BeautifulSoup):
    return book_article.find(class_="post-title").text.strip()

def get_book_info_link(book_article:BeautifulSoup):
    return book_article.find(class_="post-title").a["href"].strip()

async def get_book_info_urls_for_page(_url:str) -> dict:
    """get book info urls from given url by using BeautifulSoup

    Args:
        url (str): page url to scrape

    Returns:
        dict: { name_of_book : url_of_book }
    """
    soup = await create_soup(_url)
    if soup is None:
        return {}
    articles = get_book_articles(soup)
    # urls = {}
    # for article in articles:
    #     book_name = get_book_name(article)
    #     book_info_url = get_book_info_link(article)
    #     urls[book_name] = book_info_url
    _urls = {
        get_book_name(article): get_book_info_link(article) for article in articles
        }
    return _urls

async def create_csv_using_df(_df:pd.DataFrame, name:str):
    _df.to_csv(f'{name}.csv', index=False)
    print("CSV saved successfully!")
    
async def create_json_using_df(_df:pd.DataFrame, name:str):
    _df.to_json(f'{name}.json', orient='records')
    print("JSON saved successfully!")

async def create_dataframe(_list_of_dicts:list[dict]):
    dict_data = {key: value for dictionary in _list_of_dicts for key, value in dictionary.items()}

    df = pd.DataFrame(list(dict_data.items()), columns=['book', 'url'])
    
    await asyncio.gather(
        create_csv_using_df(df, "urls_of_book_info"),
        create_json_using_df(df, "urls_of_book_info")
    )

def get_arguments(func, values_list):
    signature = inspect.signature(func)
    parameters = list(signature.parameters.values())
    
    if len(parameters) != len(values_list):
        raise ValueError("The length of values_list must match the number of function parameters.")
    
    return {param.name: value for param, value in zip(parameters, values_list)}

def make_sync(func, params):
    return asyncio.run(func(**params))

async def get_book_info_urls(base_url:str, last_page:int = 2):

    _urls = [generate_url(base_url, page_number) for page_number in range(1, last_page+1)]
    # _tasks = [get_book_info_urls_for_page(_url) for _url in _urls]
    # list_of_dicts = await asyncio.gather(*_tasks)
    
    arguments = [(get_book_info_urls_for_page, get_arguments(get_book_info_urls_for_page, [_url])) for _url in _urls]
    print(arguments)
    
    with Pool(processes=4) as pool: 
        results = pool.starmap(make_sync, arguments)
    
    print(results)

    # list_of_dicts = []
    # for _url in _urls:
    #     list_of_dicts.append(await get_book_info_urls_for_page(_url))
    # await create_dataframe(list_of_dicts)





async def main():
    _urls = [generate_url(BASE_URL, page_number) for page_number in range(1, 10+1)]
    _tasks = [get_book_info_urls_for_page(_url) for _url in _urls]
    print(_tasks)
    results = await asyncio.gather(*_tasks)
    print(results)

if __name__ == '__main__':
    BASE_URL:str = "https://allbooksworld.com/page/{page_number}/"
    _urls = [generate_url(BASE_URL, page_number) for page_number in range(1, 10+1)]
    # tasks = [get_book_info_urls(_url) for _url in _urls]
    # # url = generate_url(BASE_URL, 1)
    # # html = create_soup(url)
    asyncio.run(main())
