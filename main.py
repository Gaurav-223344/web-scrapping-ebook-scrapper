import pandas as pd
import asyncio
from time import time

from ebook_scrapper.scapper import get_book_info_urls




if __name__ == '__main__':
    start_time = time()
    BASE_URL:str = "https://allbooksworld.com/page/{page_number}/"
    list_of_dicts = asyncio.run(get_book_info_urls(BASE_URL, 2630))
    end_time =  time()
    print(f"Execution time: {end_time- start_time} seconds")