import pandas as pd
import asyncio
from time import time

# from ebook_scrapper.scapper import get_book_info_urls
from ebook_scrapper.book_info_url import BookInfoUrl

def get_book_info_urls_main():
    start_time = time()
    book_info_url = BookInfoUrl()
    
    asyncio.run(book_info_url.get_all(1, 5))
    end_time =  time()
    print(f"Execution time: {end_time- start_time} seconds")

# if __name__ == '__main__':
#     get_book_info_urls_main()