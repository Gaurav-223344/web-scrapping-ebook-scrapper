
import asyncio
import os
from typing import List
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool
from ebook_scrapper.web_scrapper import WebScrapper
from ebook_scrapper.book_info_url import BookInfoUrl

class BookDetails(WebScrapper):
    
    def __init__(self, first_page:int, last_page:int, download_urls:bool=False):
        self.book_info_urls:dict = asyncio.run(BookInfoUrl().get_all(first_page, last_page, download_urls))

    async def get_all_book_data(self, url):
        soup = await self.create_soup(url)
        return self.get_data(soup)
    
    async def run(self):
        _urls = (
            book_info_url.get('url')
            for book_info_url in self.book_info_urls
        )
        
        arguments = [
            (
                self.get_all_book_data,
                self.get_arguments(self.get_all_book_data, [_url]),
            )
            for _url in _urls
        ]

        results = []
        with Pool(processes=20) as pool:
            results = pool.starmap(self.make_sync, arguments)

        dict_data = [item for result in results for item in result]
        output_path = os.path.join('data', "ebooks_details")
        # if download:
        #     
        #     await self.create_dataframe(dict_data, output_path, True)

        await self.create_dataframe(dict_data,  output_path, True)
            
    def get_all_required_tags(self, soup:BeautifulSoup) -> List[BeautifulSoup]:
        try:
            section = soup.find('section', id='content')
            h2_summary = list(filter(lambda ele: 'summary' in ele.text.lower() ,section.find_all('h2')) )
            all_next_siblings = h2_summary[0].find_next_siblings()
            return all_next_siblings
        except Exception as e:
            return []
    
    def generate_summary_using_tags(self, tags:List[BeautifulSoup]) -> str:
        try:
            return " ".join(( p.text for p in tags))
        except Exception as e:
            return ""
    
    def generate_details_using_tag(self, tag:BeautifulSoup) -> dict:
        try:
            li_details = tag.find_all('li')
            return {key: value for li in li_details if (split_text := li.text.split(":", 1)) and len(split_text) == 2 and (key := split_text[0]) and (value := split_text[1])}
        except:
            return {}

    
    
    def get_data(self, soup:BeautifulSoup) -> dict:
        
        required_tags:List[BeautifulSoup] = self.get_all_required_tags(soup)
        if len(required_tags) == 0 : return {}
        
        p_summary:List[BeautifulSoup] = []
        ul_details:BeautifulSoup = None

        for ele in required_tags:
            if ele.name == 'p':
                p_summary.append(ele)
                
            if ele.name == 'ul':
                ul_details = ele
                break
        
        summary:str = self.generate_summary_using_tags(p_summary)
        book_details:dict = self.generate_details_using_tag(ul_details)
        book_details["Summary"] = summary
        return book_details

if __name__  == "__main__":
    book_details = BookDetails(1, 1, True)
    asyncio.run(book_details.run())