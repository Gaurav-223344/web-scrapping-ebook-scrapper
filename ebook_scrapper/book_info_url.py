import asyncio
import inspect
import pandas as pd
import os
from bs4 import BeautifulSoup
from multiprocessing import Pool
from ebook_scrapper.web_scrapper import WebScrapper


class BookInfoUrl(WebScrapper):

    def __init__(self) -> None:
        self._BASE_URL: str = "https://allbooksworld.com/page/{page_number}/"

    def generate_url(self, _url: str, page_number: int) -> str:
        """generate url with page number

        Args:
            url (str): url to scrape
            page_number (int): page number of the url

        Returns:
            str: modified url with pagination
        """
        return _url.format(page_number=page_number)

    def get_book_articles(self, soup: BeautifulSoup):
        _articles = soup.find_all("article", class_="post")
        return _articles

    def get_book_name(self, book_article: BeautifulSoup):
        return book_article.find(class_="post-title").text.strip()

    def get_book_info_link(self, book_article: BeautifulSoup):
        return book_article.find(class_="post-title").a["href"].strip()

    async def get_book_info_urls_for_page(self, _url: str) -> list:
        """get book info urls from given url by using BeautifulSoup

        Args:
            url (str): page url to scrape

        Returns:
            list: [{ name_of_book : url_of_book }]
        """

        soup = await self.create_soup(_url)
        if soup is None:
            return {}
        articles = self.get_book_articles(soup)
        _urls = [
            {
                "name": self.get_book_name(article), 
                "url" : self.get_book_info_link(article)
            } for article in articles]
        #     self.get_book_name(article): self.get_book_info_link(article) for article in articles
        # }
        # _urls = {}
        # for article in articles:
        #     book_name = self.get_book_name(article)
        #     book_info_link = self.get_book_info_link(article)
        #     _urls[book_name] = book_info_link
        return _urls

    async def get_all(self, initial_page: int = 1, last_page: int = 10, download=False):

        if initial_page <= 0:
            raise Exception("Wrong Page")
        print("Getting urls for details information of books...")
        _urls = (
            self.generate_url(self._BASE_URL, page_number)
            for page_number in range(initial_page, last_page + 1)
        )

        # # normal loop
        # for _url in _urls:
        #     await self.get_book_info_urls_for_page(_url)

        # # async await
        # tasks = [self.get_book_info_urls_for_page(_url) for _url in _urls]

        # results = await asyncio.gather(*tasks)

        # parallel multiprocessing
        arguments = [
            (
                self.get_book_info_urls_for_page,
                self.get_arguments(self.get_book_info_urls_for_page, [_url]),
            )
            for _url in _urls
        ]

        results = []
        with Pool(processes=20) as pool:
            results = pool.starmap(self.make_sync, arguments)

        dict_data = [item for result in results for item in result]
        if download:
            output_path = os.path.join('data', "urls_of_book_info")
            await self.create_dataframe(dict_data, output_path, True)

        print("----" * 20)
        return dict_data


if __name__ == '__main__':
    from time import time
    start_time = time()
    book_info_url = BookInfoUrl()
    BASE_URL:str = "https://allbooksworld.com/page/{page_number}/"
    asyncio.run(book_info_url.get_all(1, 1))
    end_time =  time()
    print(f"Execution time: {end_time- start_time} seconds")
