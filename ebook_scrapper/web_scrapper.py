import httpx
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import inspect
from typing import List

class WebScrapper:
    
    async def fetch(self, _url: str):
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.get(_url, timeout=60)
                print(f"{_url} : ", response.status_code)
                if response.status_code == 200:
                    return response.text
                else:
                    return None
        except Exception as e:
            return None

    async def create_soup(self, _url: str) -> BeautifulSoup:
        """get successful response using requests and then
        create soup object

        Args:
            url (str): url to create soup

        Returns:
            BeautifulSoup: soup object
        """
        try:
            response_text = await self.fetch(_url)
            if response_text is None:
                return response_text
            soup = BeautifulSoup(response_text, "html.parser")
            return soup
        except Exception as e:
            raise e
    
    async def create_csv_using_df(self, _df: pd.DataFrame, path: str, index:bool=False):
        _df.to_csv(path, index=index)
        print(f"csv saved successfully!")

    async def create_json_using_df(self, _df: pd.DataFrame, path: str):
        _df.to_json(path, orient="records")
        print(f"json saved successfully!")

    async def create_dataframe(self, _list_of_dicts: list[dict], path:str, index:bool=False, columns=[]):
        if(len(columns) != 0):
            df = pd.DataFrame(_list_of_dicts, columns=columns)
        else:
            df = pd.DataFrame(_list_of_dicts)
            
        await asyncio.gather(
            self.create_csv_using_df(df, path, index),
            # self.create_json_using_df(df, path),
        )

    def get_arguments(self, func, values_list):
        signature = inspect.signature(func)
        parameters = list(signature.parameters.values())

        if len(parameters) != len(values_list):
            raise ValueError(
                "The length of values_list must match the number of function parameters."
            )

        return {param.name: value for param, value in zip(parameters, values_list)}

    def make_sync(self, func, params):
        return asyncio.run(func(**params))
    
    
if __name__ == '__main__':
    scraper = WebScrapper()
    data = [{}, {"a":1}, {"a":7}]
    asyncio.run(scraper.create_dataframe(data, "test.csv"))