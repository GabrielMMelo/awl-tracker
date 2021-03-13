import datetime
from functools import reduce
import locale
import os
from pathlib import Path

from dotenv import load_dotenv
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from awl.spiders.awl import AWLSpider
from services.gcp.storage import GCPStorage


load_dotenv(dotenv_path=".env")


class ETL:
    DATA_PATH = Path().joinpath('data/')
    storage = GCPStorage()
    _bucket_name = os.getenv('BUCKET_NAME')

    def _to_gcp(self, source_path, target_path):
        self.storage.upload_blob(
            self._bucket_name,
            source_path,
            target_path
        )


class Extract(ETL):
    def __init__(self, spider=AWLSpider):
        self.spider = spider
        self.settings = get_project_settings()

    def upload_to_bucket(self):
        pass

    def run(self):
        os.remove(self.settings["FEED_URI"])
        now = datetime.datetime.now()
        process = CrawlerProcess(self.settings)
        process.crawl(self.spider)
        process.start()

        target_path = self.settings['FEED_URI'] \
            .replace('data/', '') \
            .replace('.json', '_{}.json'.format(datetime.datetime.strftime(now, '%Y-%m-%d_%H:%M')))

        self._to_gcp(self.settings['FEED_URI'], target_path)
        return self.settings["FEED_URI"]


class Transform(ETL):
    def __init__(self):
        self.df = None
        locale.setlocale(locale.LC_TIME, "pt_BR")

    def _read_input(self):
        print(self.DATA_PATH.joinpath('raw/awl.json'))
        self.df = pd.read_json(self.DATA_PATH.joinpath('raw/awl.json'))

    @staticmethod
    def concat_date(x, y):
        return '/'.join([str(x), str(datetime.datetime.strptime(y, '%B').month if len(x) <= 2 else str(y))])

    def _transform(self):
        # Added date
        # set locale to extract month number from its portuguese name
        pattern = 'Item\ adicionado\ (..?)\ de\ (\w*)\ de\ (....)'
        self.df['added_date'] = [reduce(self.concat_date, row)
                                 for row in self.df['added_date'].str.extract(pattern).values.tolist()]

        # Review stars
        pattern = '(.\..)\ .*'
        self.df['review_stars'] = self.df['review_stars'].str.extract(pattern)

        # Total price
        # neither sold by Amazon nor external sellers
        filter = (self.df['price'].isna()) & (self.df['sellers_price'].isna())
        self.df.loc[filter, 'total_price'] = -1

        # only sold by external sellers (might apply not tracked delivery tax)
        filter = (self.df['price'].isna()) & (~self.df['sellers_price'].isna())
        self.df.loc[filter, 'total_price'] = self.df['sellers_price']

        # sold by Amazon without free shipping
        filter = (~self.df['price'].isna()) & (~self.df['delivery_price'].isna())
        self.df.loc[filter, 'total_price'] = self.df['price'] + self.df['delivery_price']

        # sold by Amazon with free shipping
        filter = (~self.df['price'].isna()) & (self.df['delivery_price'].isna())
        self.df.loc[filter, 'total_price'] = self.df['price']

        # Availability
        # df['availability'].loc[~df['availability'].isnull()] = 1
        # df['availability'].fillna(0, inplace=True)
        self.df['availability'] = 1
        filter = (self.df['total_price'] == -1.0)
        self.df.loc[filter, 'availability'] = 0

        # Reference date
        self.df['reference_date'] = datetime.datetime.now()

    def run(self):
        self._read_input()
        self._transform()

        return self.df


class Load(ETL):
    def __init__(self, df):
        self.df = df
        self._target_path = 'out/awl.csv'

    def run(self):
        self.df.to_csv(self.DATA_PATH.joinpath(self._target_path), sep=';', index=False, mode='a', header='false')
        self._to_gcp(self.DATA_PATH.joinpath(self._target_path), self._target_path)


