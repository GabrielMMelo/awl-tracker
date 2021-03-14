import datetime
from functools import reduce
from io import StringIO
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

    def _from_gcp(self, source_path):
        return self.storage.download_blob_as_string(
            self._bucket_name,
            source_path
        )

    def _to_gcp(self, content, destination_path):
        self.storage.upload_from_string(
            self._bucket_name,
            content,
            destination_path
        )


class Extract(ETL):
    def __init__(self, spider=AWLSpider, historical=False):
        self.historical = historical
        self.spider = spider
        self.settings = get_project_settings()

    def run(self):
        now = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M')
        if self.historical:
            self.settings["FEED_URI"] = self.settings["FEED_URI"].replace('.json', '%s%s%s' % ('_', now, '.json'))
        process = CrawlerProcess(self.settings)
        process.crawl(self.spider)
        process.start()

        return self.settings["FEED_URI"]


class Transform(ETL):
    def __init__(self):
        self.df = None
        self.df_master = None
        self._raw_path = 'raw/awl.json'
        self._interim_path = 'interim/awl.csv'
        self._master_path = 'master/awl.csv'
        locale.setlocale(locale.LC_TIME, "pt_BR")

    def _read_inputs(self):
        self.df = pd.read_json(StringIO(self._from_gcp(self._raw_path).decode("utf-8")))
        self.df_master = pd.read_csv(StringIO(self._from_gcp(self._master_path).decode("utf-8")), sep=";")

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

        # Join to df_gcp
        self.df = pd.concat([self.df, self.df_master])
        self.df.drop_duplicates(subset=['reference_date', 'name'], inplace=True)

    def run(self):
        self._read_inputs()
        self._transform()
        self._to_gcp(self.df.to_csv(), self._interim_path)

        return self.df


class Load(ETL):
    def __init__(self):
        self.df = None
        self._interim_path = 'interim/awl.csv'
        self._master_path = 'master/awl.csv'

    def run(self):
        # fix comma separated file (instead of ;) and drop index column before load it
        self.df = pd.read_csv(StringIO(self._from_gcp(self._interim_path).decode("utf-8")), sep=",") \
            .drop(columns=['Unnamed: 0'])
        self._to_gcp(self.df.to_csv(sep=";"), self._master_path)
