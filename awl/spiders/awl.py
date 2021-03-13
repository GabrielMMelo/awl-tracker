import json
import scrapy


class AWLSpider(scrapy.Spider):
    name = 'awl'
    base_domain = 'https://amazon.com.br'
    allowed_domains = ['amazon.com.br']
    total_items = []

    def start_requests(self):
        urls = ['https://www.amazon.com.br/hz/wishlist/genericItemsPage/3202G6PAUQGQ5?']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        #print(response.body)
        g_items = response.css('#g-items')
        items = g_items.css('li')
        for item in items:
            item_dict = {}
            item_dict["name"] = item.re('itemName.*title=\"(.*)"\ href')[0]
            item_dict["url"] = ''.join([self.base_domain, item.css('a[id*=itemName]::attr(href)').get()])
            item_dict["review_count"] = int(item.re('review_count.*aria-label=\"(.*)\"\ class')[0].replace('.', ''))
            item_dict["review_stars"] = item.re('review_stars.*<span.*>(.*)</span>')[0]
            item_dict["added_date"] = item.css('span[id*="itemAddedDate"]::text').get().strip()

            _availability = item.css('span[id*="availability-msg"]::text').get()
            if _availability:
                item_dict["availability"] = _availability.strip()

            _price = item.css('span[id*="itemPrice"] span::text').get()
            if _price is not None:
                item_dict["price"] = float(_price.replace("R$", "").replace(".", "").replace(",", "."))

            _delivery_price = item.css('span[id*="itemPrice"] ~span span::text').re('R\$(.*)')
            if _delivery_price:
                item_dict["delivery_price"] = float(_delivery_price[0].replace(',', '.'))

            _best_seller_price = item.css('span[class*=itemUsedAndNewPrice]::text').get()
            if _best_seller_price is not None:
                item_dict["sellers_price"] = float(_best_seller_price.strip().replace("R$", "").replace(".", "").replace(",", '.'))

            yield item_dict

        # check for new pages
        next_results_selector = response.css('script[data-a-state*="scrollState"][type="a-state"]::text').get()
        if next_results_selector is not None:
            next_results_json = json.loads(next_results_selector)
            next_results_url = ''.join([self.base_domain, next_results_json['showMoreUrl']])
            yield response.follow(next_results_url, callback=self.parse)