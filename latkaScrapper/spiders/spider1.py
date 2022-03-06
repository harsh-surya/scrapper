import math

import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess


class WebCrawler(scrapy.Spider):
    name = 'spider'
    start_urls = ['https://getlatka.com/']

    def parse(self, response):

        #extracting all table rows from the webpage
        for row in response.css('table.data-table_table__2P6Tl tr')[1:]:
            columns = row.css('td')
            name = columns[1].css('a.cells_link__2252j::text').extract()
            revenue = columns[2].css('::text').extract()
            funding = columns[3].css('::text').extract()
            valuation = columns[4].css('::text').extract()
            cashflow = columns[5].css('span::text').extract()
            founder = columns[6].css('a.cells_name__KCAFe::text').extract()
            teamsize = columns[7].css('::text').extract()
            age = columns[8].css('::text').extract()
            location = columns[9].css('::text').extract()
            industry = columns[10].css('a.home_ellipses__2KmVe::text').extract()
            asof = columns[11].css('::text').extract()

            yield{
                'Name': ''.join(name).strip(),
                'Revenue': ''.join(revenue).strip(),
                'Funding': ''.join(funding).strip(),
                'Valuation': ''.join(valuation).strip(),
                'CashFlow': ''.join(cashflow).strip(),
                'Founder': ''.join(founder).strip(),
                'TeamSize': ''.join(teamsize).strip(),
                'Age': ''.join(age).strip(),
                'Location': ''.join(location).strip(),
                'Industry': ''.join(industry).strip(),
                'AsOf': ''.join(asof).strip(),
            }

        #we get the url to the next page using the next button at the bottom of the page
        next_urls = response.xpath('//a[@class="pagination_button__1f2SL pagination_special_button__3cnmT"]/@href').extract()
        texts = response.xpath('//a[@class="pagination_button__1f2SL pagination_special_button__3cnmT"]/text()').extract()
        next_page = response.urljoin(next_urls[-1])
        next_text = texts[-1].strip()

        if next_text == 'Next':
            yield Request(next_page, callback=self.parse)


process = CrawlerProcess({
    'FEED_FORMAT': 'json',
    'FEED_URI': 'output.json',
})

process.crawl(WebCrawler)
process.start()
# python multiprocessing library is not supported with scrapy. hence parallel processing can not be done.
# beautifulSoup could have been used for parallel processing