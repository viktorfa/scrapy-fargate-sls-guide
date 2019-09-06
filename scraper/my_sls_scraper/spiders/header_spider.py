from urllib.parse import urlparse
from w3lib.html import remove_tags

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class HeaderSpider(CrawlSpider):
    name = "header_spider"

    start_urls = ["https://scrapy.org"]
    allowed_domains = ["scrapy.org"]

    def __init__(self, **kwargs):
        # Enables overriding attributes for a flexible spider
        if kwargs.get("start_urls"):
            self.start_urls = kwargs.get("start_urls")
            self.allowed_domains = list(
                urlparse(x).hostname for x in self.start_urls
            )

        super().__init__(
            **{k: v for k, v in kwargs.items() if not hasattr(self, k)}
        )

    rules = [  # Get all links on start url
        Rule(
            link_extractor=LinkExtractor(
                deny=r"\?",
            ),
            follow=False,
            callback="parse_page",
        )
    ]

    def parse_start_url(self, response):
        return self.parse_page(response)

    def parse_page(self, response):
        header = response.css("h1, h2").extract_first(
        ) or response.css("title").extract_first() or response.url
        return {
            "header": remove_tags(header),
            "url": response.url,
        }
