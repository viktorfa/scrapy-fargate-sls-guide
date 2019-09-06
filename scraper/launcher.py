import sys
import json

from my_sls_scraper.crawl import crawl


def scrape(event={}, context={}):
    crawl(**event)


if __name__ == "__main__":
    try:
        event = json.loads(sys.argv[1])
    except IndexError:
        event = {}
    scrape(event)
