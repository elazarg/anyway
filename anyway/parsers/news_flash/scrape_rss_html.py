from datetime import datetime
import logging

import requests
from bs4 import BeautifulSoup

from anyway.parsers.news_flash_parser import get_latest_date_from_db
from anyway.parsers.news_flash_parser import insert_new_flash_news

from anyway.parsers.news_flash_classifiers import classify_news_flash
from ..location_extraction import manual_filter_location_of_text, geocode_extract, get_db_matching_location, \
    set_accident_resolution


def parse_raw_item_walla(rss_soup: BeautifulSoup, html_soup: BeautifulSoup):
    # get title from html_soup and not from rss_soup because lxml strips CDATA
    title = html_soup.find('h1', class_="title").get_text()
    author = html_soup.find('div', class_="author").get_text()
    description = rss_soup.description.get_text()
    return title, author, description


def parse_raw_item_ynet(rss_soup: BeautifulSoup, html_soup: BeautifulSoup):
    title = rss_soup.title.get_text()
    description_text = html_soup.find('script', type="application/ld+json").get_text()
    author = description_text.split('(')[-1].split(')')[0]
    description = description_text.split('\"description\"')[1].split('(')[0]
    return title, author, description


site_config = {
    'walla': {
        'rss_link': 'https://rss.walla.co.il/feed/22',
        'time_format': '%a, %d %b %Y %H:%M:%S %Z',
        'parser': parse_raw_item_walla
    },
    'ynet': {
        'rss_link': 'https://www.ynet.co.il/Integration/StoryRss1854.xml',
        'time_format': '%a, %d %b %Y %H:%M:%S %z',
        'parser': parse_raw_item_ynet
    }
}


def fetch_url(link):
    return requests.get(link).text


def fetch_and_parse_recent_news_items(site_name, latest_date=None, *, fetch_rss=fetch_url, fetch_html=fetch_url):
    config = site_config[site_name]
    parse = config['parser']
    body = fetch_rss(config['rss_link'])
    for rss_item_soup in BeautifulSoup(body, 'lxml').find_all('item'):
        raw_date = rss_item_soup.pubdate.get_text()
        date_time = datetime.strptime(raw_date, config['time_format']).replace(tzinfo=None)
        if latest_date is not None and date_time <= latest_date:
            # found all the recent news flashes
            break

        link = rss_item_soup.guid.get_text()
        linked_html = fetch_html(link)
        html_soup = BeautifulSoup(linked_html, 'lxml')
        title, author, description = parse(rss_item_soup, html_soup)
        yield {
            'date_parsed': date_time,
            'title': title,
            'link': link,
            'source': site_name,
            'description': description,
            'author': author,
        }


def init_news_item(entry_parsed):
    news_item = {'date_parsed': entry_parsed['date_parsed'],
                 'title': entry_parsed['title'],
                 'link': entry_parsed['link'],
                 'source': entry_parsed['source'],
                 'description': entry_parsed['description'],
                 'author': entry_parsed['author'],
                 'accident': False,
                 'location': None,
                 'lat': None,
                 'lon': None,
                 'road1': None,
                 'road2': None,
                 'road_segment_name': None,
                 'yishuv_name': None,
                 'street1_hebrew': None,
                 'street2_hebrew': None,
                 'resolution': None,
                 'region_hebrew': None,
                 'district_hebrew': None,
                 'non_urban_intersection_hebrew': None}
    return news_item


def extract_geo_features(news_item, maps_key):
    location = None
    if news_item['description'] is not None:
        location = manual_filter_location_of_text(news_item['description'])
    if location is None:
        location = manual_filter_location_of_text(news_item['title'])
    news_item['location'] = location
    geo_location = geocode_extract(location, maps_key)
    if geo_location is not None:
        news_item['lat'] = geo_location['geom']['lat']
        news_item['lon'] = geo_location['geom']['lng']
        news_item['resolution'] = set_accident_resolution(geo_location)
        db_location = get_db_matching_location(news_item['lat'], news_item['lon'], news_item['resolution'],
                                               geo_location['road_no'])
        for col in ['region_hebrew', 'district_hebrew', 'yishuv_name', 'street1_hebrew', 'street2_hebrew',
                    'non_urban_intersection_hebrew', 'road1', 'road2', 'road_segment_name']:
            news_item[col] = db_location[col]


def scrape_news_flash(site_name, maps_key):
    latest_date = get_latest_date_from_db(site_name)

    for entry_parsed in fetch_and_parse_recent_news_items(site_name, latest_date):
        news_item = init_news_item(entry_parsed)

        news_item['accident'] = classify_news_flash(news_item['title'])

        if news_item['accident']:
            extract_geo_features(news_item, maps_key)

        insert_new_flash_news(**news_item)
        logging.info('new flash news added, is accident: ' + str(news_item['accident']))
