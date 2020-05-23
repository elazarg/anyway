import logging

from anyway.parsers.location_extraction import manual_filter_location_of_text
from anyway.parsers.news_flash_classifiers import classify_news_flash, classify_tweets
from anyway.parsers.news_flash_parser import get_all_news_flash_data_for_updates, update_news_flash_bulk
from anyway.parsers.news_flash.scrape_rss_html import extract_geo_features


# TODO: add to site_configs
news_flash_classifiers = {'ynet': classify_news_flash,
                          'twitter': classify_tweets,
                          'walla': classify_news_flash}


def update_news_flash(maps_key, news_flash_data, bulk_size=100):
    news_flash_id_list = []
    params_dict_list = []
    for news_flash_id, title, description, item_source, old_location in news_flash_data:
        item_data = description
        if item_data is None:
            item_data = title
        news_item = {}
        try:
            accident = news_flash_classifiers[item_source](item_data)
            news_item['accident'] = accident
            if accident:
                location = manual_filter_location_of_text(item_data)
                if location != old_location:
                    extract_geo_features(news_item, maps_key)
            else:
                news_item['lat'] = None
                news_item['lon'] = None
                for col in ['region_hebrew', 'district_hebrew', 'yishuv_name', 'street1_hebrew', 'street2_hebrew',
                            'non_urban_intersection_hebrew', 'road1', 'road2', 'road_segment_name', 'resolution']:
                    news_item[col] = None
            news_flash_id_list.append(news_flash_id)
            params_dict_list.append(news_item)
            if len(news_flash_id_list) >= bulk_size:
                update_news_flash_bulk(news_flash_id_list, params_dict_list)
                news_flash_id_list = []
                params_dict_list = []
            logging.info('new flash news updated, is accident: ' + str(news_item['accident']))
        except Exception as e:
            logging.info('new flash news failed to update, index: ' + str(news_flash_id))
            logging.info(e)
    if len(news_flash_id_list) > 0:
        update_news_flash_bulk(news_flash_id_list, params_dict_list)


def main(maps_key, source=None, news_flash_id=None):
    if news_flash_id is not None:
        news_flash_data = get_all_news_flash_data_for_updates(id=news_flash_id)
    elif source is not None:
        news_flash_data = get_all_news_flash_data_for_updates(source=source)
    else:
        news_flash_data = get_all_news_flash_data_for_updates()
    if len(news_flash_data) > 0:
        update_news_flash(maps_key, news_flash_data)
    else:
        logging.info('no matching news flash found, source={0}, id={1}'.format(source, news_flash_id))
