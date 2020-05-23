import os
import sys
from ..mda_twitter.mda_twitter import mda_twitter
from .scrape_rss_html import scrape_news_flash


def main(google_maps_key):
    """
    main function for beginning of the news flash process
    :param google_maps_key_path: path to google maps key
    """
    sys.path.append(os.path.dirname(os.path.realpath(__file__)))
    scrape_news_flash('ynet', google_maps_key)
    scrape_news_flash('walla', google_maps_key)
    mda_twitter()
