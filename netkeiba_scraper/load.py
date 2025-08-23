import random
import requests
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from netkeiba_scraper.helper import load_config
from netkeiba_scraper.parse import parse_html, parse_json


class BaseLoader:
    def __init__(self, data_type):
        options = Options()
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)
        
        self.config = load_config(data_type)
        self.validator = self.config['property'].get('validator', '')
        
        loaders = {
            'entry': self.load_entry,
            'result': self.load_result,
            'horse': self.load_horse
        }
        self.load = loaders.get(data_type)
        if not self.load:
            raise ValueError(f"Unexpected data type: {data_type}")

    def create_url(self, base_url, entity_id):
        return base_url.replace('{ID}', entity_id)

    def load_contents(self, url):
        time.sleep(random.uniform(2, 3))
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.validator))
            )
            return self.driver.page_source
        except Exception as e:
            raise RuntimeError(f"Failed to load contents from {url}: {e}") from e

    def parse_with_error_handling(self, parse_funcs, entity_id):
        results = []
        for parse_func, args in parse_funcs:
            try:
                result = parse_func(*args)
                results.append(result)
            except RuntimeError as e:
                raise RuntimeError(f"Failed to parse data for {entity_id}: {e}") from e
        return results

    def load_entry(self, entity_id):
        url = self.create_url(self.config['property']['url'], entity_id)
        content = self.load_contents(url)

        parse_funcs = [
            (parse_html, ('race', content, entity_id)),
            (parse_html, ('entry', content, entity_id))
        ]
        race, entry = self.parse_with_error_handling(parse_funcs, entity_id)

        return race, entry

    def load_result(self, entity_id):
        url = self.create_url(self.config['property']['url'], entity_id)
        content = self.load_contents(url)

        parse_funcs = [
            (parse_html, ('race_db', content, entity_id)),
            (parse_html, ('result', content, entity_id))
        ]
        race, entry = self.parse_with_error_handling(parse_funcs, entity_id)

        return race, entry

    def load_horse(self, entity_id):
        url = self.create_url(self.config['property']['url'], entity_id)
        content = self.load_contents(url)

        parse_funcs = [
            (parse_html, ('horse', content, entity_id)),
            (parse_html, ('history', content, entity_id))
        ]
        horse, history = self.parse_with_error_handling(parse_funcs, entity_id)

        return horse, history

    def close(self):
        self.driver.quit()

        
class RaceIDLoader:
    def __init__(self, workers=1, max_pages=20):
        options = Options()
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)
        self.workers = workers
        self.max_pages = max_pages
        self.base_url = "https://db.netkeiba.com/"

    def create_url(self, year, month, page):
        return (
            f"{self.base_url}?pid=race_list&word=&start_year={year}&start_mon={month}"
            f"&end_year={year}&end_mon={month}&jyo%5B0%5D=01&jyo%5B1%5D=02&jyo%5B2%5D=03"
            f"&jyo%5B3%5D=04&jyo%5B4%5D=05&jyo%5B5%5D=06&jyo%5B6%5D=07&jyo%5B7%5D=08"
            f"&jyo%5B8%5D=09&jyo%5B9%5D=10&kyori_min=&kyori_max=&sort=date&list=100&page={page}"
        )

    def load_contents(self, url):
        """Load the page using Selenium (no validator needed)."""
        time.sleep(random.uniform(1, 2))
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href^='/race/']"))
            )
            return self.driver.page_source
        except Exception as e:
            raise RuntimeError(f"Failed to load page {url}: {e}") from e

    def parse_race_ids(self, html):
        """Extract race IDs from page HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all("a", href=re.compile(r"^/race/\d{12}/"))
        return [a['href'].split('/')[2] for a in links]

    def fetch_race_ids_page(self, args):
        year, month, page = args
        url = self.create_url(year, month, page)
        try:
            html = self.load_contents(url)
            return self.parse_race_ids(html)
        except Exception as e:
            print(f"[ERROR] Page {page}: {e}")
            return []

    def get_race_ids_in_month(self, year, month):
        print(f"Fetching race IDs for {year}/{month} ...")
        all_race_ids = set()
        for page in range(1, self.max_pages + 1):
            page_ids = self.fetch_race_ids_page((year, month, page))
            all_race_ids.update(page_ids)
            if len(page_ids) < 100:
                break  # last page
        print(f"Found {len(all_race_ids)} race IDs.\n")
        return sorted(all_race_ids)

    def get_race_ids_in_period(self, start_year, start_month, end_year, end_month):
        all_race_ids = set()
        current = datetime(start_year, start_month, 1)
        end = datetime(end_year, end_month, 1)

        while current <= end:
            year, month = current.year, current.month
            month_ids = self.get_race_ids_in_month(year, month)
            all_race_ids.update(month_ids)

            # Move to next month
            if month == 12:
                current = datetime(year + 1, 1, 1)
            else:
                current = datetime(year, month + 1, 1)

        return sorted(all_race_ids)

    def close(self):
        self.driver.quit()