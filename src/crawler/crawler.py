import time

import requests
from bs4 import BeautifulSoup as soup
from selenium import webdriver

from selenium.webdriver.chrome.options import Options

from utils.logger_utils import get_logger

logger = get_logger('Base Crawler')


class Crawler:
    @staticmethod
    def time_throttling(start_time, end_time, time_throttle):
        if time_throttle > (end_time - start_time):
            time.sleep(time_throttle - end_time + start_time)

    def __init__(self, soup_calls_limit=5, sleep_time=1, max_retry_times=3):
        # Number of consecutive calls
        self.get_url_soup_calls = 1
        # Number of consecutive calls before sleep
        self.soup_calls_limit = soup_calls_limit
        # Sleep time
        self.sleep_time = sleep_time
        # Max number of retry times
        self.max_retry_times = max_retry_times

    def _request(self, url, func, headers=None, *args, **kwargs):
        retry_time = 0
        data = None
        while retry_time < self.max_retry_times:
            try:
                response = requests.get(url, headers=headers)
                status = response.status_code
                if 200 <= status < 300:
                    resp = response.json()
                    data = func(resp, *args, **kwargs)
                    break
                else:
                    logger.warning(f'Fail ({status}) to request url {url}')
            except Exception as ex:
                logger.exception(ex)
            retry_time += 1
        return data

    def _get_url_soup(self, url):
        # Read the html of the page
        if self.get_url_soup_calls <= self.soup_calls_limit:
            self.get_url_soup_calls += 1
        else:
            time.sleep(self.sleep_time)
            # Reset get_url_soup_calls
            self.get_url_soup_calls = 1

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
        }
        response = requests.get(url, headers=headers)
        status = response.status_code
        content = response.text
        page_soup = soup(content, "html.parser")
        return page_soup, status

    def fetch_data(self, url, func, *args, **kwargs):
        retry_time = 0
        data = None
        while retry_time < self.max_retry_times:
            try:
                page_soup, status = self._get_url_soup(url)
                if 200 <= status < 300:
                    data = func(page_soup, *args, **kwargs)
                    break
                else:
                    logger.warning(f'Fail ({status}) to request url {url}')
            except Exception as ex:
                logger.exception(ex)
            retry_time += 1
        return data

    @staticmethod
    def use_chrome_driver(driver, url, handler_func, **kwargs):
        data = None
        try:
            driver.get(url)
            data = handler_func(driver, **kwargs)
        except Exception as ex:
            logger.exception(ex)
        finally:
            # driver.quit()
            ...
        return data

    @classmethod
    def get_driver(cls):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        # run Selenium in headless mode
        chrome_options.add_argument('--no-sandbox')
        # overcome limited resource problems
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("lang=en")
        # open Browser in maximized mode
        chrome_options.add_argument("start-maximized")
        # disable infobars
        chrome_options.add_argument("disable-infobars")
        # disable extension
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Mobile Safari/537.36')
        driver = webdriver.Chrome(options=chrome_options)
        return driver

    @staticmethod
    def crawl_img(url):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
            }
            response = requests.get(url, headers=headers)
            return response.content
        except Exception as e:
            logger.warning(e)
            return ""
