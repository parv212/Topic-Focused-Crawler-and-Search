import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import write_files
from preprocess import Normalizer, Frontier, FrontierItem
from politeness_check import Robots


class Crawler:
    def __init__(self):
        self.seed_urls = None
        self.frontier = Frontier()
        self.normalizer = Normalizer()
        self.all_urls = None
        self.crawled_urls = set()
        self.count = 0
        self.all_out_links = {}
        self.redirected_map = {}
        self.robots = {}
        self.robots_delay = {}
        self.robots_timer = {}
        self.time_out = 3
        self.total_count = 4000

    def initializer(self, seed_urls):
        self.all_urls = set(seed_urls)
        self.seed_urls = seed_urls
        self.frontier.initializer(seed_urls)

    def crawl(self):
        current_wave = 0
        while True:
            if self.frontier.is_empty():
                self.frontier.change_wave(current_wave + 1)

            if self.frontier.is_empty():
                for url in self.crawled_urls:
                    write_files.write_in_links(
                        {url: list(self.frontier.objs[url].in_links)}
                    )
                return "Completed"

            current_wave, score, url = self.frontier.pop()
            hostpage = self.normalizer.get_domain(url)

            if hostpage not in self.robots:
                try:
                    robots = Robots("http://" + hostpage + "/robots.txt")
                    self.robots[hostpage] = robots
                    if robots.delay > self.time_out:
                        self.robots_delay[hostpage] = self.time_out
                    else:
                        self.robots_delay[hostpage] = robots.delay
                    self.robots_timer[hostpage] = datetime.now()
                except Exception as e:
                    print("Error: ", e)
                    continue
            delay = self.robots_delay[hostpage]
            if not self.robots[hostpage].fetch(url):
                print("Not Allowed: ", url)
                continue
            else:
                since_last_crawl = datetime.now() - self.robots_timer[hostpage]
                if since_last_crawl.total_seconds() < delay:
                    time.sleep(delay - since_last_crawl.total_seconds())

                print("Current: ", url)
                try:
                    url_head = self.get_header(url)
                    if url_head.status_code == 404:
                        continue
                except Exception as e:
                    self.robots_timer[hostpage] = datetime.now()
                    print("No header present ", e)
                    continue

                header_dic = dict(url_head.headers)

                if "content-type" in url_head.headers:
                    content_type = url_head.headers["content-type"]
                else:
                    content_type = "text/html"

                if "text/html" not in content_type:
                    continue
                else:
                    try:
                        soup, raw_html, base_url, lang = self.get_page(url)
                        self.robots_timer[hostpage] = datetime.now()
                        if not self.crawl_page(base_url, lang):
                            continue
                        if base_url in self.crawled_urls:
                            self.frontier.objs[base_url].in_links.update(
                                self.frontier.objs[url].in_links
                            )
                            continue
                        else:
                            self.crawled_urls.add(base_url)
                            frontier_item = FrontierItem(base_url)
                            frontier_item.in_links = self.frontier.objs[url].in_links
                            self.frontier.objs[base_url] = frontier_item
                            self.redirected_map[url] = base_url
                    except Exception as e:
                        print("Error while getting page: ", e)
                        self.robots_timer[hostpage] = datetime.now()
                        continue

                    raw_out_links = self.get_out_links(soup)
                    out_links = []

                    text = self.get_text(soup)
                    if len(soup.select("title")) != 0:
                        title = soup.select("title")[0].get_text()
                    else:
                        title = None

                    write_files.write_contents(base_url, text, title)
                    write_files.write_raw_html({base_url: raw_html})

                    for link in raw_out_links:
                        processed_link = self.normalizer.canonicalize(
                            base_url, hostpage, link
                        )
                        if len(processed_link) != 0:
                            out_links.append(processed_link)
                            if processed_link not in self.all_urls:
                                frontier_item = FrontierItem(processed_link, link)
                                frontier_item.update_in_links(base_url)
                                self.frontier.put(frontier_item, current_wave + 1)
                                self.all_urls.add(processed_link)
                            else:
                                if processed_link in self.redirected_map:
                                    redirected = self.redirected_map[processed_link]
                                    self.frontier.update_inlinks(redirected, base_url)
                                else:
                                    self.frontier.update_inlinks(
                                        processed_link, base_url
                                    )
                    write_files.write_out_links({base_url: out_links})
                self.count += 1
                print(self.count, current_wave, url, score)
                if self.count == self.total_count:
                    for url in self.crawled_urls:
                        write_files.write_in_links(
                            {url: list(self.frontier.objs[url].in_links)}
                        )
                    print("Completed")
                    return

    def get_out_links(self, soup):
        a = soup.select("a")
        out_links = []
        for item in a:
            if item.get("href"):
                out_links.append(item["href"])
        return out_links

    def get_page(self, url: str):
        headers = {"Connection": "close"}
        mod = requests.get(url=url, headers=headers, timeout=self.time_out)
        soup = BeautifulSoup(mod.text, "lxml")
        try:
            if soup.select("html")[0].has_attr("lang"):
                lang = soup.select("html")[0]["lang"]
            else:
                lang = "en"
        except Exception:
            lang = "en"
        base_url = mod.url
        return soup, mod.text, base_url, lang

    def get_header(self, url: str):
        headers = {"Connection": "close"}
        head = requests.head(
            url=url, headers=headers, timeout=self.time_out, allow_redirects=True
        )
        return head

    def get_text(self, soup: BeautifulSoup):
        output = ""
        text = soup.find_all("p")
        for t in text:
            new_t = t.get_text()
            new_t = re.sub("\n", "", new_t)
            new_t = re.sub("  +", " ", new_t)
            if len(new_t) == 0:
                continue
            output += "{} ".format(new_t)
        return output

    def crawl_page(self, base_url, lang):
        result = True
        if "en" not in lang.lower():
            result = False
        black_list = [
            ".jpg",
            ".svg",
            ".png",
            ".pdf",
            ".gif",
            "youtube",
            "edit",
            "footer",
            "sidebar",
            "cite",
            "special",
            "mailto",
            ".webm",
            "tel:",
            "javascript",
            ".ogv",
            "amazon",
        ]
        block = 0
        for key in black_list:
            if key in base_url.lower():
                block = 1
                break
        if block == 1:
            result = False

        return result


seed_urls = [
    "https://en.wikipedia.org/wiki/IPhone",
    "http://en.wikipedia.org/wiki/History_of_Apple_Inc.",
    "https://en.wikipedia.org/wiki/MacOS_Sonoma",
    "https://en.wikipedia.org/wiki/Mac_operating_systems",
    "http://en.wikipedia.org/wiki/IOS",
    "https://en.wikipedia.org/wiki/MacBook",
]

crawler = Crawler()
crawler.initializer(seed_urls)
crawler.crawl()
