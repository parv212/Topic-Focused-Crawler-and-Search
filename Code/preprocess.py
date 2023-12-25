import re
import math
import queue


class Frontier:
    def __init__(self):
        self.queue = queue.PriorityQueue()
        self.objs = {}
        self.wavenumbers = {}

    def initializer(self, seed_links):
        self.wavenumbers[0] = set()
        for link in seed_links:
            item = FrontierItem(link)
            item.calc_score()
            self.objs[link] = item
            self.wavenumbers[0].add(link)
            self.queue.put((0, item.score, link))

    def initialize_rest(self, rest_links: list):
        for q in rest_links:
            self.queue.put(q)

    def pop(self):
        return self.queue.get()

    def put(self, item, wave):
        link = item.link
        self.objs[link] = item
        if wave not in self.wavenumbers:
            self.wavenumbers[wave] = set()
        self.wavenumbers[wave].add(link)

    def update_inlinks(self, link, in_link):
        self.objs[link].update_in_links(in_link)

    def is_empty(self):
        return self.queue.empty()

    def change_wave(self, wave):
        if wave not in self.wavenumbers:
            return
        ex = []
        cutoff = 1.04
        for url in self.wavenumbers[wave]:
            item = self.objs[url]
            item.calc_score()
            if item.score > cutoff:
                continue
            ex.append((item.score, url))
            self.queue.put((wave, item.score, url))
        ex.sort()


class FrontierItem:
    def __init__(self, link: str, raw_link=None):
        self.link = link
        self.raw_link = raw_link
        self.keywords = [
            "Apple IPhone",
            "iPhone",
            "Apple IPad",
            "Mac",
            "Airpods",
            "AirMax",
            "Apple",
            "Apple Store",
            "Tim Cook",
            "Steve Jobs",
            "Macbook",
            "Macbook Air",
            "Macbook Pro",
            "Apple Watch",
            "MacOS",
            "watchOS",
            "Apple Music",
            "Apple TV+",
            "iCloud",
            "App Store",
            "iTunes",
            "Tim Cook",
            "Apple M1 chip",
            "WWDC",
        ]
        self.in_links = set()
        self.score = 0
        self.text = ""
        self.raw_html = ""

    def calc_score(self):
        keyword_count = 0
        for k in self.keywords:
            if self.raw_link is not None:
                if len(re.findall(k, self.raw_link, flags=re.IGNORECASE)) != 0:
                    keyword_count += 1
            else:
                if len(re.findall(k, self.link, flags=re.IGNORECASE)) != 0:
                    keyword_count += 1
        keyword_score = math.exp(-keyword_count)
        in_urls_score = math.exp(-len(self.in_links))
        self.score = keyword_score + in_urls_score

    def update_in_links(self, link: str):
        self.in_links.add(link)


class Normalizer:
    def get_domain(self, url: str):
        dom = re.findall("//[^/]*\w", url, flags=re.IGNORECASE)[0]
        dom = dom[2:]
        return dom

    def canonicalize(self, base_url: str, domain: str, url: str):
        # Check for \\ in the url
        if "\\" in url:
            mod = re.sub("\\\\+", "/", url.encode("unicode_escape").decode())
        else:
            mod = url

        # Remove tab and next line characters from the string
        mod = re.sub("[\n\t ]*", "", mod)
        try:
            # Remove anchors from the modified url
            mod = re.sub("#.*", "", mod)

            # Check if the url has characters or digits or not
            if not re.findall("\w", mod):
                return ""

            if re.match("^[\w~]+[^:]*$", mod):
                mod = re.sub("/\w*[^/]*\w*$", "/" + mod, base_url)
            elif re.match("^\w+[^/]+\w$", mod):
                mod = re.sub("/\w*[^/]*\w*$", "/" + mod, base_url)
            elif re.match("^\./\w+[^:]*[\w/]$", mod):
                mod = re.sub("/\w*[^/]*\w*$", mod[1:], base_url)
            elif re.match("^\?[^/]*", mod):
                mod = base_url + mod

            # Resolving relative path
            if re.match("^(?:\.{2}/)+\w+.*", mod):
                value = re.findall("\.{2}/\w+.*", mod)[0][2:]
                length = len(re.findall("\.{2}", mod))
                names = re.findall("/\w+(?:\.\w+)*", base_url)
                target_value = "".join(names[-length - 1 :])
                mod = re.sub(target_value, value, base_url)

            # Black list elements that if exists in the url than skip it
            black_list = [
                ".svg",
                ".png",
                ".pdf",
                ".gif",
                "mailto",
                ".webm",
                "tel:",
                "javascript",
                "www.vatican.va",
                ".ogv",
                "amazon",
                ".jpg",
                "youtube",
                "edit",
                "footer",
                "sidebar",
                "cite",
                "special",
            ]

            for val in black_list:
                if val in mod.lower():
                    return ""

            # Remove default ports for both https and http calls
            if re.match("https", mod, flags=re.IGNORECASE) is not None:
                mod = re.sub(":443", "", mod)
            elif re.match("http", mod, flags=re.IGNORECASE) is not None:
                mod = re.sub(":80", "", mod)

            # Substitute https with http
            mod = re.sub("http", "http", mod, flags=re.IGNORECASE)
            mod = re.sub("https", "http", mod, flags=re.IGNORECASE)

            if re.match("^/.+", mod) is not None:
                mod = "http://" + domain + mod
            elif re.match("^//.+", mod) is not None:
                mod = "http:" + mod

            # Remove duplicate slashes
            dup_slash = re.findall("\w//+.", mod)
            if len(dup_slash) != 0:
                for i in dup_slash:
                    temp = i[0] + "/" + i[-1]
                    mod = re.sub(i, temp, mod)

            # Find domain in url and convert it to lower case
            s_domain = re.findall("//[^/]*\w", mod)
            s_domain = s_domain[0]
            lc_domain = s_domain.lower()
            mod = re.sub(s_domain, lc_domain, mod)

            if re.match(".*com$", mod) is not None:
                mod += "/"

            percent_code = re.findall("%\w{2}", mod)
            for p in percent_code:
                mod = re.sub(p, p.upper(), mod)
            return mod

        except Exception as e:
            print("Error in filtering: ", e)
            return ""
