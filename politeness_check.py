import urllib.request
import urllib.robotparser


class CheckTimeout(urllib.robotparser.RobotFileParser):
    def __init__(self, url="", timeout=3):
        super().__init__(url)
        self.timeout = timeout

    def read(self):
        self.allow_everything = False
        self.disallow_everything = False

        try:
            file = urllib.request.urlopen(self.url, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                self.disallow_everything = True
            elif e.code >= 400:
                self.allow_everything = True
        else:
            data = file.read()
            self.parse(data.decode("utf-8").splitlines())


class Robots:
    def __init__(self, url):
        self.url = url
        self.obj = self.initializer()
        self.delay = 1.0
        self.get_delay()

    def initializer(self):
        obj = CheckTimeout()
        obj.set_url(self.url)
        obj.read()
        return obj

    def get_delay(self):
        delay = self.obj.crawl_delay(useragent="*")
        if delay is not None:
            self.delay = delay

    def fetch(self, new_url):
        return self.obj.can_fetch("*", new_url)
