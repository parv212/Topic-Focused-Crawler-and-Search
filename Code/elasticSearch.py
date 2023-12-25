from urllib.parse import urlparse, urlunparse
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
import re


class Parser:
    def __init__(self):
        self.path = ""
        self.doc = "documents.txt"
        self.in_links = "in_links.json"
        self.out_links = "out_links.json"
        self.count = 4000

    def parse_docs(self):
        docs = {}
        doc = list()
        add_file_flag = 0
        txt_flag = 0
        with open(self.path + self.doc, "r", encoding="utf-8") as f:
            count = 0
            for line in f:
                line = line.strip()
                if re.search("</DOC>", line):
                    add_file_flag = 0
                    docs[data_id] = " ".join(doc)
                    doc = list()
                    count += 1
                    if count == self.count:
                        return docs
                if add_file_flag == 1:
                    if re.search("</DOCNO>", line):
                        data_id = re.sub("(<DOCNO>)|(</DOCNO>)", "", line)
                    if re.search("</TEXT>", line):
                        txt_flag = 0
                    if txt_flag == 1:
                        doc.append(line)
                    if re.search("<TEXT>", line):
                        if re.search("[A-Z|a-z]*[a-z]", line):
                            doc.append(line[6:])
                        txt_flag = 1
                if re.search("<DOC>", line):
                    add_file_flag = 1
        return docs

    def parse_links(self):
        in_links = {}
        out_links = {}
        count = 0
        with open(self.path + self.in_links, "r", encoding="utf-8") as fi:
            for line in fi:
                in_links.update(json.loads(line))
                count += 1
        count = 0
        with open(self.path + self.out_links, "r", encoding="utf-8") as fo:
            for line in fo:
                out_links.update(json.loads(line))
                count += 1
        return in_links, out_links


def query_preprocess(url):
    parsed_url = urlparse(url)
    parsed_url = parsed_url._replace(
        scheme=parsed_url.scheme.lower(), netloc=parsed_url.netloc.lower()
    )
    if parsed_url.port:
        parsed_url = parsed_url._replace(netloc=parsed_url.hostname)
    parsed_url = parsed_url._replace(fragment="")
    path = parsed_url.path.replace("//", "/")
    if path:
        clean_path = path if path[0] == "/" else "/" + path
        clean_parsed = parsed_url._replace(path=clean_path)
        segments = clean_parsed.path.split("/")
        segments = [s for s in segments if s != "."]
        i = 0
        while i < len(segments):
            if segments[i] == "..":
                if i > 0:
                    del segments[i - 1 : i + 1]
                    i -= 1
            else:
                i += 1
        path = "/".join(segments)
        new_parts = (
            clean_parsed.scheme,
            clean_parsed.netloc,
            path,
            clean_parsed.params,
            clean_parsed.query,
            clean_parsed.fragment,
        )
        return urlunparse(new_parts)
    else:
        return urlunparse(parsed_url)


class Elastic:
    def __init__(self):
        self.cloud_id = "8660e1288ca3484ea440e8357734116f:dXMtZWFzdC0yLmF3cy5lbGFzdGljLWNsb3VkLmNvbTo0NDMkNDk1ODRlZjk0YmJjNDMyNDhjNDYyY2RiYTlkNmFiMzYkNDYzOGJjNWY1ZWE1NGVkZjhkMjM5NzM2ZGI1MjA2ZTg="
        self.index = "search-apple-tech"
        self.es = Elasticsearch(
            request_timeout=20000,
            cloud_id=self.cloud_id,
            http_auth=("elastic", "nBP49VgJJfZD9XWgzkxU16y0"),
        )
        print(self.es.ping())
        self.parser = Parser()

    def indexer(self):
        docs = self.parser.parse_docs()
        in_links, out_links = self.parser.parse_links()

        for id in docs:
            search_query = {"query": {"term": {"_id": query_preprocess(id)}}}
            response = self.es.search(index=self.index, body=search_query)

            author = "Parv Thakkar"

            if response["hits"]["total"]["value"] > 0:
                existing_doc = response["hits"]["hits"][0]["_source"]
                if existing_doc["author"] != author:
                    existing_doc["author"] += f", {author}"
            else:
                existing_doc = {"author": author, "inlinks": [], "outlinks": []}

            for inlink in in_links[id]:
                if inlink not in existing_doc["inlinks"]:
                    existing_doc["inlinks"].append(inlink)

            for outlink in out_links[id]:
                if outlink not in existing_doc["outlinks"]:
                    existing_doc["outlinks"].append(outlink)

            actions = [
                {
                    "_op_type": "update",
                    "_index": self.index,
                    "_id": query_preprocess(id),
                    "doc": {
                        "author": existing_doc["author"],
                        "content": docs[id],
                        "inlinks": existing_doc["inlinks"],
                        "outlinks": existing_doc["outlinks"],
                    },
                    "doc_as_upsert": True,
                }
            ]
            helpers.bulk(self.es, actions=actions)


my_es = Elastic()
my_es.indexer()
