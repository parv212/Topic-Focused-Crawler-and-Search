import json


def write_contents(url: str, text: str, title=None):
    with open("documents.txt", "a", encoding="utf-8") as f:
        f.write("<DOC>\n")
        f.write("<DOCNO>{}</DOCNO>\n".format(url))
        if title is not None:
            f.write("<HEAD><TITLE>{}</TITLE></HEAD>\n".format(title))
        f.write("<TEXT>\n")
        f.write(text + "\n")
        f.write("</TEXT>\n")
        f.write("</DOC>\n")


def write_raw_html(raw_html: dict):
    with open("raw_html.json", "a") as f:
        json.dump(raw_html, f)
        f.write("\n")


def write_out_links(out_links: dict):
    with open("out_links.json", "a", encoding="utf-8") as f:
        json.dump(out_links, f)
        f.write("\n")


def write_in_links(in_links: dict):
    with open("in_links.json", "a", encoding="utf-8") as f:
        json.dump(in_links, f)
        f.write("\n")
