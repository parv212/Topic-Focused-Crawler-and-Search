from urllib.parse import urlparse
import streamlit as st
from elasticsearch7 import Elasticsearch


def domain_retrieval(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return domain


st.markdown(
    f'<h1 style="color:white;">{"Vertical Search Page HW3"}</h1>',
    unsafe_allow_html=True,
)
with st.form("my_form"):
    text_query = st.text_input("Enter your Query")
    submitted = st.form_submit_button("Submit")
    if submitted:
        INDEX = "search-apple-tech"
        cloud_id = "8660e1288ca3484ea440e8357734116f:dXMtZWFzdC0yLmF3cy5lbGFzdGljLWNsb3VkLmNvbTo0NDMkNDk1ODRlZjk0YmJjNDMyNDhjNDYyY2RiYTlkNmFiMzYkNDYzOGJjNWY1ZWE1NGVkZjhkMjM5NzM2ZGI1MjA2ZTg="
        es = Elasticsearch(
            request_timeout=10000,
            cloud_id=cloud_id,
            api_key=(
                "apple-tech-1",
                "SzJUN0pZd0JpM1ZKNzd5b3gzZnQ6S2x3WXhxQ1pRUi1GeTZVaG9vVGkzUQ==",
            ),
            http_auth=("elastic", "nBP49VgJJfZD9XWgzkxU16y0"),
        )
        response = es.search(
            index=INDEX, body={"query": {"match": {"content": text_query}}}, size=100
        )
        display_list = {}
        new_dict = {}
        for hit in response["hits"]["hits"]:
            display_list[hit["_id"]] = [
                hit["_source"]["content"],
                hit["_source"]["author"],
            ]
        for i in display_list:
            domain = domain_retrieval(i)
            if domain not in new_dict:
                new_dict[domain] = [i, display_list[i]]
            else:
                if len(new_dict[domain]) < 2:
                    new_dict[domain].append([i, display_list[i]])
        for i in new_dict.values():
            st.write(i[0])
            with st.expander("Click Here for article text"):
                st.write("Author")
                st.write(i[1][1])
                st.write("Text")
                st.write(i[1][0])
