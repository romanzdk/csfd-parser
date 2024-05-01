# mypy: disable-error-code="union-attr"
import asyncio
import os
import random
from typing import Any

import aiohttp
import bs4
import pandas as pd

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.csfd.cz/soukrome/chci-videt/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}


def load_want_to_see_htmls() -> set[str]:
    html_contents = set()
    directory = "data"
    for filename in os.listdir(directory):
        if filename.endswith(".html"):
            full_path = os.path.join(directory, filename)
            print(f"Reading = {full_path}")
            with open(full_path, "r", encoding="utf-8") as file:
                html_contents.add(file.read())
    return html_contents


def extract_movies_urls(html_pages: set[str]) -> set[str]:
    urls = set()
    for page in html_pages:
        soup = bs4.BeautifulSoup(page)
        table = soup.find_all("div", {"class": "watchlist-table"})[0]
        for row in table:
            if isinstance(row, bs4.element.NavigableString):
                continue
            url = row.find("h3").find("a")["href"]
            if "https:" not in url:
                url = "https://www.csfd.cz" + url
            urls.add(url)
    return urls


async def fetch(
    session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore
) -> str:
    async with semaphore:
        await asyncio.sleep(random.uniform(0.5, 2.0))
        print(url)
        async with session.get(url) as response:
            resp = await response.text()
            print(f"{url} downloaded")
            return resp


async def download(urls: set[str]) -> list[Any]:
    tasks = []
    semaphore = asyncio.Semaphore(5)

    async with aiohttp.ClientSession() as session:
        for url in urls:
            tasks.append(fetch(session, url, semaphore))
        return await asyncio.gather(*tasks)


def get_by_class(html: bs4.BeautifulSoup, class_: str, element: str = "div") -> str:
    return str(html.find(element, {"class": class_}).text)


def extract_movie_metadata(html: str) -> dict[str, str]:
    metadata = {}
    movie_html = bs4.BeautifulSoup(html)

    name = metadata["name"] = movie_html.find("h1").text.strip()
    try:
        metadata["genres"] = get_by_class(movie_html, class_="genres")
        try:
            metadata["type"] = (
                get_by_class(movie_html, element="span", class_="type")
                .replace("(", "")
                .replace(")", "")
                .title()
            )
        except AttributeError:
            metadata["type"] = "Film"
        origin_year_length = get_by_class(movie_html, class_="origin").split("\n")
        metadata["origin"] = origin_year_length[0].strip()
        metadata["year"], metadata["length"] = origin_year_length[1].strip().split(",")
        metadata["rating"] = float(
            get_by_class(movie_html, class_="film-rating-average")
            .strip()
            .replace("%", "")
        )
        metadata["link"] = movie_html.find("link")["href"]  # type:ignore[index]
        metadata["plot"] = (
            movie_html.find("div", {"class": "plot-full"})
            .text.strip()
            .replace("\t", "")
            .replace("\n\n", "")
        )
    except AttributeError as e:
        print(name, e)
    return metadata


async def main():
    source_htmls = load_want_to_see_htmls()
    urls = extract_movies_urls(source_htmls)
    htmls = await download(urls)

    result = []
    for html in htmls:
        result.append(extract_movie_metadata(html))

    pd.DataFrame(result).to_excel("movies.xlsx", index=False)


if __name__ == "__main__":
    asyncio.run(main())
