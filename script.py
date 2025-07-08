# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "beautifulsoup4",
#     "markdownify",
#     "requests",
# ]
# ///
from pathlib import Path
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

# Constants
SITEMAP = "https://openlineage.io/sitemap.xml"
PREFIX = "https://openlineage.io/docs/spec/"
OUTPUT_DIR = Path("openlineage-docs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_spec_links():
    """Parse the sitemap and filter links with the spec prefix."""
    resp = requests.get(SITEMAP)
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    return [
        loc.text
        for url in root.findall("sm:url", ns)
        if (loc := url.find("sm:loc", ns)) is not None and loc.text.startswith(PREFIX)
    ]


def scrape_and_convert_to_markdown(url: str) -> str | None:
    """Download the page and convert <article> to markdown."""
    print(f"Fetching: {url}")
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    article = soup.find("article")
    if not article:
        print(f"Skipping {url} (no <article> found)")
        return None
    return md(str(article), heading_style="ATX")


def save_markdown(url: str, markdown: str):
    """Save the markdown to a file using pathlib based on URL structure."""
    relative_path = url.replace(PREFIX, "").strip("/")
    if not relative_path:
        relative_path = "index"
    output_path = OUTPUT_DIR / f"{relative_path}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    print(f"Saved: {output_path}")


def run():
    for url in get_spec_links():
        md_text = scrape_and_convert_to_markdown(url)
        if md_text:
            save_markdown(url, md_text)


if __name__ == "__main__":
    run()
