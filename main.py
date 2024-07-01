"""
main.py
"""

import os
import re
import time
import random
import logging
import traceback

from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup

from database import (
    get_stock_values,
    insert_stock_values,
    get_posts_url,
    insert_into_posts,
    insert_into_comments,
)

load_dotenv()

CSV_FILE_PATH = "input.csv"
REDDIT_URL = "https://www.reddit.com/r/ausstocks/new/"
HOTCOPPER_URL = "https://hotcopper.com.au/postview/"

# For Reddit
USERNAME = os.getenv("REDDIT_USER_NAME")
PASSWORD = os.getenv("REDDIT_PASS_WORD")

# Setup logging
logging.basicConfig(
    filename=f"logs/app-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Set the path to the ChromeDriver executable
chromedriver_path = "chromedriver-32.exe"
service = Service(chromedriver_path)

# Set the path to the Portable Chrome executable
chrome_exe_path = "chrome-win32/chrome.exe"
chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = chrome_exe_path
chrome_options.add_argument("--blink-settings=imagesEnabled=false")


def normalize_text(text: str) -> str:
    """
    Normalize the given `text` by converting Unicode quote characters to their ASCII equivalents,
    normalizing whitespace within lines, and reducing excessive newline characters.

    Parameters:
    - text (str): The text to be normalized.

    Returns:
    - str: The normalized text.
    """
    # Define a mapping of Unicode quote characters to their ASCII equivalents
    quote_mapping = {
        "‘": "'",
        "’": "'",
        "“": '"',
        "”": '"',
        "‛": "'",
        "‟": '"',
        # Add more mappings as needed
    }

    # Use regex to substitute Unicode quote characters with ASCII equivalents
    pattern = re.compile("|".join(map(re.escape, quote_mapping.keys())))
    converted_text = pattern.sub(lambda match: quote_mapping[match.group(0)], text)

    # Normalize whitespace within lines
    lines = [re.sub(r"\s+", " ", line).strip() for line in converted_text.split("\n")]
    spacefree_text = "\n".join(lines)

    # Replace three or more new-line characters with two new-line characters
    normalized_text = re.sub(r"\n{3,}", "\n\n", spacefree_text).strip()

    return normalized_text


def evaluate_stock(stknames, title, content):
    """
    Parameters:
    - stknames(list[dict(id, stk)]): List of (stock_id, stock_name)

    Returns:
    `presents`, `stockid`, `stockname`."""
    text = title + " " + content
    for stk in stknames:
        id, name = stk["id"], stk["stk"]
        pattern = r'\b{}\b'.format(re.escape(name))  # Word boundary match for name
        if re.search(pattern, text):
            return True, id, name

    return False, -1, ""


def get_data_reddit(driver, href, stknames):
    data = {}
    h1 = ""
    graphs = []
    try:
        match = re.search(r"/comments/([^/]+)/", href)
        comment_id = match.group(1) if match else ""

        if comment_id:
            driver.get(href)
            time.sleep(random.randint(1, 4))

            try:
                id = "t3_" + comment_id + "-read-more-button"
                driver.find_element(By.ID, id).click()
            finally:
                pass

            page_source = driver.page_source
            # Use BeautifulSoup to parse the page source
            soup = BeautifulSoup(page_source, "html.parser")

            # Extract text from h1 tag
            h1_tag = soup.find("h1", {"id": "post-title-t3_" + comment_id})

            if h1_tag:
                h1 = h1_tag.text.strip()

            # Extract text from all paragraphs in the specified div
            div_tag = soup.find(
                "div", {"id": "t3_" + comment_id + "-post-rtjson-content"}
            )
            if div_tag:
                paras = div_tag.find_all("p")
                for paragraph in paras:
                    graphs.append(paragraph.text.strip())

            presents, stockid, stockname = evaluate_stock(
                stknames, h1, normalize_text("\n".join(graphs))
            )
            if presents:
                comms = []
                try:
                    comments = soup.find(
                        "div", {"id": "comment-tree-content-anchor-" + comment_id}
                    )
                    if comments:
                        shreddit = comments.find_all("shreddit-comment")
                        for x in shreddit:
                            thingid = x.get("thingid")
                            if thingid:
                                auth = x.get("author")
                                div_tag = x.find(
                                    "div", {"id": thingid + "-comment-rtjson-content"}
                                )
                                if div_tag:
                                    paras = div_tag.find_all("p")
                                    ps = []
                                    for paragraph in paras:
                                        ps.append(paragraph.text.strip())
                                    if ps:
                                        comms.append(
                                            [auth, normalize_text("\n".join(ps))]
                                        )
                finally:
                    data = {
                        "type": "Reddit",
                        "url": href,
                        "post_id": comment_id,
                        "title": normalize_text(h1),
                        "description": normalize_text("\n".join(graphs)),
                        "stockid": stockid,
                        "stockname": stockname,
                        "comments": comms,
                    }

    except Exception as ex:
        logger.exception(f"Error!: {str(ex)}\n{traceback.format_exc()}")

    finally:
        return data


def reddit(driver, stknames, saved_urls) -> bool:
    "Perform reddit scraping."
    try:
        # startIF the driver can't access to reddit.com,
        driver.get("https://www.reddit.com/login/")
        time.sleep(2)
        driver.find_element(By.ID, "login-username").send_keys(USERNAME)
        time.sleep(5)
        driver.find_element(By.ID, "login-password").send_keys(PASSWORD)
        time.sleep(20)
        driver.find_element(By.ID, "login-password").send_keys(Keys.ENTER)
        time.sleep(10)
        driver.get(REDDIT_URL)
        time.sleep(5)

        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        # Find all <a> tags with slot="full-post-link"
        full_post_links = soup.find_all("a", {"slot": "full-post-link"})

        # Extract href attributes from the found tags
        hrefs = [
            ("https://www.reddit.com" + link.get("href")) for link in full_post_links
        ]

        for href in hrefs:
            if href not in saved_urls:
                data = get_data_reddit(driver, href, stknames)
                if not data:
                    continue
                postid = insert_into_posts(
                    type=data["type"],
                    url=data["url"],
                    post_id=data["post_id"],
                    title=data["title"],
                    description=data["description"],
                    stockid=data["stockid"],
                    stockname=data["stockname"],
                )
                logger.info(
                    f"One post is inserted to database: postid=`{postid}`"
                    f"stkname=`{data["stockname"]}`, url=`{data["url"]}`"
                )
                if postid < 0:
                    logger.warn("Error: 1245.")
                else:
                    for comments in data["comments"]:
                        comment_insert_result = insert_into_comments(
                            text=comments[1],
                            author=comments[0],
                            postid=postid,
                            stockid=data["stockid"],
                            stockname=data["stockname"],
                        )
                        if comment_insert_result < 0:
                            logger.warn("Error: 4592.")
                        else:
                            logger.info("One comment is inserted to database.")
            else:
                break
        return True
    except Exception as ex:
        logger.exception(f"Exception 2342!: {str(ex)}\n{traceback.format_exc()}")
        return False


def process_hot_copper(driver, title, href, stockid, stockname):
    try:
        driver.get(href)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")
        quote = soup.find("blockquote", {"class": "message-text ugc baseHtml"}).text
        quote = normalize_text(quote)
        if quote:
            index = href.find("post_id=")
            if index != -1:
                post_id = href[index + len("post_id=") :]
            else:
                post_id = ""
            insert_into_posts(
                type="Hotcopper",
                url=href,
                post_id=post_id,
                title=title,
                description=quote,
                stockid=stockid,
                stockname=stockname,
            )
    except Exception as ex:
        logger.exception(f"Error!: {str(ex)}\n{traceback.format_exc()}")


def hot_copper(driver, stknames, saved_urls) -> bool:
    try:
        driver.get(HOTCOPPER_URL)
        time.sleep(5)
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, "html.parser")

        data = []
        table_body = soup.find("table", {"class": "table is-fullwidth"})
        all_rows = table_body.find_all("tr")
        for row in all_rows:
            href = title = pill = title1 = ""
            try:
                title = (
                    row.find(
                        "td",
                        {
                            "class": "forum-td no-overflow is-hidden-touch has-text-weight-semibold"
                        },
                    )
                    .find("a")
                    .text
                )
            except AttributeError:
                title = ""  # Handle the case when the attribute is not found

            try:
                pill = row.find("span", {"class": "stock-pill "}).find("a").text
            except AttributeError:
                pill = ""  # Handle the case when the attribute is not found

            try:
                t = row.find("a", {"class": "subject-a"})
                title1 = t.text
                href = "https://hotcopper.com.au" + t["href"]
            except AttributeError:
                title1 = ""  # Handle the case when the attribute is not found

            if href in saved_urls:
                break
            if title1 and href:
                title = normalize_text(" ".join([title, pill, title1]))
                presents, stockid, stockname = evaluate_stock(
                    stknames=stknames, title=title, content=""
                )
                if presents:
                    data.append([title, href, stockid, stockname])
        for x in data:
            process_hot_copper(
                driver=driver, title=x[0], href=x[1], stockid=x[2], stockname=x[3]
            )
        return True
    except Exception as ex:
        logger.exception(f"Exception 7803!: {str(ex)}\n{traceback.format_exc()}")
        return False


def run_application():
    try:
        stknames_db = [
            stk_dict["stk"] for stk_dict in get_stock_values()
        ]  # Fetch stock values from database

        df = pd.read_csv(CSV_FILE_PATH, header=None)
        stknames_csv = df.iloc[:, 0].tolist()

        missing_names_list = [name for name in stknames_csv if name not in stknames_db]
        insert_stock_values(missing_names_list)

        stknames = get_stock_values()  # list[dict(id, stk)]

        saved_urls = get_posts_url()

        # Create a Chrome driver instance
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # Run Reddit and HotCopper scraping
        reddit_success = reddit(driver, stknames, saved_urls)
        hot_copper_success = hot_copper(driver, stknames, saved_urls)

        if reddit_success and hot_copper_success:
            print("Scraping completed successfully.")
        else:
            print("Scraping encountered errors.")
    except Exception as ex:
        logger.exception(f"Error!: {str(ex)}\n{traceback.format_exc()}")
    finally:
        driver.quit()


if __name__ == "__main__":
    run_application()
