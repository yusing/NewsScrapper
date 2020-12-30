import asyncio
import concurrent.futures
import threading
import warnings
from asyncio.runners import run
from datetime import datetime
from textwrap import wrap
from time import sleep
from urllib.parse import urlparse

import mysql.connector
import newspaper
import requests
from bs4 import BeautifulSoup
from bs4.element import ResultSet
from dateutil.parser import parse
from mysql.connector.cursor import MySQLCursor
from newspaper import Config
from newspaper.article import Article

warnings.filterwarnings("ignore") # suppress all warnings

log_file = open('log.txt', 'w')
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
}
feeds_url = [
    'https://trends.google.com/trends/trendingsearches/daily/rss?geo=HK', # Google Trends HK
    'https://www.reddit.com/.rss', # Reddit main page
    # add any rss feed below (e.g. https://www.youtube.com/feeds/videos.xml?channel_id=[CHANNEL_ID]
]

parse_done : int
articles_count : int
feeds_done : int
rows_inserted : int
last_url : str
show_rss_dl_status : bool
show_scrapping_status : bool
show_timer : bool
feeds : list[tuple[str,str]] = []
config_en = Config()
config_en.memoize_articles = False
config_en.browser_user_agent = headers["User-Agent"]
config_en.headers = headers
config_en.fetch_images = False

def log(text):
    log_file.write(str(text))
    log_file.write('\n')
def background(f):
    def wrapped(*args, **kwargs):
        try:
            return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)
        except:
            return asyncio.new_event_loop().run_in_executor(None, f, *args, **kwargs)
    return wrapped
@background
def insert_db(entry: tuple[str]):
    global last_url
    global rows_inserted
    try:
        db = mysql.connector.connect(
            host='localhost',
            user='root',
            database='news'
        )
        last_url = entry[5]
        cursor : MySQLCursor= db.cursor()
        cursor.execute('''
        INSERT IGNORE INTO news (source, date, title, summary, text, url) 
        VALUES (%s, %s, %s, %s, %s, %s)
        ''', entry)
        db.commit()
        db.close()
        if cursor.rowcount != 0:
            rows_inserted += 1
    except:
        pass
@background
def setShowRSSDownloadStatus(show : bool):
    global show_rss_dl_status
    show_rss_dl_status = show
    while show_rss_dl_status:
        print('\033[H\033[J', end='')
        print(f'{len(feeds)}/{len(feeds_url)} ({len(feeds)/len(feeds_url)*100:.2f}%) Downloading RSS')
        sleep(.1)
@background
def setShowScrappingStatus(show : bool):
    global show_scrapping_status
    global last_url
    global rows_inserted
    show_scrapping_status = show
    while articles_count == 0:
        sleep(.5)
    while show_scrapping_status:
        print('\033[H\033[J', end='')
        print(f'Scrapping articles {parse_done}/{articles_count} ({parse_done/articles_count*100:.2f}%) ({rows_inserted} rows inserted)', end=' ')
        print(f'URL: "{last_url[:80]+"... (truncated)" if len(last_url)>80 else last_url}"')
        sleep(.1)
@background
def setShowTimer(show: bool, time: int = 0):
    global show_timer
    show_timer = show
    while time > 0:
        print('\033[H\033[J', end='')
        print(f'Next scrapping is starting in {time//3600:02d}:{(time%3600)//60:02d}:{time%60:02d}')
        time -= 1
        sleep(1)
@background
def downloadRSS(url: str):
    global headers
    r = requests.get(url, headers=headers)
    if r.status_code == requests.codes.ok:
        feeds.append((r.content, url))
    else:
        log(f'failed to download RSS from {url}')
        feeds.append((None, None))
@background
def parseArticle(articles: ResultSet, host: str, src_ele: str, summary_ele: str, date_ele: str, url_ele: str):
    global parse_done
    global config_en
    global articles_count
    articles_count += len(articles)
    for a in articles:
        src = a.find(src_ele)
        summary = a.find(summary_ele)
        date = a.find(date_ele)
        if src is None:
            src = host
        else:
            src = src.text
        if summary is None:
            summary = a.find('description') # fallback
        if summary is not None:
            summary = summary.text
        url = a.find(url_ele)
        if url is not None:
            url = url.text.strip()
        else:
            url = ''
        if url != '':
            article = Article(url, config=config_en)
            if date is not None:
                try:
                    date = parse(date.text)
                except:
                    date = None
            try:
                article.download()
                article.parse()
            except Exception as ex:
                log(f'{ex}, url is "{url}"')
            finally:
                if article.publish_date is datetime and date is None:
                    date = article.publish_date.strftime('%Y-%m-%d %H:%M:%S')
                insert_db((src, date, article.title, summary, article.text, article.url))
        parse_done += 1
@background
def parseRedditComments(comments: ResultSet, subreddit: str, link: str, title: str, summary: str, date: str):
    global parse_done
    global articles_count
    text = ''
    articles_count += 1
    for comment in comments:
        if comment.author is not None:
            try:
                user = comment.author.find('name').text
                date = parse(comment.updated.text).strftime('%Y-%m-%d %H:%M')
                content = BeautifulSoup(comment.content.text).p
                if content is not None:
                    content = content.text
                    formatted = "\n    ".join(wrap(content, 70))
                    text += f'{user} ({date}):\n    {formatted}\n'
            except Exception as ex:
                log(ex)
    insert_db((subreddit, date, title, summary, text, link))
    parse_done += 1
@background
def parseYouTubeRSS(a):
    global articles_count
    global parse_done
    articles_count += 1
    title = a.find('title').text
    link = a.find('link', {'rel': 'alternate'})['href']
    author = f'YouTube/{a.author.find("name").text}'
    date = parse(a.find('published').text).strftime('%Y-%m-%d %H:%M')
    summary = a.find('media:group').find('media:description').text
    # source, date, title, summary, text, url
    insert_db((author, date, title, summary, None, link))
    parse_done += 1
@background
def scrapRSS(rss:str, url: str):
    soup = BeautifulSoup(rss, features='xml')
    if 'reddit.com' in url:
        for a in soup.findAll('entry'):
            url = a.find('link')['href']+'/.rss'
            r = requests.get(url, headers=headers)
            if r.status_code != requests.codes.ok:
                log(f'failed to download reddit rss {url}')
                continue
            bs = BeautifulSoup(r.content, features='xml')
            subreddit = bs.find('category')['label']
            link = bs.find('link', {'rel': 'alternate'})['href']
            summary = bs.find('subtitle')
            if summary is not None:
                summary = summary.text
            title = bs.find('title').text
            date = parse(bs.find('updated').text).strftime('%Y-%m-%d %H:%M')
            comments = bs.find_all('entry')
            parseRedditComments(comments, subreddit, link, title, summary, date)
    elif 'youtube.com' in url:
        for a in soup.findAll('entry'):
            parseYouTubeRSS(a)
    else:
        if 'trends.google.com' in url:
            root = 'ht:news_item'
            url_ele = 'ht:news_item_url'
            src_ele = 'ht:news_item_source'
            summary_ele = 'ht:news_item_snippet'
            date_ele = 'pubDate'
        else:
            root = 'item'
            url_ele = 'link'
            src_ele = 'source'
            summary_ele = 'summary'
            date_ele = 'pubDate'
        try:
            host = urlparse(url).netloc
        except Exception as ex:
            log(f'Error parsing url "{ex}"')
            return
        if soup.find(root) is not None:
            articles = soup.findAll(root)
        else:
            articles = soup.findAll('entry')
        if articles is not None:
            try:
                parseArticle(articles, host, src_ele, summary_ele, date_ele, url_ele)
            except Exception as ex:
                log(ex)
def fetch_news():
    db = mysql.connector.connect(
        host='localhost',
        user='root',
        database='news'
    )
    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS news(
        url VARCHAR(512) UNIQUE,
        source VARCHAR(255) NOT NULL,
        date DATETIME,
        title VARCHAR(255) NOT NULL UNIQUE,
        summary VARCHAR(1024) NOT NULL,
        text TEXT NOT NULL
    )''')
    print('\033[?25l') # hide cursor
    global feeds
    global feeds_done
    global parse_done
    global articles_count
    global last_url
    global rows_inserted
    def downloadAllFeeds():
        feeds.clear()
        for feed in feeds_url:
            downloadRSS(feed)
        while (len(feeds) < len(feeds_url)):
            sleep(1)
    def parseAllArticles():
        for feed, url in feeds:
            if feed is not None:
                scrapRSS(feed, url)
        while (parse_done < articles_count) or articles_count == 0:
            sleep(1)
    while True:
        parse_done = 0
        articles_count = 0
        feeds_done = 0
        rows_inserted = 0
        last_url = ''
        setShowRSSDownloadStatus(True)
        downloadAllFeeds()
        setShowRSSDownloadStatus(False)
        setShowScrappingStatus(True)
        parseAllArticles()
        setShowScrappingStatus(False)
        setShowTimer(True, 1800)
        sleep(1800)
if __name__ == "__main__":
    fetch_news()
