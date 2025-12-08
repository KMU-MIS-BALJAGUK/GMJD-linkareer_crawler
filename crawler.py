from __future__ import annotations
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta

import pymysql
from dotenv import load_dotenv

# --- Selenium & BS4 ---
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("LinkareerCrawler")
load_dotenv()

DEFAULT_WAIT = 10


# -----------------------------------------------------------
# 1) BeautifulSoup 기반 HTML 파서
# -----------------------------------------------------------
def parse_list_page(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = []

    for a in soup.select("div.list-body a[href^='/activity/']"):
        href = a.get("href")
        if href:
            urls.append(urljoin("https://linkareer.com", href))

    return list(dict.fromkeys(urls))  # unique 유지


def parse_detail_page(html: str, url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")

    safe = lambda q: (
        soup.select_one(q).get_text(strip=True) if soup.select_one(q) else None
    )
    safe_attr = lambda q, a: (soup.select_one(q).get(a) if soup.select_one(q) else None)

    categories = [
        p.get_text(strip=True) for p in soup.select("ul[class^='CategoryChipList__'] p")
    ]

    return {
        "detail_url": url,
        "activity_title": safe("header[class^='ActivityInformationHeader__'] h1"),
        "activity_url": safe_attr("dl[class^='HomepageField__'] a", "href"),
        "activity_category": categories,
        "start_date": safe(".start-at + span"),
        "end_date": safe(".end-at + span"),
        "activity_img": safe_attr("img.card-image", "src")
        or safe_attr("div.poster img", "src"),
        "organization_name": safe("div > article > header > h2"),
        # Optional fields
        "award_scale": safe("dl:nth-of-type(3) dd"),
        "benefits": safe("dl:nth-of-type(6) dd"),
        "additional_benefits": safe("dl:nth-of-type(8) dd"),
        "target_participants": safe("dl:nth-of-type(2) dd"),
        "company_type": safe("dl:nth-of-type(1) dd"),
        "views": safe(
            "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > header > div > span:nth-child(2)"
        ),
    }


# -----------------------------------------------------------
# 2) Selenium 최소 로딩 구조
# -----------------------------------------------------------
class LinkareerCrawler:
    BASE_URL = "https://linkareer.com"
    LIST_URL = (
        BASE_URL
        + "/list/contest?filterType=CATEGORY&orderBy_direction=DESC&orderBy_field=CREATED_AT&page={page}"
    )

    def __init__(self, headless=True):
        self.headless = headless
        self.driver = None

    def _make_driver(self):
        opts = Options()
        opts.binary_location = "/opt/google/chrome/google-chrome"
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-software-rasterizer")
        opts.add_argument("--window-size=1200,900")

        # 🔥 JS 로딩 기다리지 않도록 설정
        opts.page_load_strategy = "normal"

        # 이미지 로딩 OFF → 속도 2배
        opts.add_experimental_option(
            "prefs", {"profile.managed_default_content_settings.images": 2}
        )

        service = Service(ChromeDriverManager(driver_version="143.0.7499.40").install())
        return webdriver.Chrome(service=service, options=opts)

    def start(self):
        self.driver = self._make_driver()

    def stop(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    # -----------------------------------------------------------
    # 페이지 HTML만 빠르게 가져오기
    # -----------------------------------------------------------
    def get_html(self, url: str) -> Optional[str]:
        try:
            self.driver.get(url)
            time.sleep(0.7)  # JS렌더링 기다리지 않음 → 최소 대기만
            return self.driver.page_source
        except Exception as e:
            logger.error("Error loading %s: %s", url, e)
            return None

    # -----------------------------------------------------------
    # 리스트 페이지 > detail URL들 추출
    # -----------------------------------------------------------
    def fetch_list_urls(self, page: int) -> List[str]:
        url = self.LIST_URL.format(page=page)
        logger.info(f"Fetching list page: {url}")

        # 페이지 로드
        self.driver.get(url)

        # 1) list-body 라는 컨테이너가 등장할 때까지 잠깐 기다림
        #    (JS 렌더링 전이더라도 컨테이너는 먼저 HTML에 등장함)
        try:
            for _ in range(20):
                html = self.driver.page_source
                if "list-body" in html:
                    break
                time.sleep(0.2)
        except Exception:
            pass

        # 2) 실제 HTML 파싱
        html = self.driver.page_source
        if not html:
            return []

        urls = parse_list_page(html)
        logger.info("Found %d URLs on page %d", len(urls), page)
        return urls

    # -----------------------------------------------------------
    # 상세 페이지 데이터 가져오기
    # -----------------------------------------------------------
    def fetch_detail(self, url: str) -> Optional[Dict]:
        logger.info(f"Visiting detail: {url}")

        html = self.get_html(url)
        if not html:
            return None

        data = parse_detail_page(html, url)
        return data

    # -----------------------------------------------------------
    # 전체 페이지 크롤링
    # -----------------------------------------------------------
    def crawl(self, max_pages=50, limit_per_page=None):
        records = []

        for page in range(1, max_pages + 1):
            logger.info(f"--- Page {page} ---")

            self.start()
            time.sleep(0.5)
            urls = self.fetch_list_urls(page)
            self.stop()

            if not urls:
                logger.info("No URLs found. Stopping.")
                break

            if limit_per_page:
                urls = urls[:limit_per_page]

            # 상세 페이지는 다시 Selenium 실행
            self.start()

            for url in urls:
                data = self.fetch_detail(url)
                if data:
                    records.append(data)

            self.stop()

        return records


# -----------------------------------------------------------
# 3) 날짜 파싱
# -----------------------------------------------------------
def _parse_date(date_str: Optional[str]):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y.%m.%d").date()
    except:
        return None


def _parse_mysql_url(url: str) -> tuple[str, int, str]:
    """jdbc:mysql://host:port/db 형태를 host, port, db 로 파싱"""
    if url.startswith("jdbc:"):
        url = url[len("jdbc:") :]
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 3306
    database = parsed.path.lstrip("/") if parsed.path else None
    if not host or not database:
        raise RuntimeError(
            "RDS_URL must include host and database, e.g. mysql://host:3306/dbname"
        )
    return host, port, database


# -----------------------------------------------------------
# 4) DB 저장
# -----------------------------------------------------------
def persist_contests_to_rds(records: List[Dict]) -> None:
    if not records:
        logger.info("No records to persist.")
        return

    host, port, db = _parse_mysql_url(os.getenv("RDS_URL"))
    user = os.getenv("RDS_USERNAME")
    password = os.getenv("RDS_PASSWORD")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        autocommit=False,
    )

    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name, organization_name FROM contests")
            existing = {(r[1], r[2]): r[0] for r in cursor.fetchall()}

            insert_sql = """
                INSERT INTO contests (
                    categories, end_date, image_url, name, organization_name,
                    site_url, start_date, award_scale, benefits, additional_benefits,
                    target_participants, company_type, views
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            update_sql = """UPDATE contests SET start_date=%s, end_date=%s, views=%s, site_url=%s WHERE id=%s"""

            inserts = []
            updates = []

            for rec in records:
                start = _parse_date(rec.get("start_date"))
                end = _parse_date(rec.get("end_date"))
                img = rec.get("activity_img")
                title = rec.get("activity_title")
                org = rec.get("organization_name") or "정보없음"

                if not (start and end and img and title):
                    continue

                key = (title, org)
                views = int(rec.get("views") or 0)

                if key in existing:
                    updates.append(
                        (
                            start,
                            end,
                            views,
                            rec.get("activity_url") or rec.get("detail_url") or "",
                            existing[key],
                        )
                    )
                else:
                    inserts.append(
                        (
                            ",".join(rec.get("activity_category", [])),
                            end,
                            img,
                            title,
                            org,
                            rec.get("activity_url") or rec.get("detail_url") or "",
                            start,
                            rec.get("award_scale") or "",
                            rec.get("benefits") or "",
                            rec.get("additional_benefits") or "",
                            rec.get("target_participants") or "",
                            rec.get("company_type") or "",
                            views,
                        )
                    )

            # 종료된 공모전 삭제
            kst_today = (datetime.utcnow() + timedelta(hours=9)).date()

            cursor.execute("DELETE FROM contests WHERE end_date < %s", (kst_today,))

            if inserts:
                cursor.executemany(insert_sql, inserts)
            if updates:
                cursor.executemany(update_sql, updates)

            conn.commit()


# -----------------------------------------------------------
# 5) Main
# -----------------------------------------------------------
def main():
    crawler = LinkareerCrawler(headless=True)

    records = crawler.crawl(
        max_pages=int(os.getenv("LINKAREER_PAGE_LIMIT", 50)),
        limit_per_page=int(os.getenv("LINKAREER_PER_PAGE_LIMIT", 28)),
    )

    logger.info(f"Collected {len(records)} items")

    if os.getenv("SKIP_DB_WRITE", "false") == "true":
        print(json.dumps(records[:3], indent=4, ensure_ascii=False))
        return

    persist_contests_to_rds(records)


if __name__ == "__main__":
    main()
