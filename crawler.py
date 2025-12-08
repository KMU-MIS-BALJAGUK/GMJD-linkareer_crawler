import json
import logging
import os
import time
from datetime import datetime, date
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

import pymysql
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("LinkareerCrawler")

load_dotenv()

DEFAULT_WAIT = 8000  # ms


class LinkareerCrawler:
    BASE_URL = "https://linkareer.com"
    LIST_URL = (
        "https://linkareer.com/list/contest"
        "?filterType=CATEGORY&orderBy_direction=DESC&orderBy_field=CREATED_AT&page={page}"
    )

    def __init__(self, throttle: float = 0.3):
        self.browser = None
        self.context = None
        self.throttle = throttle

    async def start(self, headless=True):
        """Playwright Browser 시작"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        self.context = await self.browser.new_context(
            viewport={"width": 1200, "height": 900},
        )
        self.page = await self.context.new_page()

    async def stop(self):
        """Playwright Browser 종료"""
        if self.browser:
            await self.browser.close()
            self.browser = None
            logger.info("Browser closed.")

    async def fetch_list_page(self, page_number: int) -> List[str]:
        url = self.LIST_URL.format(page=page_number)
        logger.info(f"Opening list page: {url}")

        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except PlaywrightTimeout:
            logger.warning(f"Timeout while opening list page: {url}")
            return []

        # div.list-body 대기 (대기시간 충분히)
        try:
            await self.page.wait_for_selector("div.list-body", timeout=20000)
        except PlaywrightTimeout:
            logger.error("list-body not found — page load failed")
            return []

        anchors = await self.page.locator("div.list-body a[href^='/activity/']").all()

        urls = []
        seen = set()

        for a in anchors:
            try:
                href = await a.get_attribute("href")
            except PlaywrightTimeout:
                continue
            if href:
                full = urljoin(self.BASE_URL, href)
                if full not in seen:
                    seen.add(full)
                    urls.append(full)

        logger.info(f"Found {len(urls)} activity URLs on page {page_number}")
        return urls

    async def fetch_activity_details(self, url: str) -> Optional[Dict]:
        """2) 상세 페이지 스크랩"""

        logger.info(f"Visiting detail page: {url}")

        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except PlaywrightTimeout:
            logger.error(f"Timeout loading detail page: {url}")
            return None

        try:
            await self.page.wait_for_selector(
                "header[class^='ActivityInformationHeader__']"
            )
        except PlaywrightTimeout:
            logger.warning(f"No title header found — skipping: {url}")
            return None

        async def safe(selector: str):
            try:
                return await self.page.locator(selector, strict=False).inner_text(
                    timeout=2000
                )
            except:
                return None

        async def safe_attr(selector: str, attr: str):
            try:
                return self.page.locator(selector, strict=False).get_attribute(
                    attr, timeout=2000
                )
            except:
                return None

        # Playwright는 await 필요
        result = {
            "detail_url": url,
            "activity_title": await safe(
                "header[class^='ActivityInformationHeader__'] h1"
            ),
            "activity_url": await safe_attr("dl[class^='HomepageField__'] a", "href"),
            "activity_category": [],
            "start_date": await safe(".start-at + span"),
            "end_date": await safe(".end-at + span"),
            "activity_img": await safe_attr("img.card-image", "src"),
            "organization_name": await safe("div > article > header > h2"),
            # Optional Fields
            "award_scale": await safe("dl:nth-of-type(3) dd"),
            "benefits": await safe("dl:nth-of-type(6) dd"),
            "target_participants": await safe("dl:nth-of-type(2) dd"),
            "company_type": await safe("dl:nth-of-type(1) dd"),
            "views": await safe("header span:nth-child(2)"),
        }

        # categories
        cat_elements = self.page.locator("ul[class^='CategoryChipList__'] p")
        count = await cat_elements.count()
        categories = []
        for i in range(count):
            txt = await cat_elements.nth(i).inner_text()
            if txt:
                categories.append(txt)
        result["activity_category"] = categories

        return result

    async def _reset_context(self):
        if self.context:
            try:
                await self.context.close()
            except:
                pass

        self.context = await self.browser.new_context(
            viewport={"width": 1200, "height": 900}
        )
        self.page = await self.context.new_page()

    async def crawl_pages(self, max_pages=100, limit_per_page=None):
        await self.start()  # 브라우저만 실행

        all_data = []

        for page_number in range(1, max_pages + 1):

            # 🔥 페이지 시작할 때 context/page 새로 생성
            await self._reset_context()

            urls = await self.fetch_list_page(page_number)
            if not urls:
                logger.warning(f"No URLs found on page {page_number}. Stopping.")
                break

            if limit_per_page:
                urls = urls[:limit_per_page]

            for url in urls:
                data = await self.fetch_activity_details(url)
                if data:
                    all_data.append(data)

                await self.page.wait_for_timeout(self.throttle * 1000)

            logger.info(f"Finished page {page_number}")

        await self.stop()
        return all_data


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y.%m.%d").date()
    except ValueError:
        logger.warning("Cannot parse date: %s", date_str)
        return None


def _get_required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value


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


def persist_contests_to_rds(records: List[Dict]) -> None:
    if not records:
        logger.info("No records to persist. Skipping DB update")
        return

    host, port, database = _parse_mysql_url(_get_required_env("RDS_URL"))
    if os.getenv("RDS_PORT"):
        port = int(os.getenv("RDS_PORT"))
    user = _get_required_env("RDS_USERNAME")
    password = _get_required_env("RDS_PASSWORD")
    if os.getenv("RDS_DB_NAME"):
        database = os.getenv("RDS_DB_NAME")

    connection = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
    )

    with connection:
        with connection.cursor() as cursor:

            # ============================================================
            # 1️⃣ 현재 DB에 존재하는 contest 목록 로딩
            # ============================================================
            cursor.execute("SELECT id, name, organization_name FROM contests")
            existing_rows = cursor.fetchall()

            existing_map = {
                (row[1], row[2]): row[0]  # (name, organization_name) → id
                for row in existing_rows
            }

            logger.info("Loaded %d existing contests from DB", len(existing_map))

            # ============================================================
            # 2️⃣ INSERT or UPDATE 로직 적용
            # ============================================================
            insert_sql = """
                INSERT INTO contests (
                    categories, end_date, image_url, name, organization_name, site_url, start_date,
                    award_scale, benefits, additional_benefits, target_participants, company_type, views
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            update_sql = """
                UPDATE contests
                SET start_date = %s,
                    end_date   = %s,
                    views      = %s
                WHERE id = %s
            """

            insert_payloads = []
            update_payloads = []

            for record in records:
                start_date = _parse_date(record.get("start_date"))
                end_date = _parse_date(record.get("end_date"))
                image_url = record.get("activity_img")
                site_url = record.get("activity_url") or record.get("detail_url")
                title = record.get("activity_title")
                organization = record.get("organization_name") or title or "정보없음"
                categories = record.get("activity_category") or []
                category_str = ",".join(categories)
                views = int(record.get("views") or 0)

                if not (start_date and end_date and image_url and site_url and title):
                    logger.warning(
                        "Skipping invalid record: %s", record.get("detail_url")
                    )
                    continue

                key = (title, organization)

                # ====================================================
                # 존재 여부 체크 → UPDATE or INSERT
                # ====================================================
                if key in existing_map:
                    contest_id = existing_map[key]
                    update_payloads.append((start_date, end_date, views, contest_id))
                else:
                    insert_payloads.append(
                        (
                            category_str,
                            end_date,
                            image_url,
                            title,
                            organization,
                            site_url,
                            start_date,
                            record.get("award_scale") or "",
                            record.get("benefits") or "",
                            record.get("additional_benefits") or "",
                            record.get("target_participants") or "",
                            record.get("company_type") or "",
                            views,
                        )
                    )

            # ============================================================
            # 3️⃣ INSERT 실행
            # ============================================================
            if insert_payloads:
                cursor.executemany(insert_sql, insert_payloads)
                logger.info("Inserted %d new contests", len(insert_payloads))

            # ============================================================
            # 4️⃣ UPDATE 실행
            # ============================================================
            if update_payloads:
                cursor.executemany(update_sql, update_payloads)
                logger.info("Updated %d existing contests", len(update_payloads))

            connection.commit()


import asyncio


def main():
    max_pages = int(os.getenv("LINKAREER_PAGE_LIMIT", "100"))
    per_page_limit_env = os.getenv("LINKAREER_PER_PAGE_LIMIT")
    per_page_limit = int(per_page_limit_env) if per_page_limit_env else None

    crawler = LinkareerCrawler()

    records = asyncio.run(
        crawler.crawl_pages(max_pages=max_pages, limit_per_page=per_page_limit)
    )

    logging.info(f"Collected {len(records)} contest items")

    if os.getenv("SKIP_DB_WRITE", "false").lower() == "true":
        print(json.dumps(records[:2], indent=4, ensure_ascii=False))
        return

    persist_contests_to_rds(records)


if __name__ == "__main__":
    main()
