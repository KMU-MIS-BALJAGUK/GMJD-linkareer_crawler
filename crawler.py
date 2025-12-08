from __future__ import annotations
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import pymysql
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
# "LinkareerCrawler" 로거 생성
logger = logging.getLogger("LinkareerCrawler")

load_dotenv()

DEFAULT_WAIT = 12


class LinkareerCrawler:
    """
    링커리어(https://linkareer.com) 크롤링 클래스.
    """

    # 최신순으로 정렬된 공모전 목록 페이지 URL의 기본 형태
    Newest_Url = "https://linkareer.com/list/contest?filterType=CATEGORY&orderBy_direction=DESC&orderBy_field=CREATED_AT&page="

    BASE_URL = "https://linkareer.com"

    LIST_PATH = "/list/contest"

    def __init__(
        self,
        headless: bool = True,
        wait_time: int = DEFAULT_WAIT,
        viewport: tuple = (1200, 900),
        throttle: float = 1.0,
    ):
        """
        Args:
            headless (bool): True일 경우 브라우저 창을 띄우지 않고 백그라운드에서 실행
            wait_time (int): 웹 요소가 나타날 때까지 기다리는 최대 시간(초)
            viewport (tuple): 브라우저 창 크기를 (너비, 높이) 튜플로 설정
            throttle (float): 각 HTTP 요청 사이에 추가하는 대기 시간(초)
        """
        self.headless = headless
        self.wait_time = wait_time
        self.viewport = viewport
        self.throttle = throttle
        self.driver = None

    def _make_driver(self):
        opts = Options()

        opts.binary_location = "/opt/google/chrome/google-chrome"

        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-software-rasterizer")
        opts.add_argument("--remote-debugging-port=9222")
        opts.add_argument("--window-size=1200,900")

        prefs = {"profile.managed_default_content_settings.images": 2}
        opts.add_experimental_option("prefs", prefs)

        # 버전 고정 (driver_version)
        chrome_driver_path = ChromeDriverManager(
            driver_version="143.0.7499.40"
        ).install()

        service = Service(chrome_driver_path)

        return webdriver.Chrome(service=service, options=opts)

    def start(self):
        """웹 드라이버 시작"""
        if self.driver is None:
            logger.info("Starting WebDriver")
            self.driver = self._make_driver()

    def stop(self):
        """웹 드라이버를 안전하게 종료하고 리소스를 해제"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
            logger.info("WebDriver stopped.")

    def fetch_activity_urls(self) -> List[str]:
        """
        URL 이동 없이, 현재 페이지의 리스트 영역에서만 activity URL들을 추출.
        (React 렌더링 안정화 포함)
        """
        self.start()
        driver = self.driver

        wait = WebDriverWait(driver, self.wait_time)

        # 1) list-body 로딩 대기
        try:
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.list-body"))
            )
        except TimeoutException:
            logger.warning("Timeout waiting for list-body on current page")
            return []

        # 2) React 렌더링 안정화 (anchor 개수 변화 감지)
        prev_count = -1
        stable_count = 0
        for _ in range(20):
            anchors = driver.find_elements(
                By.CSS_SELECTOR, "div.list-body a[href^='/activity/']"
            )
            curr_count = len(anchors)

            if curr_count == prev_count:
                stable_count += 1
            else:
                stable_count = 0

            if stable_count >= 3:  # 3번 연속 동일 → 렌더링 완료
                break

            prev_count = curr_count
            time.sleep(0.2)

        # 3) anchors 파싱
        anchors = driver.find_elements(
            By.CSS_SELECTOR, "div.list-body a[href^='/activity/']"
        )

        logger.info("Found %d anchors on current page", len(anchors))

        seen = set()
        urls = []

        for el in anchors:
            try:
                href = el.get_attribute("href")
                if not href:
                    continue

                full_url = urljoin(self.BASE_URL, href)
                if full_url not in seen:
                    seen.add(full_url)
                    urls.append(full_url)
            except Exception:
                continue

        logger.info("Found %d unique activity URLs on current page", len(urls))
        return urls

    def fetch_activity_details(self, detail_url: str) -> Optional[Dict]:
        """
        상세 정보 추출
        activity 상세 페이지를 방문하여 세부 정보를 추출

        Args:
            detail_url (str): 정보를 추출할 상세 페이지의 절대 URL.

        Returns:
            Optional[Dict]: 추출된 정보가 담긴 딕셔너리. 실패 시 None을 반환.
        """
        self.start()
        driver = self.driver
        logger.info("Visiting detail page: %s", detail_url)

        # 🔥 새 탭 열기
        driver.execute_script(f"window.open('{detail_url}', '_blank');")
        driver.switch_to.window(driver.window_handles[-1])

        wait = WebDriverWait(driver, self.wait_time)
        try:
            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "header[class^='ActivityInformationHeader__']")
                )
            )
        except TimeoutException:
            logger.warning("Timeout waiting for detail page to render: %s", detail_url)
            return None

        time.sleep(self.throttle)

        # 결과를 저장할 딕셔너리를 기본값으로 초기화
        result = {
            "activity_title": None,
            "activity_url": None,
            "activity_category": [],
            "start_date": None,
            "end_date": None,
            "activity_img": None,
            "organization_name": None,
            "detail_url": detail_url,
            # --- 추가해야 하는 필드들 ---
            "award_scale": None,
            "benefits": None,
            "additional_benefits": None,
            "target_participants": None,
            "company_type": None,
            "views": None,
        }

        # --- 각 필드 스크래핑 시작 ---

        # 제목 (activity_title): 헤더(<header>) 안의 <h1> 태그에서 텍스트 추출
        try:
            # ActivityInformationHeader__로 시작하는 class의 h1
            title_element = driver.find_element(
                By.CSS_SELECTOR, "header[class^='ActivityInformationHeader__'] h1"
            )
            result["activity_title"] = title_element.text.strip()
        except NoSuchElementException:
            logger.debug("Title not found on %s", detail_url)

        # 홈페이지 URL (activity_url): 'HomepageField' 클래스로 시작하는 <dl> 내부의 <a> 태그에서 href 속성 추출
        try:
            home_anchor = driver.find_element(
                By.CSS_SELECTOR, "dl[class^='HomepageField__'] a"
            )
            result["activity_url"] = home_anchor.get_attribute("href")
        except NoSuchElementException:
            logger.debug("Homepage/activity_url not found on %s", detail_url)

        # 카테고리 (activity_category): 카테고리 칩 목록 내부의 모든 <p> 태그 텍스트를 가져와 '/' 기준으로 분리하고, 하나의 리스트로 만듭니다.
        try:
            category_elements = driver.find_elements(
                By.CSS_SELECTOR, "ul[class^='CategoryChipList__'] p"
            )

            categories = []
            for p_element in category_elements:
                text = p_element.text.strip()
                if text:
                    categories.append(text)  # split 하지 않음!

            result["activity_category"] = categories

        except NoSuchElementException:
            logger.debug("Category not found on %s", detail_url)

        # 접수 시작일 (start_date): 'start-at' 클래스를 가진 <span> 태그의 텍스트를 추출
        try:
            result["start_date"] = driver.find_element(
                By.CSS_SELECTOR, ".start-at + span"
            ).text.strip()
        except NoSuchElementException:
            logger.debug("Start date not found on %s", detail_url)

        # 접수 마감일 (end_date): 'end-at' 클래스를 가진 <span> 태그의 텍스트를 추출
        try:
            result["end_date"] = driver.find_element(
                By.CSS_SELECTOR, ".end-at + span"
            ).text.strip()
        except NoSuchElementException:
            logger.debug("End date not found on %s", detail_url)

        # 대표 이미지 (activity_img): 'card-image' 클래스 <img> 태그의 src 속성을 추출
        try:
            result["activity_img"] = driver.find_element(
                By.CSS_SELECTOR, "img.card-image"
            ).get_attribute("src")
        except NoSuchElementException:
            logger.debug("img.card-image not found, trying fallback selector.")
            try:
                poster_img = driver.find_element(By.CSS_SELECTOR, "div.poster > img")
                result["activity_img"] = poster_img.get_attribute("src")
            except NoSuchElementException:
                logger.debug("Activity image not found on %s", detail_url)

        # --- 추가 항목들 수집 (CSS_SELECTOR는 직접 넣어야 함) ---

        try:
            # 예: 상금 규모
            result["award_scale"] = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > div > article > div.ActivityInfomationField__StyledWrapper-sc-2edfa11d-0.bKwmrS > dl:nth-child(3) > dd",
            ).text.strip()
        except NoSuchElementException:
            pass

        try:
            # 예: 혜택
            result["benefits"] = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > div > article > div.ActivityInfomationField__StyledWrapper-sc-2edfa11d-0.bKwmrS > dl:nth-child(6) > dd",
            ).text.strip()
        except NoSuchElementException:
            pass

        try:
            # 예: 추가 혜택
            result["additional_benefits"] = driver.find_elements(
                By.CSS_SELECTOR,
                "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > div > article > div.ActivityInfomationField__StyledWrapper-sc-2edfa11d-0.bKwmrS > dl:nth-child(8) > dd",
            )
            # 배열 형태일 수 있으므로 join 처리
            result["additional_benefits"] = ", ".join(
                [el.text.strip() for el in result["additional_benefits"]]
            )
        except NoSuchElementException:
            pass

        try:
            # 예: 참가 대상
            result["target_participants"] = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > div > article > div.ActivityInfomationField__StyledWrapper-sc-2edfa11d-0.bKwmrS > dl:nth-child(2) > dd",
            ).text.strip()
        except NoSuchElementException:
            pass

        try:
            # 예: 회사 유형
            result["company_type"] = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > div > article > div.ActivityInfomationField__StyledWrapper-sc-2edfa11d-0.bKwmrS > dl:nth-child(1) > dd",
            ).text.strip()
        except NoSuchElementException:
            pass

        try:
            # 조회수
            result["views"] = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > header > div > span:nth-child(2)",
            ).text.strip()
        except NoSuchElementException:
            pass

        # 주최/주관 (organization_name): 다양한 라벨을 대상으로 텍스트를 추출
        try:
            result["organization_name"] = driver.find_element(
                By.CSS_SELECTOR,
                "#__next > div.id-__StyledWrapper-sc-826dfe1d-0.hLmKRJ > div > main > div > div > section:nth-child(1) > div > article > header > h2",
            ).text.strip()
        except NoSuchElementException:
            pass

        # 🔥 상세 페이지 탭 닫기
        driver.close()

        # 🔥 원래 목록 탭으로 돌아가기
        driver.switch_to.window(driver.window_handles[0])

        return result

    def _extract_organization_name(self, driver) -> Optional[str]:
        """상세 페이지 내 주최/주관 정보를 추출"""
        label_candidates = ["주최", "주관", "주최/주관", "주최/주관/후원"]
        xpaths = [
            "//dt[contains(normalize-space(.), '{label}')]/following-sibling::dd[1]",
            "//p[contains(normalize-space(.), '{label}')]/following-sibling::*[1]",
            "//span[contains(normalize-space(.), '{label}')]/following-sibling::*[1]",
        ]
        for label in label_candidates:
            for xpath in xpaths:
                try:
                    text = driver.find_element(
                        By.XPATH, xpath.format(label=label)
                    ).text.strip()
                    if text:
                        return text
                except NoSuchElementException:
                    continue
        # 일부 상세 페이지에서는 별도의 컴포넌트 class 이름을 사용할 수 있으므로 여분의 시도
        try:
            # class 이름이 HostField__ 로 시작하는 dl에 주최 정보가 포함되는 경우 처리
            dl = driver.find_element(By.CSS_SELECTOR, "dl[class^='HostField__'] dd")
            text = dl.text.strip()
            if text:
                return text
        except NoSuchElementException:
            return None
        return None

    def get_current_page(self) -> int:
        try:
            current_btn = self.driver.find_element(
                By.CSS_SELECTOR, "button.button-page-number.active-page span"
            )
            return int(current_btn.text.strip())
        except Exception:
            return 1

    def click_page_number(self, page_number: int) -> bool:
        try:
            btn = self.driver.find_element(
                By.XPATH,
                f"//button[contains(@class,'button-page-number')]/span[text()='{page_number}']/..",
            )
            btn.click()
            return True
        except Exception:
            return False

    def click_next_arrow(self) -> bool:
        try:
            next_arrow = self.driver.find_element(
                By.CSS_SELECTOR, "button.button-arrow-next:not(.Mui-disabled)"
            )
            next_arrow.click()
            return True
        except Exception:
            return False

    def go_to_next_page(self) -> bool:
        driver = self.driver

        current = self.get_current_page()
        next_page_number = current + 1

        # 1) 같은 블록 내 번호 버튼 존재?
        if self.click_page_number(next_page_number):
            return True

        # 2) 없다 → 오른쪽 화살표 클릭하여 다음 블록으로 이동
        if self.click_next_arrow():
            # 화살표 클릭 후 페이지 번호가 바뀔 때까지 기다림
            time.sleep(1)
            return True

        # 3) 더 이상 이동 불가 → 마지막 페이지
        return False

    def wait_for_list_update(self, prev_first_url: str):
        for _ in range(20):
            try:
                first_url = self.driver.find_element(
                    By.CSS_SELECTOR, "div.list-body a[href^='/activity/']"
                ).get_attribute("href")

                if first_url != prev_first_url:
                    return True
            except:
                pass

            time.sleep(0.2)

        return False

    def crawl_pages_by_click(
        self, max_pages: int = 100, per_page_limit: Optional[int] = None
    ):
        """
        페이지 버튼 클릭 기반 크롤링 (React 기반 페이지 전환 지원)
        Args:
            max_pages (int): 최대 몇 개의 페이지를 크롤링할지
            per_page_limit (Optional[int]): 한 페이지에서 몇 개의 상세만 크롤링할지 (None이면 전체)
        """
        self.start()

        # -----------------------------------------
        # ✅ 최초 페이지 직접 접근 (중요!!)
        # -----------------------------------------
        first_url = f"{self.Newest_Url}1"
        logger.info(f"Opening initial list page: {first_url}")
        self.driver.get(first_url)

        # React 렌더링 기다림
        WebDriverWait(self.driver, self.wait_time).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.list-body"))
        )
        time.sleep(1)

        collected = []

        for _ in range(max_pages):

            # -----------------------------
            # 1) 현재 페이지의 리스트에서 URL 가져오기
            # -----------------------------
            urls = self.fetch_activity_urls()  # URL?page=N 방식 미사용
            if not urls:
                break

            prev_first = urls[0]  # 다음 페이지 로딩 완료 여부 판단용

            # -----------------------------
            # 2) 상세 페이지 크롤링
            # -----------------------------
            limit = per_page_limit or len(urls)

            for url in urls[:limit]:
                details = self.fetch_activity_details(url)
                if details:
                    collected.append(details)

            # -----------------------------
            # 3) 다음 페이지 클릭 (없으면 종료)
            # -----------------------------
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "button.button-page-number")
                )
            )

            if not self.go_to_next_page():
                break

            # -----------------------------
            # 4) 다음 페이지 로딩 안정화 대기
            # -----------------------------
            self.wait_for_list_update(prev_first)

        self.stop()
        return collected


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


def main():
    max_pages = int(os.getenv("LINKAREER_PAGE_LIMIT", "100"))
    per_page_limit_env = os.getenv("LINKAREER_PER_PAGE_LIMIT")
    per_page_limit = int(per_page_limit_env) if per_page_limit_env else None
    headless = os.getenv("LINKAREER_HEADLESS", "true").lower() != "false"

    crawler = LinkareerCrawler(headless=headless)

    # 페이지번호 클릭 기반 크롤링
    records = crawler.crawl_pages_by_click(
        max_pages=max_pages, per_page_limit=per_page_limit
    )

    logger.info("Collected %d contest detail records", len(records))

    # 디버그용 (DB 쓰기 스킵)
    if os.getenv("SKIP_DB_WRITE", "false").lower() == "true":
        print(json.dumps(records[:2], indent=4, ensure_ascii=False))
        return

    # RDS 저장
    persist_contests_to_rds(records)


if __name__ == "__main__":
    main()
