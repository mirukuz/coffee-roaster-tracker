import re
import time
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Global constants
# ---------------------------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
REQUEST_DELAY = 0.5

COUNTRIES = [
    "Ethiopia", "Kenya", "Colombia", "Brazil", "Guatemala", "Peru",
    "Rwanda", "Burundi", "Panama", "Honduras", "Costa Rica", "Yemen",
    "Tanzania", "El Salvador", "Bolivia", "India", "Indonesia",
    "Papua New Guinea", "Myanmar", "Nicaragua", "Mexico", "Ecuador",
    "Laos", "Thailand", "Uganda", "Malawi", "Zambia", "Zimbabwe",
    "Cameroon", "Congo", "DRC", "Philippines", "Timor-Leste",
    "Vietnam", "Haiti", "Dominican Republic", "Jamaica",
]

PROCESSES = [
    "Anaerobic", "Carbonic Maceration", "Extended Fermentation",
    "Double Fermentation", "Natural", "Washed", "Honey",
    "Pulped Natural", "Wet Hulled", "Semi-Washed", "Sun Dried",
    "Fermented", "Experimental",
]

ROASTERS = [
    {
        "name": "Market Lane",
        "platform": "shopify",
        "domain": "marketlane.com.au",
        "api_path": "/products.json",
        "allow_types": ["Coffee", "coffee", "Whole Bean", "Filter Coffee", "Espresso Coffee"],
    },
    {
        "name": "Small Batch",
        "platform": "woocommerce",
        "url": "https://www.smallbatch.com.au/shop/",
    },
    {
        "name": "Proud Mary",
        "platform": "shopify",
        "domain": "proudmarycoffee.com.au",
        "api_path": "/collections/coffee/products.json",
    },
    {
        "name": "Seven Seeds",
        "platform": "shopify",
        "domain": "sevenseeds.com.au",
        "api_path": "/collections/coffee/products.json",
    },
    {
        "name": "Common Folk",
        "platform": "shopify",
        "domain": "commonfolkcoffee.com.au",
        "api_path": "/collections/single-origin/products.json",
    },
    {
        "name": "Ona Coffee",
        "platform": "shopify",
        "domain": "onacoffee.com.au",
        "api_path": "/collections/all/products.json",
        "allow_types": ["Coffee", "coffee"],
    },
    {
        "name": "Padre Coffee",
        "platform": "shopify",
        "domain": "padrecoffee.com.au",
        "api_path": "/collections/coffee/products.json",
    },
]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

RETRY_STATUSES = {429, 500, 502, 503, 504}


def fetch_url(url):
    """Fetch a URL with up to 3 retries on transient errors.

    Returns a Response object on success, or None on failure.
    Always sleeps REQUEST_DELAY seconds after the call (successful or not).
    """
    short_url = url if len(url) <= 80 else url[:77] + "..."
    response = None
    for attempt in range(3):
        try:
            t0 = time.time()
            print(f"  → GET {short_url}", end="", flush=True)
            response = requests.get(url, headers=HEADERS, timeout=15)
            elapsed = time.time() - t0
            print(f"  [{response.status_code}] {elapsed:.1f}s", flush=True)
            if response.status_code not in RETRY_STATUSES:
                break
            # Retryable status — wait before next attempt
            if attempt < 2:
                print(f"  ⚠ 状态 {response.status_code}，等待 2s 后重试...", flush=True)
                time.sleep(2)
        except requests.exceptions.Timeout:
            print(f"  [超时] >15s", flush=True)
            if attempt < 2:
                time.sleep(2)
            else:
                response = None
        except requests.RequestException as e:
            print(f"  [错误] {e}", flush=True)
            if attempt < 2:
                time.sleep(2)
            else:
                response = None
    time.sleep(REQUEST_DELAY)
    if response is not None and response.status_code not in RETRY_STATUSES:
        return response
    return None


# ---------------------------------------------------------------------------
# Shopify scraper
# ---------------------------------------------------------------------------

def fetch_shopify_products(roaster):
    """Fetch products from a Shopify store (single request, limit=250)."""
    name = roaster["name"]
    domain = roaster["domain"]
    api_path = roaster["api_path"]
    allow_types = roaster.get("allow_types")

    url = f"https://{domain}{api_path}?limit=250"
    response = fetch_url(url)
    if response is None:
        print(f"❌ {name}: 请求失败")
        return []

    try:
        data = response.json()
    except ValueError:
        print(f"❌ {name}: 响应解析失败")
        return []

    page_products = data.get("products", [])

    if len(page_products) == 0:
        print(f"⚠️ {name}: 0个产品（collection slug 可能已变更）")
        return []

    # Apply allow_types filter if specified
    if allow_types:
        filtered = [
            p for p in page_products
            if p.get("product_type", "") == "" or p.get("product_type", "") in allow_types
        ]
        print(f"  ✦ 共 {len(page_products)} 个产品，过滤后保留 {len(filtered)} 个咖啡豆", flush=True)
        products = filtered
    else:
        print(f"  ✦ 共 {len(page_products)} 个产品", flush=True)
        products = page_products

    # Build normalised product dicts
    result = []
    for p in products:
        tags = p.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",")]
        title = p.get("title", "")
        body_html = p.get("body_html", "") or ""
        variants = p.get("variants", [])

        origin = extract_origin(tags, title, body_html)
        process = extract_process(tags, title, body_html)
        price = extract_price(variants)
        handle = p.get("handle", "")
        url = f"https://{domain}/products/{handle}"

        result.append({
            "name": title,
            "origin": origin,
            "process": process,
            "price": price,
            "url": url,
        })

    return result


# ---------------------------------------------------------------------------
# WooCommerce scraper
# ---------------------------------------------------------------------------

def fetch_woocommerce_products(roaster):
    """Fetch products from a WooCommerce shop page."""
    name = roaster["name"]
    shop_url = roaster["url"]

    response = fetch_url(shop_url)
    if response is None:
        print(f"⚠️ {name}: 无法访问商店页面")
        return []

    soup = BeautifulSoup(response.content, "lxml")

    # Find product containers
    product_items = soup.select("ul.products li.product")
    if not product_items:
        product_items = soup.select(".type-product")

    if not product_items:
        print(f"⚠️ {name}: 未找到产品列表")
        return []

    results = []
    print(f"  ✦ 找到 {len(product_items)} 个产品，逐个访问详情页...", flush=True)
    for idx, item in enumerate(product_items, 1):
        # Name
        name_el = item.select_one(".woocommerce-loop-product__title")
        if name_el is None:
            name_el = item.select_one("h2")
        product_name = name_el.get_text(strip=True) if name_el else "—"

        # Price
        price_el = item.select_one(".price .woocommerce-Price-amount bdi")
        product_price = price_el.get_text(strip=True) if price_el else "—"

        # Product URL
        link_el = item.select_one("a.woocommerce-LoopProduct-link")
        product_url = link_el["href"] if link_el and link_el.get("href") else ""

        print(f"  [{idx}/{len(product_items)}] {product_name}", flush=True)

        # Fetch detail page for origin/process extraction
        origin = "—"
        process = "—"
        if product_url:
            detail_resp = fetch_url(product_url)
            if detail_resp is not None:
                detail_soup = BeautifulSoup(detail_resp.content, "lxml")
                body_html = ""
                for selector in [
                    ".woocommerce-product-details__short-description",
                    ".entry-content",
                    "article",
                ]:
                    el = detail_soup.select_one(selector)
                    if el:
                        body_html = str(el)
                        break
                origin = extract_origin([], product_name, body_html)
                process = extract_process([], product_name, body_html)

        results.append({
            "name": product_name,
            "origin": origin,
            "process": process,
            "price": product_price,
            "url": product_url,
        })

    return results


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _plain_text_prefix(body_html, max_chars=1000):
    """Return plain text from HTML, truncated to max_chars."""
    if not body_html:
        return ""
    soup = BeautifulSoup(body_html, "lxml")
    return soup.get_text(" ", strip=True)[:max_chars]


def extract_origin(tags, title, body_html):
    """Extract coffee origin country/countries from available metadata."""
    found = []
    seen = set()

    def _add(country):
        key = country.lower()
        if key not in seen:
            seen.add(key)
            found.append(country)

    # Build a lookup map: lowercase -> original case from COUNTRIES list
    country_map = {c.lower(): c for c in COUNTRIES}

    # 1. Tags: look for explicit "COUNTRY: ..." label
    tag_country_re = re.compile(r"COUNTRY[:\s]+(\S[^,]+)", re.IGNORECASE)
    for tag in tags:
        m = tag_country_re.search(tag)
        if m:
            _add(m.group(1).strip())

    # 2. Tags: match against COUNTRIES list
    for tag in tags:
        tag_lower = tag.lower()
        for c_lower, c_orig in country_map.items():
            if c_lower in tag_lower:
                _add(c_orig)

    # 3. Title
    title_lower = title.lower()
    for c_lower, c_orig in country_map.items():
        if c_lower in title_lower:
            _add(c_orig)

    # 4. Body HTML (plain text, first 1000 chars)
    body_text = _plain_text_prefix(body_html)
    body_lower = body_text.lower()
    for c_lower, c_orig in country_map.items():
        if c_lower in body_lower:
            _add(c_orig)

    return " / ".join(found) if found else "—"


def extract_process(tags, title, body_html):
    """Extract processing method from available metadata."""
    body_text = _plain_text_prefix(body_html)

    sources = [
        (" ".join(tags), True),   # tags joined
        (title, True),
        (body_text, True),
    ]

    for process in PROCESSES:
        pattern = re.compile(re.escape(process), re.IGNORECASE)
        for text, _ in sources:
            if pattern.search(text):
                return process

    return "—"


def extract_price(variants):
    """Return formatted price string from the cheapest variant."""
    if not variants:
        return "—"

    cheapest = None
    cheapest_price = None

    for v in variants:
        try:
            price_val = float(v.get("price", 0) or 0)
        except (ValueError, TypeError):
            price_val = 0.0
        if cheapest_price is None or price_val < cheapest_price:
            cheapest_price = price_val
            cheapest = v

    if cheapest is None:
        return "—"

    price_str = f"${cheapest_price:.2f}"
    option1 = (cheapest.get("option1") or "").strip()
    if option1:
        return f"{price_str} / {option1}"
    return price_str


# ---------------------------------------------------------------------------
# Markdown report generator
# ---------------------------------------------------------------------------

def generate_markdown(results):
    """Generate a Markdown report from a list of roaster result dicts."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# 墨尔本咖啡烘焙商最新咖啡豆报告",
        f"更新时间：{now}",
        "",
        "---",
        "",
    ]

    total_products = 0
    origin_found = 0
    process_found = 0

    for entry in results:
        roaster_name = entry["roaster"]
        products = entry["products"]
        n = len(products)
        total_products += n

        lines.append(f"## {roaster_name}（{n}款）")
        lines.append("")
        lines.append("| 咖啡豆名称 | 产地 | 处理法 | 价格 | 链接 |")
        lines.append("|-----------|------|--------|------|------|")

        for p in products:
            name = p.get("name", "—").replace("|", "\\|")
            origin = p.get("origin", "—").replace("|", "\\|")
            process = p.get("process", "—").replace("|", "\\|")
            price = p.get("price", "—").replace("|", "\\|")
            url = p.get("url", "")

            if origin != "—":
                origin_found += 1
            if process != "—":
                process_found += 1

            lines.append(f"| {name} | {origin} | {process} | {price} | [购买]({url}) |")

        lines.append("")
        lines.append("---")
        lines.append("")

    # Data quality footer
    if total_products > 0:
        origin_pct = round(origin_found / total_products * 100)
        process_pct = round(process_found / total_products * 100)
    else:
        origin_pct = 0
        process_pct = 0

    lines.append(
        f"*数据质量：产地提取率 {origin_found}/{total_products} ({origin_pct}%) "
        f"| 处理法提取率 {process_found}/{total_products} ({process_pct}%)*"
    )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    os.makedirs("output", exist_ok=True)

    total = len(ROASTERS)
    all_results = []

    for i, roaster in enumerate(ROASTERS, start=1):
        name = roaster["name"]
        platform = roaster["platform"]
        print(f"[{i}/{total}] 正在抓取 {name}...")

        if platform == "shopify":
            products = fetch_shopify_products(roaster)
        elif platform == "woocommerce":
            products = fetch_woocommerce_products(roaster)
        else:
            print(f"⚠️ {name}: 未知平台 {platform}")
            products = []

        print(f"✓ {name}: {len(products)}款咖啡豆")
        all_results.append({"roaster": name, "products": products})

    markdown = generate_markdown(all_results)

    date_str = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join("output", f"coffee_report_{date_str}.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown)

    print()
    print(f"报告已生成：{os.path.abspath(output_path)}")


if __name__ == "__main__":
    main()
