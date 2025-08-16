from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import requests
from bs4 import BeautifulSoup
import re
import os
import logging
from sqlalchemy import create_engine, Column, Integer, String, JSON
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

# ---------------- Load .env ----------------
load_dotenv()  # looks for a .env file in project root

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
PERSIST_DB = os.getenv("PERSIST_DB", "false").lower() == "true"
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
SECRET_KEY = os.getenv("SECRET_KEY", "fallback-secret")

# ---------------- Logging ----------------
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger("shopify_insights")

# ---------------- Database Setup ----------------
Base = declarative_base()
engine = None
SessionLocal = None

if PERSIST_DB:
    engine = create_engine(DATABASE_URL, echo=False, future=True)
    SessionLocal = sessionmaker(bind=engine)

    class BrandRecord(Base):
        __tablename__ = "brands"
        id = Column(Integer, primary_key=True)
        url = Column(String(512), unique=True, nullable=False)
        raw = Column(JSON)

    Base.metadata.create_all(bind=engine)

# ---------------- Pydantic Models ----------------
class Product(BaseModel):
    id: Optional[int]
    title: Optional[str]
    handle: Optional[str]
    variants: Optional[List[Dict[str, Any]]]
    images: Optional[List[str]]

class ContactInfo(BaseModel):
    emails: List[str] = []
    phones: List[str] = []
    addresses: List[str] = []

class BrandContext(BaseModel):
    website_url: HttpUrl
    store_title: Optional[str]
    about_text: Optional[str]
    products: List[Product] = []
    hero_products: List[Product] = []
    policies: Dict[str, Optional[str]] = {}
    faqs: List[Dict[str, str]] = []
    social_handles: Dict[str, str] = {}
    contact: ContactInfo = ContactInfo()
    important_links: Dict[str, str] = {}
    metadata: Dict[str, Any] = {}

# ---------------- FastAPI ----------------
app = FastAPI(title="Shopify Insights Fetcher")

headers = {
    "User-Agent": "Mozilla/5.0 (compatible; ShopifyInsightsFetcher/1.0; +https://example.com)"
}

# ---------------- Helper Functions ----------------
def safe_get(url: str, timeout: int = 10) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r
        else:
            logger.debug(f"GET {url} returned status {r.status_code}")
            return None
    except requests.RequestException as e:
        logger.debug(f"Request failed for {url}: {e}")
        return None

def fetch_products_json(base_url: str) -> List[Product]:
    products: List[Product] = []
    candidate = base_url.rstrip("/") + "/products.json"
    r = safe_get(candidate)
    if r:
        try:
            data = r.json()
            raw_products = data.get("products") or data.get("items") or []
            for p in raw_products:
                prod = Product(
                    id=p.get("id"),
                    title=p.get("title"),
                    handle=p.get("handle"),
                    variants=p.get("variants"),
                    images=[img.get("src") for img in p.get("images", [])] if p.get("images") else []
                )
                products.append(prod)
        except ValueError:
            logger.debug("products.json returned non-json")
    else:
        logger.debug("/products.json not available or returned non-200")
    return products

def extract_links_and_text(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title and soup.title.string else None
    links = {a.get_text(strip=True): a.get('href') for a in soup.find_all('a', href=True)}
    product_cards = []
    selectors = ['.product-card', '.product', '.featured-product', '.grid-item', '.product-grid-item']
    for sel in selectors:
        for card in soup.select(sel):
            a = card.find('a', href=True)
            if a:
                href = a['href']
                text = a.get_text(strip=True)
                product_cards.append({'href': href, 'text': text})
    for tag in soup.find_all(attrs={"data-product-handle": True}):
        product_cards.append({'href': tag.get('data-product-handle'), 'text': tag.get_text(strip=True)})
    emails = set(re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", html))
    phones = set(re.findall(r"\+?\d[\d\-\s()]{6,}\d", html))
    about_text = None
    about_candidates = soup.find_all(lambda tag: tag.name in ['p', 'div'] and ('about' in (tag.get('id') or '').lower() or 'about' in ' '.join(tag.get('class') or []).lower()))
    if about_candidates:
        about_text = ' '.join([c.get_text(strip=True) for c in about_candidates[:3]])
    else:
        desc = soup.find('meta', attrs={'name': 'description'})
        if desc and desc.get('content'):
            about_text = desc.get('content')
    social = {}
    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'instagram.com' in href:
            social['instagram'] = href
        elif 'facebook.com' in href:
            social['facebook'] = href
        elif 'twitter.com' in href or 'x.com' in href:
            social['twitter'] = href
        elif 'tiktok.com' in href:
            social['tiktok'] = href
        elif 'youtube.com' in href:
            social['youtube'] = href
    return {
        'title': title,
        'links': links,
        'product_cards': product_cards,
        'emails': list(emails),
        'phones': list(phones),
        'about_text': about_text,
        'social': social
    }

def find_policy_links(base_url: str, html_links: Dict[str, str]) -> Dict[str, Optional[str]]:
    policies = {'privacy_policy': None,'refund_policy': None,'terms_of_service': None}
    base = base_url.rstrip('/')
    patterns = {
        'privacy_policy': ['/policies/privacy-policy', '/policies/privacy-policy/'],
        'refund_policy': ['/policies/refund-policy', '/policies/refund-policy/','/policies/returns','/policies/return-policy'],
        'terms_of_service': ['/policies/terms-of-service','/policies/terms-of-service/']
    }
    for name, pats in patterns.items():
        for k, v in html_links.items():
            if not v:
                continue
            for pat in pats:
                if pat in v:
                    policies[name] = requests.compat.urljoin(base, v)
        if not policies[name]:
            for pat in pats:
                candidate = base + pat
                r = safe_get(candidate)
                if r:
                    policies[name] = candidate
                    break
    return policies

def try_fetch_faqs(soup: BeautifulSoup) -> List[Dict[str,str]]:
    faqs: List[Dict[str,str]] = []
    for details in soup.find_all('details'):
        summary = details.find('summary')
        if summary:
            q = summary.get_text(strip=True)
            a = details.get_text(strip=True).replace(q, '').strip()
            faqs.append({'q': q, 'a': a})
    if not faqs:
        for li in soup.select('.faq, .faqs, .accordion, .question'):
            q_tag = li.find(class_=re.compile('question|q\b', re.I))
            a_tag = li.find(class_=re.compile('answer|a\b', re.I))
            if q_tag and a_tag:
                faqs.append({'q': q_tag.get_text(strip=True), 'a': a_tag.get_text(strip=True)})
    return faqs

def hero_product_matches(product_cards: List[Dict[str,str]], products: List[Product], base_url: str) -> List[Product]:
    heroes: List[Product] = []
    if not products:
        return heroes
    handles_to_product = { (p.handle or '').lower(): p for p in products }
    for card in product_cards:
        href = card.get('href') or ''
        text = (card.get('text') or '').lower()
        m = re.search(r'/products/([^/?#]+)', href)
        handle = None
        if m:
            handle = m.group(1).lower()
        if handle and handle in handles_to_product:
            heroes.append(handles_to_product[handle])
            continue
        for p in products:
            if p.title and p.title.lower() in text:
                heroes.append(p)
                break
    unique = []
    seen = set()
    for h in heroes:
        key = h.handle or h.title or str(h.id)
        if key not in seen:
            unique.append(h)
            seen.add(key)
    return unique

# ---------------- API Routes ----------------
@app.post('/fetch', response_model=BrandContext)
def fetch_insights(payload: Dict[str, str]):
    website_url = payload.get('website_url')
    if not website_url:
        raise HTTPException(status_code=400, detail="website_url is required")
    if not website_url.startswith('http'):
        website_url = 'https://' + website_url
    r = safe_get(website_url)
    if not r:
        if website_url.startswith('https://') and 'www.' not in website_url:
            alt = website_url.replace('https://', 'https://www.')
            r = safe_get(alt)
            if r:
                website_url = alt
    if not r:
        raise HTTPException(status_code=401, detail=f"Website not found or unreachable: {website_url}")
    html = r.text
    parsed = extract_links_and_text(html)
    products = fetch_products_json(website_url)
    hero_candidates = parsed.get('product_cards', [])
    hero_products = hero_product_matches(hero_candidates, products, website_url)
    policies = find_policy_links(website_url, parsed.get('links', {}))
    soup = BeautifulSoup(html, 'html.parser')
    faqs = try_fetch_faqs(soup)
    important = {}
    for text, href in parsed.get('links', {}).items():
        if not href:
            continue
        ltext = (text or '').lower()
        href_l = href.lower()
        if 'track' in ltext or 'track' in href_l:
            important['order_tracking'] = requests.compat.urljoin(website_url, href)
        if 'contact' in ltext or 'contact' in href_l:
            important['contact'] = requests.compat.urljoin(website_url, href)
        if 'blog' in ltext or '/blogs' in href_l:
            important['blog'] = requests.compat.urljoin(website_url, href)
    contact = ContactInfo(
        emails=parsed.get('emails', []),
        phones=parsed.get('phones', []),
        addresses=[]
    )
    metadata = {'found_products_count': len(products),'found_hero_count': len(hero_products)}
    result = BrandContext(
        website_url=website_url,
        store_title=parsed.get('title'),
        about_text=parsed.get('about_text'),
        products=products,
        hero_products=hero_products,
        policies=policies,
        faqs=faqs,
        social_handles=parsed.get('social', {}),
        contact=contact,
        important_links=important,
        metadata=metadata
    )
    if PERSIST_DB and SessionLocal is not None:
        try:
            session = SessionLocal()
            rec = BrandRecord(url=website_url, raw=result.dict())
            session.add(rec)
            session.commit()
            session.close()
        except Exception as e:
            logger.error(f"DB persist error: {e}")
    return result

@app.get('/')
def root():
    return {"status": "ok", "note": "POST /fetch with {\"website_url\": \"https://example.com\"}"}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run('shopify_insights_fastapi:app', host='0.0.0.0', port=8000, reload=True)
