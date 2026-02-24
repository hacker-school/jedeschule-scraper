"""HTML scraping: Niedersachsen, Bremen, Hessen, Rheinland-Pfalz."""

import json
import re
import time
import urllib.parse

from bs4 import BeautifulSoup

from jedeschule_lite.schema import School
from jedeschule_lite.utils import fetch, post


# --- Niedersachsen ---

def scrape_niedersachsen() -> list[School]:
    """Scrape Niedersachsen schools from nibis.de (XSRF + JSON API)."""
    # Step 1: Get XSRF token from the search page
    session_resp = fetch("https://schulen.nibis.de/search/advanced")
    cookies = session_resp.cookies

    xsrf_token = None
    for cookie in session_resp.headers.get("Set-Cookie", "").split(","):
        for part in cookie.split(";"):
            part = part.strip()
            if part.startswith("XSRF-TOKEN="):
                xsrf_token = urllib.parse.unquote(part.split("=", 1)[1])
                break

    if not xsrf_token:
        print("WARNING: Could not extract XSRF token for Niedersachsen")
        return []

    # Step 2: Search for all schools
    search_body = {
        "type": "Advanced",
        "eingabe": None,
        "filters": {
            "classifications": [],
            "lschb": [
                "RLSB Braunschweig", "RLSB Hannover",
                "RLSB Lüneburg", "RLSB Osnabrück",
            ],
            "towns": [], "countys": [], "regions": [],
            "features": [], "bbs_classifications": [],
            "bbs_occupations": [], "bbs_orientations": [],
            "plz": 0, "oeffentlich": "on", "privat": "on",
        },
    }

    search_resp = post(
        "https://schulen.nibis.de/school/search",
        json=search_body,
        headers={
            "X-XSRF-TOKEN": xsrf_token,
            "X-Inertia": "true",
            "Content-Type": "application/json;charset=utf-8",
        },
        cookies=cookies,
    )

    search_data = search_resp.json()
    school_list = search_data.get("props", {}).get("schools", [])
    print(f"  Niedersachsen: found {len(school_list)} schools, fetching details...")

    # Step 3: Fetch details for each school
    schools = []
    for i, s in enumerate(school_list):
        schulnr = s.get("schulnr")
        if not schulnr:
            continue

        try:
            detail_resp = fetch(
                f"https://schulen.nibis.de/school/getInfo/{schulnr}",
                cookies=cookies,
            )
            item = detail_resp.json()
        except Exception:
            continue

        name = " ".join([
            item.get("schulname", ""),
            item.get("namenszuatz", ""),
        ]).strip()

        address_info = (item.get("sdb_adressen") or [{}])[0]
        ort = address_info.get("sdb_ort", {})
        school_type = (item.get("sdb_art") or {}).get("art")
        provider = (item.get("sdb_traeger") or {}).get("name")

        schools.append(School(
            name=name,
            phone=item.get("telefon"),
            fax=item.get("fax"),
            email=item.get("email"),
            website=item.get("homepage"),
            address=address_info.get("strasse"),
            zip=ort.get("plz"),
            city=ort.get("ort"),
            school_type=school_type,
            provider=provider,
            legal_status=(item.get("sdb_traegerschaft") or {}).get("bezeichnung"),
            id=f"NI-{schulnr}",
        ))

        if (i + 1) % 200 == 0:
            print(f"  Niedersachsen: {i + 1}/{len(school_list)} details fetched")
        time.sleep(0.1)

    return schools


# --- Bremen ---

def scrape_bremen() -> list[School]:
    """Scrape Bremen schools from bildung.bremen.de."""
    list_url = "http://www.bildung.bremen.de/detail.php?template=35_schulsuche_stufe2_d"
    list_resp = fetch(list_url)
    soup = BeautifulSoup(list_resp.text, "html.parser")

    links = soup.select(".table_daten_container a")
    print(f"  Bremen: found {len(links)} school links")

    def fix_number(number: str) -> str:
        return "".join(c for c in number if c.isdigit())

    schools = []
    for link in links:
        href = link.get("href", "")
        if "Sid=" not in href:
            continue

        school_id = href.split("de&Sid=", 1)[-1] if "de&Sid=" in href else ""
        detail_url = urllib.parse.urljoin(list_url, href)

        try:
            detail_resp = fetch(detail_url)
        except Exception:
            continue

        detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
        lis = detail_soup.select(".kogis_main_visitenkarte ul li")
        if not lis:
            continue

        collection = {"id": school_id.zfill(3)}

        name_elem = detail_soup.select_one(".main_article h3")
        collection["name"] = name_elem.get_text(strip=True) if name_elem else None

        for li in lis:
            span = li.select_one("span[title]")
            if span:
                key = span.get("title", "")
                value = " ".join(li.stripped_strings)
                collection[key] = value

        if not collection.get("name"):
            continue

        # Parse address
        address_raw = collection.get("Anschrift:", "").strip()
        zip_match = re.findall(r"\d{5}", address_raw)
        zip_code = zip_match[0] if zip_match else None
        address_parts = re.split(r"\d{5}", address_raw)
        address = address_parts[0].strip() if address_parts else None
        city = address_parts[1].strip() if len(address_parts) > 1 else None

        # Parse director
        director = None
        if "Ansprechperson" in collection:
            director = (
                collection["Ansprechperson"]
                .replace("Schulleitung:", "")
                .replace("Vertretung:", ",")
                .split(",")[0]
                .replace("\n", "")
                .strip()
            )

        schools.append(School(
            name=collection["name"].strip(),
            id=f"HB-{collection['id']}",
            address=address,
            zip=zip_code,
            city=city,
            website=(collection.get("Internet") or "").strip() or None,
            email=(collection.get("E-Mail-Adresse") or "").strip(),
            fax=fix_number(collection.get("Telefax", "")),
            phone=fix_number(collection.get("Telefon", "")),
            director=director,
        ))

        time.sleep(0.1)

    return schools


# --- Hessen ---

def scrape_hessen() -> list[School]:
    """Scrape Hessen schools from schul-db.bildung.hessen.de."""
    import requests as req

    base_url = "https://schul-db.bildung.hessen.de/schul_db.html"
    session = req.Session()
    list_resp = session.get(base_url, timeout=30)
    soup = BeautifulSoup(list_resp.text, "html.parser")

    # Get CSRF token and school types
    csrf = soup.select_one('input[name="csrfmiddlewaretoken"]')
    csrf_token = csrf["value"] if csrf else ""

    school_type_options = soup.select('#id_school_type option')
    school_types = [opt["value"] for opt in school_type_options if opt.get("value")]

    # Collect all detail page URLs by searching per school type
    detail_urls = set()
    for st in school_types:
        form_data = {
            "school_name": "",
            "school_town": "",
            "school_zip": "",
            "school_number": "",
            "csrfmiddlewaretoken": csrf_token,
            "school_type": st,
            "submit_hesse": "Hessische+Schule+suchen+...",
        }
        try:
            search_resp = session.post(base_url, data=form_data, timeout=30)
            search_soup = BeautifulSoup(search_resp.text, "html.parser")
            for a in search_soup.select("tbody tr td a[href]"):
                detail_urls.add(a["href"])
        except Exception:
            continue

    print(f"  Hessen: found {len(detail_urls)} school detail pages")

    def extract_coords_from_osm_url(url: str) -> tuple:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        if "marker" in qs and qs["marker"]:
            try:
                lat_str, lon_str = qs["marker"][0].split(",", 1)
                return float(lat_str), float(lon_str)
            except (ValueError, IndexError):
                pass
        return None, None

    schools = []
    for i, detail_url in enumerate(detail_urls):
        try:
            detail_resp = session.get(detail_url, timeout=30)
        except Exception:
            continue

        detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
        pre_elems = detail_soup.select("pre")
        if not pre_elems:
            continue

        text_nodes = [pre.get_text() for pre in pre_elems]
        address_lines = text_nodes[0].split("\n")
        if len(address_lines) < 4:
            continue

        zip_city_match = re.search(r"(\d+) (.+)", address_lines[3])
        if not zip_city_match:
            continue

        school = {
            "name": address_lines[1].strip(),
            "address": address_lines[2].strip(),
            "city": zip_city_match.group(2).strip(),
            "zip": zip_city_match.group(1),
        }

        # Fax
        for text in text_nodes:
            if "Fax: " in text:
                fax_line = [l for l in text.split("\n") if "Fax: " in l]
                if fax_line:
                    school["fax"] = fax_line[0].replace("Fax: ", "").strip()

        # Phone and website from links
        for a in detail_soup.select("pre a[href]"):
            href = a.get("href", "")
            if "tel:" in href:
                school["phone"] = href.replace("tel:", "")
            elif "http" in href:
                school["website"] = href

        # School type
        type_elem = detail_soup.select_one('main .col-md-9.col-lg-9')
        if type_elem:
            school["school_type"] = type_elem.get_text(strip=True)

        # School ID from URL
        school["id"] = detail_url.split("=")[-1]

        # Coordinates from OSM iframe
        iframe = detail_soup.select_one('iframe[src*="openstreetmap.org"]')
        lat, lon = None, None
        if iframe:
            lat, lon = extract_coords_from_osm_url(iframe.get("src", ""))
            # Filter placeholder coordinates
            if lat == -1.0 and lon == -1.0:
                lat, lon = None, None

        schools.append(School(
            name=school.get("name"),
            phone=school.get("phone"),
            fax=school.get("fax"),
            website=school.get("website"),
            address=school.get("address"),
            city=school.get("city"),
            zip=school.get("zip"),
            school_type=school.get("school_type"),
            id=f"HE-{school.get('id')}",
            latitude=lat,
            longitude=lon,
        ))

        if (i + 1) % 100 == 0:
            print(f"  Hessen: {i + 1}/{len(detail_urls)} details fetched")
        time.sleep(0.1)

    return schools


# --- Rheinland-Pfalz ---

_RP_SCHOOL_TYPES = {
    'BEA': 'BEA',
    'BBS': 'Berufsbildende Schule',
    'FWS': 'Freie Waldorfschule',
    'GHS': 'Grund- und Hauptschule (org. verbunden)',
    'GRS+': 'Grund- und Realschule plus (org. verbunden)',
    'GS': 'Grundschule',
    'GY': 'Gymnasium',
    'HS': 'Hauptschule',
    'IGS': 'Integrierte Gesamtschule',
    'Koll': 'Kolleg',
    'Koll/AGY': 'Kolleg und Abendgymnasium (org.verbunden)',
    'RS': 'Realschule',
    'RS+': 'Realschule plus',
    'RS+FOS': 'Realschule plus mit Fachoberschule',
    'StudSem': 'Studienseminar',
}


def scrape_rheinland_pfalz() -> list[School]:
    """Scrape RLP schools from bildung.rlp.de."""
    # Use the GeoPortal JSON API instead of HTML crawling for reliability
    # This provides the same data without needing CrawlSpider pagination
    url = (
        "https://www.geoportal.rlp.de/spatial-objects/350"
        "/collections/schulstandorte/items?f=json&limit=4000"
    )

    try:
        data = fetch(url, timeout=60).json()
        return _parse_rlp_geoportal(data)
    except Exception as e:
        print(f"  RLP GeoPortal failed ({e}), falling back to HTML scraping...")
        return _scrape_rlp_html()


def _parse_rlp_geoportal(data: dict) -> list[School]:
    """Parse RLP schools from GeoPortal GeoJSON response."""
    from jedeschule_lite.utils import parse_geojson_features

    features = parse_geojson_features(data)
    schools = []

    for f in features:
        name = f.get("schulname") or f.get("name")
        school_id = f.get("schulnummer") or f.get("OBJECTID")

        schools.append(School(
            name=name,
            id=f"RP-{school_id}",
            address=f.get("strasse"),
            zip=f.get("plz"),
            city=f.get("ort"),
            school_type=f.get("schulart") or f.get("schulform"),
            phone=f.get("telefon"),
            fax=f.get("telefax"),
            email=f.get("email"),
            website=f.get("internet") or f.get("homepage"),
            provider=f.get("traeger"),
            latitude=f.get("lat"),
            longitude=f.get("lon"),
        ))

    return schools


def _scrape_rlp_html() -> list[School]:
    """Fallback: scrape RLP schools from bildung.rlp.de HTML."""
    import requests as req

    session = req.Session()
    base_url = "https://bildung.rlp.de/schulen"
    list_resp = session.get(base_url, timeout=30)
    soup = BeautifulSoup(list_resp.text, "html.parser")

    # Find all school detail links
    detail_links = set()
    for a in soup.select('a[href*="einzelanzeige"]'):
        detail_links.add(a["href"])

    print(f"  RLP HTML: found {len(detail_links)} school links")

    schools = []
    for i, link in enumerate(detail_links):
        try:
            detail_resp = session.get(link, timeout=30)
        except Exception:
            continue

        detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
        container = detail_soup.select_one(".rlp-schooldatabase-detail")
        if not container:
            continue

        h1 = container.select_one("h1")
        name = h1.get_text(strip=True) if h1 else None

        item = {}
        for row in container.select("tr"):
            cells = row.select("td")
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).replace(":", "")
                value_parts = [t.strip() for t in cells[1].stripped_strings]
                item[key] = value_parts[0] if len(value_parts) == 1 else value_parts

        school_id = item.get("Schulnummer", "")

        # Coordinates from OSM link
        lat, lon = None, None
        osm_link = container.select_one('a[href*="openstreetmap"]')
        if osm_link:
            parts = osm_link["href"].rstrip("/").split("/")
            try:
                lat, lon = float(parts[-2]), float(parts[-1])
            except (ValueError, IndexError):
                pass

        # Address
        address_info = item.get("Anschrift", [])
        if isinstance(address_info, list) and len(address_info) >= 2:
            address = address_info[1] if len(address_info) > 1 else None
            last = address_info[-1]
            zip_city = last.split(" ", 1)
            zip_code = zip_city[0] if zip_city else None
            city = zip_city[1] if len(zip_city) > 1 else None
        else:
            address, zip_code, city = None, None, None

        # School type from Kurzbezeichnung
        school_type = None
        kurz = item.get("Kurzbezeichnung")
        if kurz:
            first_part = kurz.split(" ")[0] if isinstance(kurz, str) else ""
            if first_part.startswith("SF"):
                school_type = "Förderschule"
            else:
                school_type = _RP_SCHOOL_TYPES.get(first_part)

        email = (item.get("E-Mail") or "").replace("(at)", "@")

        schools.append(School(
            name=name,
            id=f"RP-{school_id}",
            address=address,
            city=city,
            zip=zip_code,
            latitude=lat,
            longitude=lon,
            website=item.get("Internet"),
            email=email,
            provider=item.get("Träger"),
            fax=item.get("Telefax"),
            phone=item.get("Telefon"),
            school_type=school_type,
        ))

        if (i + 1) % 100 == 0:
            print(f"  RLP HTML: {i + 1}/{len(detail_links)} details fetched")
        time.sleep(0.1)

    return schools
