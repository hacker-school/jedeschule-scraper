"""GeoJSON/WFS API scrapers: Berlin, Brandenburg, Hamburg, Saarland."""

from jedeschule_lite.schema import School
from jedeschule_lite.utils import fetch, parse_geojson_features


def scrape_berlin() -> list[School]:
    """Scrape Berlin schools from GDI WFS GeoJSON endpoint."""
    url = (
        "https://gdi.berlin.de/services/wfs/schulen?"
        "SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature&srsname=EPSG:4326"
        "&typename=fis:schulen&outputFormat=application/json"
    )
    data = fetch(url).json()
    features = parse_geojson_features(data)

    return [
        School(
            name=f.get("schulname"),
            id=f"BE-{f.get('bsn')}",
            address=f"{f.get('strasse', '')} {f.get('hausnr', '')}".strip(),
            zip=f.get("plz"),
            city="Berlin",
            website=f.get("internet"),
            email=f.get("email"),
            school_type=f.get("schulart"),
            legal_status=f.get("traeger"),
            fax=f.get("fax"),
            phone=f.get("telefon"),
            latitude=f.get("lat"),
            longitude=f.get("lon"),
        )
        for f in features
    ]


def scrape_brandenburg() -> list[School]:
    """Scrape Brandenburg schools from Schullandschaft WFS GeoJSON."""
    url = (
        "https://schullandschaft.brandenburg.de/edugis/wfs/schulen?"
        "SERVICE=WFS&VERSION=1.1.0&REQUEST=GetFeature"
        "&typename=ms:Schul_Standorte"
        "&srsname=epsg:4326&outputFormat=application/json"
    )
    data = fetch(url).json()
    features = parse_geojson_features(data)

    return [
        School(
            name=f.get("schulname"),
            id=f"BB-{f.get('schul_nr')}",
            address=f.get("strasse_hausnr"),
            zip=f.get("plz"),
            city=f.get("ort"),
            website=f.get("homepage"),
            email=f.get("dienst_email"),
            school_type=f.get("schulform"),
            fax=f.get("faxnummer"),
            phone=f.get("telefonnummer"),
            provider=f.get("schulamtname"),
            longitude=f.get("lon"),
            latitude=f.get("lat"),
        )
        for f in features
    ]


def scrape_hamburg() -> list[School]:
    """Scrape Hamburg schools from two GeoJSON API endpoints."""
    urls = [
        "https://api.hamburg.de/datasets/v1/schulen/collections/staatliche_schulen/items?limit=1000",
        "https://api.hamburg.de/datasets/v1/schulen/collections/nicht_staatliche_schulen/items?limit=1000",
    ]
    headers = {"Accept": "application/geo+json, application/json, */*"}

    schools = []
    for url in urls:
        data = fetch(url, headers=headers).json()
        for f in parse_geojson_features(data):
            city_parts = (f.get("adresse_ort") or "").split()
            zip_code = city_parts[0] if city_parts else None
            city = " ".join(city_parts[1:]) if len(city_parts) > 1 else None

            schools.append(School(
                name=f.get("schulname"),
                id=f"HH-{f.get('schul_id')}",
                address=f.get("adresse_strasse_hausnr"),
                zip=zip_code,
                city=city,
                website=f.get("schul_homepage"),
                email=f.get("schul_email"),
                school_type=f.get("schulform"),
                fax=f.get("fax"),
                phone=f.get("schul_telefonnr"),
                director=f.get("name_schulleiter"),
                latitude=f.get("lat"),
                longitude=f.get("lon"),
            ))

    return schools


def scrape_saarland() -> list[School]:
    """Scrape Saarland schools from GeoPortal GeoJSON endpoint."""
    url = (
        "https://geoportal.saarland.de/spatial-objects/257"
        "/collections/Staatliche_Dienste:Schulen_SL/items"
        "?f=json&limit=2500"
    )
    data = fetch(url).json()
    features = parse_geojson_features(data)

    return [
        School(
            name=f.get("Bezeichnung"),
            id=f"SL-{f.get('OBJECTID')}",
            address=(f.get("Straße") or "").strip(),
            zip=f.get("PLZ"),
            city=f.get("Ort"),
            school_type=f.get("Schulform"),
            phone=f.get("Telefon"),
            fax=f.get("Fax"),
            website=f.get("Homepage"),
            latitude=f.get("lat"),
            longitude=f.get("lon"),
        )
        for f in features
    ]
