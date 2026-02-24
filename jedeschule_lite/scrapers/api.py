"""REST/JSON API scrapers: Baden-Wuerttemberg, Sachsen, Sachsen-Anhalt."""

import re

from jedeschule_lite.schema import School
from jedeschule_lite.utils import fetch

# --- Baden-Wuerttemberg ---

_DISCH_RE = re.compile(r'@(\d{8})\.schule\.bwl\.de', re.IGNORECASE)


def _extract_disch(email: str | None) -> str | None:
    """Extract 8-digit DISCH from BW school email address."""
    if not email:
        return None
    match = _DISCH_RE.search(email.strip())
    return match.group(1) if match else None


def scrape_baden_wuerttemberg() -> list[School]:
    """Scrape BW schools from Kultus GIS WFS GeoJSON endpoint."""
    url = (
        "https://gis.kultus-bw.de/geoserver/us-govserv/ows?"
        "service=WFS&request=GetFeature"
        "&typeNames=us-govserv%3AGovernmentalService"
        "&outputFormat=application%2Fjson"
    )
    data = fetch(url, timeout=60).json()
    schools = []

    for feature in data.get("features", []):
        uuid = feature.get("id")
        props = feature["properties"]

        # Coordinates (BW returns [lat, lon] — non-standard!)
        service_loc = props.get("serviceLocation", {})
        geom = service_loc.get("serviceLocationByGeometry", {})
        coords = geom.get("coordinates")
        lat = coords[0] if coords and len(coords) >= 2 else None
        lon = coords[1] if coords and len(coords) >= 2 else None

        # Contact and address
        contact = props.get("pointOfContact", {}).get("Contact", {})
        addr = contact.get("address", {}).get("AddressRepresentation", {})

        # School name
        locator_name = addr.get("locatorName", {})
        name_spelling = locator_name.get("spelling", {})
        name = name_spelling.get("text", "") if isinstance(name_spelling, dict) else ""

        # Street
        thoroughfare = addr.get("thoroughfare", {})
        if isinstance(thoroughfare, dict):
            street_obj = thoroughfare.get("GeographicalName", {}).get("spelling", {})
            street = street_obj.get("text", "").strip() if isinstance(street_obj, dict) else ""
        else:
            street = ""

        locator = addr.get("locatorDesignator", "").strip()
        address = f"{street} {locator}".strip() if street else None

        zip_code = addr.get("postCode", "").strip()

        post_name = addr.get("postName", {})
        city_obj = post_name.get("GeographicalName", {})
        city_spelling = city_obj.get("spelling", {})
        city = city_spelling.get("text", "").strip() if isinstance(city_spelling, dict) else ""

        email = contact.get("electronicMailAddress", "")
        phone = contact.get("telephoneVoice", "")
        fax = contact.get("telephoneFacsimile", "")
        website = contact.get("website", "")

        disch = _extract_disch(email)
        school_id = f"BW-{disch}" if disch else f"BW-UUID-{uuid}"

        service_type = props.get("serviceType", {}).get("@href", "")

        schools.append(School(
            id=school_id,
            name=name,
            address=address,
            zip=zip_code,
            city=city,
            email=email,
            phone=phone,
            fax=fax,
            website=website if website else None,
            school_type=service_type,
            latitude=lat,
            longitude=lon,
        ))

    return schools


# --- Sachsen ---

def _load_sachsen_school_types() -> dict[int, str]:
    """Load Sachsen school type mapping from API."""
    resp = fetch("https://schuldatenbank.sachsen.de/api/v1/key_tables/school_types?format=json")
    return {int(entry["key"]): entry["label"] for entry in resp.json()}


def scrape_sachsen() -> list[School]:
    """Scrape Sachsen schools from Schuldatenbank JSON API."""
    url = (
        "https://schuldatenbank.sachsen.de/api/v1/schools?"
        "owner_extended=yes"
        "&school_type_key%5B%5D=11&school_type_key%5B%5D=12"
        "&school_type_key%5B%5D=15&school_type_key%5B%5D=13"
        "&school_type_key%5B%5D=14&school_type_key%5B%5D=16"
        "&school_type_key%5B%5D=31&school_type_key%5B%5D=32"
        "&school_type_key%5B%5D=33&school_type_key%5B%5D=34"
        "&school_type_key%5B%5D=35&school_type_key%5B%5D=36"
        "&school_type_key%5B%5D=37&school_type_key%5B%5D=39"
        "&school_type_key%5B%5D=21&school_type_key%5B%5D=22"
        "&school_type_key%5B%5D=23&school_type_key%5B%5D=24"
        "&school_type_key%5B%5D=25&school_type_key%5B%5D=28"
        "&school_type_key%5B%5D=42&school_type_key%5B%5D=43"
        "&school_type_key%5B%5D=44&building_type_key=01"
        "&fields%5B%5D=id&fields%5B%5D=name"
        "&fields%5B%5D=school_category_key&fields%5B%5D=school_type_keys"
        "&fields%5B%5D=street&fields%5B%5D=postcode"
        "&fields%5B%5D=community&fields%5B%5D=community_key"
        "&fields%5B%5D=community_part&fields%5B%5D=community_part_key"
        "&fields%5B%5D=relocated"
        "&fields%5B%5D=phone_identifier_1&fields%5B%5D=phone_code_1"
        "&fields%5B%5D=phone_number_1"
        "&fields%5B%5D=phone_identifier_2&fields%5B%5D=phone_code_2"
        "&fields%5B%5D=phone_number_2"
        "&fields%5B%5D=phone_identifier_3&fields%5B%5D=phone_code_3"
        "&fields%5B%5D=phone_number_3"
        "&fields%5B%5D=fax_code&fields%5B%5D=fax_number"
        "&fields%5B%5D=mail&fields%5B%5D=homepage"
        "&fields%5B%5D=longitude&fields%5B%5D=latitude"
        "&order%5B%5D=name&format=json"
    )

    school_types = _load_sachsen_school_types()
    raw_schools = fetch(url, timeout=60).json()
    schools = []

    for item in raw_schools:
        building = (item.get("buildings") or [None])[0]
        school = School(name=item.get("name"), id=f"SN-{item.get('id')}")

        if building:
            school.address = building.get("street")
            school.zip = building.get("postcode")
            school.city = building.get("community")
            school.email = building.get("mail")
            school.website = building.get("homepage")
            school.fax = "".join([
                building.get("fax_code") or "",
                building.get("fax_number") or "",
            ])
            school.phone = "".join([
                building.get("phone_code_1") or "",
                building.get("phone_number_1") or "",
            ])
            school.latitude = building.get("latitude")
            school.longitude = building.get("longitude")

            type_keys = building.get("school_type_keys", [None])
            if type_keys:
                school.school_type = school_types.get(type_keys[0])

        schools.append(school)

    return schools


# --- Sachsen-Anhalt ---

def scrape_sachsen_anhalt() -> list[School]:
    """Scrape Sachsen-Anhalt schools from ArcGIS FeatureServer."""
    from pyproj import Transformer

    url = (
        "https://services-eu1.arcgis.com/3jNCHSftk0N4t7dd/arcgis/rest/services/"
        "Schulenstandorte_EPSG25832_2024_25_Sicht/FeatureServer/44/query?"
        "where=1%3D1&outFields=*&f=json"
    )

    transformer = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)
    data = fetch(url, timeout=60).json()
    schools = []

    for feature in data.get("features", []):
        attrs = feature["attributes"]
        geom = feature.get("geometry", {})

        latitude, longitude = None, None
        if geom and "x" in geom and "y" in geom:
            longitude, latitude = transformer.transform(geom["x"], geom["y"])

        school_id = f"ST-ARC{attrs.get('OBJECTID', 0):05d}"

        schools.append(School(
            name=attrs.get("Name"),
            id=school_id,
            city=attrs.get("Ort"),
            school_type=attrs.get("Schulform"),
            legal_status=attrs.get("Kategorie"),
            provider=attrs.get("Traeg_Anw"),
            latitude=latitude,
            longitude=longitude,
        ))

    return schools
