"""WFS XML scrapers: Bayern, Thueringen, Mecklenburg-Vorpommern."""

import xmltodict

from jedeschule_lite.schema import School
from jedeschule_lite.utils import fetch, safe_strip


def _parse_wfs_members(xml_text: str) -> list[dict]:
    """Parse wfs:member elements from a WFS XML response."""
    data = xmltodict.parse(xml_text)
    members = data.get("wfs:FeatureCollection", {}).get("wfs:member", [])
    if not isinstance(members, list):
        members = [members]
    return members


def scrape_bayern() -> list[School]:
    """Scrape Bavaria schools from WFS XML endpoint."""
    url = (
        "https://gdiserv.bayern.de/srv112940/services/schulstandortebayern-wfs?"
        "SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&srsname=EPSG:4326"
        "&typename="
        "schul:SchulstandorteGrundschulen,"
        "schul:SchulstandorteMittelschulen,"
        "schul:SchulstandorteRealschulen,"
        "schul:SchulstandorteGymnasien,"
        "schul:SchulstandorteBeruflicheSchulen,"
        "schul:SchulstandorteFoerderzentren,"
        "schul:SchulstandorteWeitererSchulen"
    )
    members = _parse_wfs_members(fetch(url).text)

    schools = []
    for member in members:
        school = next(iter(member.values()), {})
        item = {"id": school.get("@gml:id")}

        for key, value in school.items():
            if key == "schul:geometry":
                pos = value.get("gml:Point", {}).get("gml:pos", "")
                if pos:
                    lon, lat = pos.split()
                    item["lat"] = float(lat)
                    item["lon"] = float(lon)
            elif not key.startswith("@"):
                clean_key = key.split(":", 1)[-1]
                item[clean_key] = value

        schools.append(School(
            name=item.get("schulname"),
            address=item.get("strasse"),
            city=item.get("ort"),
            school_type=item.get("schulart"),
            zip=item.get("postleitzahl"),
            id=f"BY-{item.get('id')}",
            latitude=item.get("lat"),
            longitude=item.get("lon"),
        ))

    return schools


def scrape_thueringen() -> list[School]:
    """Scrape Thuringia schools from GeoProxy WFS XML endpoint."""
    url = (
        "https://www.geoproxy.geoportal-th.de/geoproxy/services/kommunal/komm_wfs?"
        "SERVICE=WFS&REQUEST=GetFeature&typeNames=kommunal:komm_schul"
        "&srsname=EPSG:4326&VERSION=2.0.0"
    )
    members = _parse_wfs_members(fetch(url).text)

    schools = []
    for member in members:
        school_data = member.get("kommunal:komm_schul", {})
        item = {}

        # Extract coordinates
        geom = school_data.get("kommunal:GEOM", {})
        pos = geom.get("gml:Point", {}).get("gml:pos", "")
        if pos:
            lon, lat = pos.split()
            item["lat"] = float(lat)
            item["lon"] = float(lon)

        # Extract fields (strip namespace prefix)
        for key, value in school_data.items():
            if key not in ("kommunal:GEOM", "@gml:id") and value:
                clean_key = key.split(":", 1)[-1] if ":" in key else key
                item[clean_key] = value

        schools.append(School(
            name=item.get("Name"),
            id=f"TH-{item.get('Schulnummer')}",
            address=" ".join(
                filter(None, [item.get("Strasse"), item.get("Hausnummer")])
            ),
            zip=item.get("PLZ"),
            city=item.get("Ort"),
            website=item.get("Webseite"),
            email=item.get("EMail"),
            school_type=item.get("Schulart"),
            provider=item.get("Traeger"),
            fax=item.get("Faxnummer"),
            phone=item.get("Telefonnummer"),
            latitude=item.get("lat"),
            longitude=item.get("lon"),
        ))

    return schools


def scrape_mecklenburg_vorpommern() -> list[School]:
    """Scrape Mecklenburg-Vorpommern schools from Geodaten WFS XML."""
    url = (
        "https://www.geodaten-mv.de/dienste/schulstandorte_wfs?"
        "SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0"
        "&srsname=EPSG%3A4326&typeNames="
        "ms:schultyp_grund,"
        "ms:schultyp_regional,"
        "ms:schultyp_gymnasium,"
        "ms:schultyp_gesamt,"
        "ms:schultyp_waldorf,"
        "ms:schultyp_foerder,"
        "ms:schultyp_abendgym,"
        "ms:schultyp_berufs"
    )

    def as_string(value: str) -> str:
        try:
            return str(int(value))
        except (ValueError, TypeError):
            return str(value) if value else ""

    def extract_school_data(school_elem: dict) -> dict:
        item = {}
        for key, value in school_elem.items():
            if key == "ms:msGeometry":
                pos = value.get("gml:Point", {}).get("gml:pos", "")
                if pos:
                    lat, lon = pos.split()
                    item["lat"] = float(lat)
                    item["lon"] = float(lon)
            elif not key.startswith("@"):
                clean_key = key.split(":", 1)[-1] if ":" in key else key
                item[clean_key] = value
        return item

    data = xmltodict.parse(fetch(url).text)
    feature_collection = data.get("wfs:FeatureCollection", {})
    members = feature_collection.get("wfs:member", [])
    if not isinstance(members, list):
        members = [members]

    schools = []
    for member in members:
        # MV has nested FeatureCollections for some school types
        if "wfs:FeatureCollection" in member:
            inner = member["wfs:FeatureCollection"].get("wfs:member", [])
            if not isinstance(inner, list):
                inner = [inner]
            for inner_member in inner:
                school_elem = next(iter(inner_member.values()), {})
                item = extract_school_data(school_elem)
                schools.append(_build_mv_school(item, as_string))
        else:
            school_elem = next(iter(member.values()), {})
            item = extract_school_data(school_elem)
            schools.append(_build_mv_school(item, as_string))

    return schools


def _build_mv_school(item: dict, as_string) -> School:
    return School(
        name=safe_strip(item.get("schulname")),
        id=f"MV-{as_string(item.get('dstnr', ''))}",
        address=safe_strip(item.get("strassehnr")),
        address2="",
        zip=as_string(item.get("plz", "")).zfill(5),
        city=safe_strip(item.get("ort")),
        website=safe_strip(item.get("internet")),
        email=safe_strip(item.get("emailadresse")),
        phone=safe_strip(item.get("telefon")),
        director=safe_strip(item.get("schulleiter")),
        school_type=safe_strip(item.get("orgform")),
        legal_status=safe_strip(item.get("rechtsstatus")),
        provider=safe_strip(item.get("schultraeger")),
        latitude=item.get("lat"),
        longitude=item.get("lon"),
    )
