"""CSV-based scrapers: Nordrhein-Westfalen, Schleswig-Holstein."""

import csv
import io

from jedeschule_lite.schema import School
from jedeschule_lite.utils import fetch


# --- NRW Helper ---

class _NRWHelper:
    """Loads NRW key mappings for school forms, legal status, providers."""

    def __init__(self):
        self.mappings = self._load_data()

    def _load_data(self) -> dict:
        sources = {
            "rechtsform": "https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/key_rechtsform.csv",
            "schulform": "https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/key_schulformschluessel.csv",
        }
        data = {}
        for key, url in sources.items():
            data[key] = self._get_map(url)
        data["provider"] = self._get_provider()
        return data

    def _get_map(self, url: str) -> dict:
        response = fetch(url)
        response.encoding = "utf-8"
        # Skip line 1 (separator info) and line 2 (headers)
        reader = csv.reader(response.text.splitlines()[2:], delimiter=";")
        return {line[0]: line[1] for line in reader if len(line) >= 2}

    def _get_provider(self) -> dict:
        url = "https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/key_traeger.csv"
        response = fetch(url)
        response.encoding = "utf-8"
        reader = csv.reader(response.text.splitlines()[2:], delimiter=";")
        return {
            line[0]: " ".join(line[n] for n in range(1, min(4, len(line)))).strip()
            for line in reader
            if line
        }

    def resolve(self, data_type: str, key: str) -> str | None:
        return self.mappings.get(data_type, {}).get(key)


def scrape_nordrhein_westfalen() -> list[School]:
    """Scrape NRW schools from open data CSV."""
    from pyproj import Transformer

    url = "https://www.schulministerium.nrw.de/BiPo/OpenData/Schuldaten/schuldaten.csv"
    response = fetch(url, timeout=60)
    body = response.content.decode("utf-8").splitlines()
    # First line contains separator info, skip it
    reader = csv.DictReader(body[1:], delimiter=";")

    helper = _NRWHelper()
    schools = []

    for item in reader:
        name = " ".join([
            item.get("Schulbezeichnung_1", ""),
            item.get("Schulbezeichnung_2", ""),
            item.get("Schulbezeichnung_3", ""),
        ]).strip()

        right, high = item.get("UTMRechtswert"), item.get("UTMHochwert")
        source_crs = item.get("EPSG")
        if source_crs == "null":
            source_crs = "EPSG:25832"

        lat, lon = None, None
        try:
            transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
            lon, lat = transformer.transform(right, high)
        except Exception:
            pass

        schools.append(School(
            name=name,
            id=f"NW-{item.get('Schulnummer')}",
            address=item.get("Strasse"),
            zip=item.get("PLZ"),
            city=item.get("Ort"),
            website=item.get("Homepage"),
            email=item.get("E-Mail"),
            legal_status=helper.resolve("rechtsform", item.get("Rechtsform")),
            school_type=helper.resolve("schulform", item.get("Schulform")),
            provider=helper.resolve("provider", item.get("Traegernummer")),
            fax=f"{item.get('Faxvorwahl', '')}{item.get('Fax', '')}",
            phone=f"{item.get('Telefonvorwahl', '')}{item.get('Telefon', '')}",
            latitude=lat,
            longitude=lon,
        ))

    return schools


def scrape_schleswig_holstein() -> list[School]:
    """Scrape Schleswig-Holstein schools from open data CSV."""
    url = "https://opendata.schleswig-holstein.de/collection/schulen/aktuell.csv"
    response = fetch(url)
    reader = csv.DictReader(response.text.splitlines(), delimiter="\t")

    return [
        School(
            name=row.get("name"),
            id=f"SH-{row.get('id')}",
            address=f"{row.get('street', '')} {row.get('houseNumber', '')}".strip(),
            zip=row.get("zipcode"),
            city=row.get("city"),
            email=row.get("email"),
            fax=row.get("fax"),
            phone=row.get("phone"),
            latitude=float(row["latitude"]) if row.get("latitude") else None,
            longitude=float(row["longitude"]) if row.get("longitude") else None,
        )
        for row in reader
    ]
