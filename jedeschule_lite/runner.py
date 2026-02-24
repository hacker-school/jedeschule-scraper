"""Runner — orchestrates all state scrapers."""

import time
from collections.abc import Callable

from jedeschule_lite.schema import School
from jedeschule_lite.scrapers.geojson import (
    scrape_berlin,
    scrape_brandenburg,
    scrape_hamburg,
    scrape_saarland,
)
from jedeschule_lite.scrapers.wfs_xml import (
    scrape_bayern,
    scrape_mecklenburg_vorpommern,
    scrape_thueringen,
)
from jedeschule_lite.scrapers.api import (
    scrape_baden_wuerttemberg,
    scrape_sachsen,
    scrape_sachsen_anhalt,
)
from jedeschule_lite.scrapers.csv_scrapers import (
    scrape_nordrhein_westfalen,
    scrape_schleswig_holstein,
)
from jedeschule_lite.scrapers.html import (
    scrape_bremen,
    scrape_hessen,
    scrape_niedersachsen,
    scrape_rheinland_pfalz,
)


# Registry: state key → scraper function
SCRAPERS: dict[str, Callable[[], list[School]]] = {
    "baden-wuerttemberg": scrape_baden_wuerttemberg,
    "bayern": scrape_bayern,
    "berlin": scrape_berlin,
    "brandenburg": scrape_brandenburg,
    "bremen": scrape_bremen,
    "hamburg": scrape_hamburg,
    "hessen": scrape_hessen,
    "mecklenburg-vorpommern": scrape_mecklenburg_vorpommern,
    "niedersachsen": scrape_niedersachsen,
    "nordrhein-westfalen": scrape_nordrhein_westfalen,
    "rheinland-pfalz": scrape_rheinland_pfalz,
    "saarland": scrape_saarland,
    "sachsen": scrape_sachsen,
    "sachsen-anhalt": scrape_sachsen_anhalt,
    "schleswig-holstein": scrape_schleswig_holstein,
    "thueringen": scrape_thueringen,
}


def scrape_state(state: str) -> list[School]:
    """Scrape schools for a single Bundesland.

    Args:
        state: State key (e.g., "berlin", "bayern", "nordrhein-westfalen")

    Returns:
        List of School objects

    Raises:
        ValueError: If the state key is unknown
    """
    key = state.lower().strip()
    if key not in SCRAPERS:
        available = ", ".join(sorted(SCRAPERS.keys()))
        raise ValueError(f"Unknown state '{state}'. Available: {available}")

    return SCRAPERS[key]()


def scrape_all(
    *,
    states: list[str] | None = None,
    on_error: str = "skip",
) -> list[School]:
    """Scrape schools from all (or selected) Bundeslaender.

    Args:
        states: List of state keys to scrape. None = all 16 states.
        on_error: "skip" to continue on failure, "raise" to stop.

    Returns:
        Combined list of School objects from all states.
    """
    targets = states or list(SCRAPERS.keys())
    all_schools: list[School] = []
    errors: dict[str, str] = {}

    for state in targets:
        key = state.lower().strip()
        if key not in SCRAPERS:
            print(f"WARNING: Unknown state '{state}', skipping")
            continue

        print(f"Scraping {key}...")
        start = time.time()

        try:
            schools = SCRAPERS[key]()
            elapsed = time.time() - start
            print(f"  {key}: {len(schools)} schools ({elapsed:.1f}s)")
            all_schools.extend(schools)
        except Exception as e:
            elapsed = time.time() - start
            print(f"  {key}: FAILED after {elapsed:.1f}s — {e}")
            errors[key] = str(e)
            if on_error == "raise":
                raise

    print(f"\nTotal: {len(all_schools)} schools from {len(targets) - len(errors)} states")
    if errors:
        print(f"Errors in {len(errors)} states: {', '.join(errors.keys())}")

    return all_schools
