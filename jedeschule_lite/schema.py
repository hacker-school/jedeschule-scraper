"""Normalized school data schema — matches the original jedeschule items.py."""

from dataclasses import dataclass, asdict


@dataclass
class School:
    id: str | None = None
    name: str | None = None
    address: str | None = None
    address2: str | None = None
    zip: str | None = None
    city: str | None = None
    website: str | None = None
    email: str | None = None
    school_type: str | None = None
    legal_status: str | None = None
    provider: str | None = None
    fax: str | None = None
    phone: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    director: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)
