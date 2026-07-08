"""psc_families — the capability-family key + static PSC family-part names.

Freeze doc §0.1.3 (corrected definition, operator-ratified 2026-07-08):

    family_key = NAICS[:4] + 'x' + psc_family(PSC)
    psc_family = PSC[0]  when the first char is a LETTER (services R…/K…/M…/S…, R&D A…)
               = PSC[:2] when numeric (products: the 2-digit FSC GROUP)

One-digit product truncation is wrong — the first TWO digits of a numeric PSC are
the FSC group; one digit collapses 1410 guided missiles / 1510 aircraft / 1903
ships into a meaningless family "1".

The names below are the static public PSC taxonomy (service category letters +
FSC groups) — the psc half of the family-titles reference (addendum §2). The
NAICS-4 half comes from `naics_reference` at build time.
"""
from __future__ import annotations

# Service / R&D category letters (PSC first character when alpha).
SERVICE_CATEGORY_NAMES: dict[str, str] = {
    "A": "Research & Development",
    "B": "Special Studies & Analyses",
    "C": "Architect & Engineering Services",
    "D": "ADP & Telecommunications",
    "E": "Purchase of Structures & Facilities",
    "F": "Natural Resources & Conservation",
    "G": "Social Services",
    "H": "Quality Control, Testing & Inspection",
    "J": "Maintenance & Repair of Equipment",
    "K": "Modification of Equipment",
    "L": "Technical Representative Services",
    "M": "Operation of Government-Owned Facilities",
    "N": "Installation of Equipment",
    "P": "Salvage Services",
    "Q": "Medical Services",
    "R": "Professional, Administrative & Management Support",
    "S": "Utilities & Housekeeping",
    "T": "Photographic, Mapping, Printing & Publication",
    "U": "Education & Training",
    "V": "Transportation, Travel & Relocation",
    "W": "Lease/Rental of Equipment",
    "X": "Lease/Rental of Facilities",
    "Y": "Construction of Structures & Facilities",
    "Z": "Maintenance, Repair & Alteration of Real Property",
}

# FSC groups (numeric PSC first two digits).
FSC_GROUP_NAMES: dict[str, str] = {
    "10": "Weapons",
    "11": "Nuclear Ordnance",
    "12": "Fire Control Equipment",
    "13": "Ammunition & Explosives",
    "14": "Guided Missiles",
    "15": "Aircraft & Airframe Structural Components",
    "16": "Aircraft Components & Accessories",
    "17": "Aircraft Launching, Landing & Ground Handling Equipment",
    "18": "Space Vehicles",
    "19": "Ships, Small Craft, Pontoons & Floating Docks",
    "20": "Ship & Marine Equipment",
    "22": "Railway Equipment",
    "23": "Motor Vehicles, Trailers & Cycles",
    "24": "Tractors",
    "25": "Vehicular Equipment Components",
    "26": "Tires & Tubes",
    "28": "Engines, Turbines & Components",
    "29": "Engine Accessories",
    "30": "Mechanical Power Transmission Equipment",
    "31": "Bearings",
    "32": "Woodworking Machinery & Equipment",
    "34": "Metalworking Machinery",
    "35": "Service & Trade Equipment",
    "36": "Special Industry Machinery",
    "37": "Agricultural Machinery & Equipment",
    "38": "Construction, Mining, Excavating & Highway Maintenance Equipment",
    "39": "Materials Handling Equipment",
    "40": "Rope, Cable, Chain & Fittings",
    "41": "Refrigeration, Air Conditioning & Air Circulating Equipment",
    "42": "Fire Fighting, Rescue & Safety Equipment",
    "43": "Pumps & Compressors",
    "44": "Furnace, Steam Plant & Drying Equipment",
    "45": "Plumbing, Heating & Waste Disposal Equipment",
    "46": "Water Purification & Sewage Treatment Equipment",
    "47": "Pipe, Tubing, Hose & Fittings",
    "48": "Valves",
    "49": "Maintenance & Repair Shop Equipment",
    "51": "Hand Tools",
    "52": "Measuring Tools",
    "53": "Hardware & Abrasives",
    "54": "Prefabricated Structures & Scaffolding",
    "55": "Lumber, Millwork, Plywood & Veneer",
    "56": "Construction & Building Materials",
    "58": "Communication, Detection & Coherent Radiation Equipment",
    "59": "Electrical & Electronic Equipment Components",
    "60": "Fiber Optics Materials, Components & Assemblies",
    "61": "Electric Wire & Power Distribution Equipment",
    "62": "Lighting Fixtures & Lamps",
    "63": "Alarm, Signal & Security Detection Systems",
    "65": "Medical, Dental & Veterinary Equipment & Supplies",
    "66": "Instruments & Laboratory Equipment",
    "67": "Photographic Equipment",
    "68": "Chemicals & Chemical Products",
    "69": "Training Aids & Devices",
    "70": "ADP Equipment, Software & Support Equipment",
    "71": "Furniture",
    "72": "Household & Commercial Furnishings & Appliances",
    "73": "Food Preparation & Serving Equipment",
    "74": "Office Machines & Text Processing Systems",
    "75": "Office Supplies & Devices",
    "76": "Books, Maps & Other Publications",
    "77": "Musical Instruments, Phonographs & Home-Type Radios",
    "78": "Recreational & Athletic Equipment",
    "79": "Cleaning Equipment & Supplies",
    "80": "Brushes, Paints, Sealers & Adhesives",
    "81": "Containers, Packaging & Packing Supplies",
    "83": "Textiles, Leather, Furs & Apparel Findings",
    "84": "Clothing, Individual Equipment & Insignia",
    "85": "Toiletries",
    "87": "Agricultural Supplies",
    "88": "Live Animals",
    "89": "Subsistence (Food)",
    "91": "Fuels, Lubricants, Oils & Waxes",
    "93": "Nonmetallic Fabricated Materials",
    "94": "Nonmetallic Crude Materials",
    "95": "Metal Bars, Sheets & Shapes",
    "96": "Ores, Minerals & Their Primary Products",
    "99": "Miscellaneous",
}


def psc_family(psc: str) -> str | None:
    """The psc half of the family key — letter for services/R&D, FSC group for products."""
    p = (psc or "").strip().upper()
    if not p:
        return None
    if p[0].isalpha():
        return p[0]
    return p[:2]


def family_key(naics: str, psc: str) -> str | None:
    """`5413xR` / `3364x15` — None when either half is absent (null ≠ zero)."""
    n = (naics or "").strip()
    fam = psc_family(psc)
    if len(n) < 4 or fam is None:
        return None
    return f"{n[:4]}x{fam}"


def psc_family_name(fam: str) -> str:
    """Display name of the psc half; falls back to the code itself."""
    if fam.isalpha():
        return SERVICE_CATEGORY_NAMES.get(fam, f"PSC {fam}")
    return FSC_GROUP_NAMES.get(fam, f"FSC {fam}")
