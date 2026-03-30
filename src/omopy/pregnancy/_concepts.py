"""Concept set definitions for the HIPPS pregnancy identification algorithm.

Bundles the concept IDs, outcome categories, Matcho spacing limits, and term
durations used by the HIP, PPS, and ESD pipeline steps.  The R package
loads these from Excel files; here they are defined as module-level
constants so the package stays dependency-free and easy to test.

The concept IDs are *representative* SNOMED / OMOP standard concept IDs.
Production deployments should refine these lists against their local
vocabulary tables.
"""

from __future__ import annotations

__all__ = [
    "OUTCOME_CATEGORIES",
    "HIP_CONCEPTS",
    "PPS_CONCEPTS",
    "ESD_CONCEPTS",
    "MATCHO_OUTCOME_LIMITS",
    "MATCHO_TERM_DURATIONS",
]


# ---------------------------------------------------------------------------
# Outcome categories
# ---------------------------------------------------------------------------

OUTCOME_CATEGORIES: dict[str, str] = {
    "LB": "Live birth",
    "SB": "Stillbirth",
    "AB": "Abortion",
    "SA": "Spontaneous abortion",
    "DELIV": "Delivery (unspecified)",
    "ECT": "Ectopic pregnancy",
    "PREG": "Pregnancy (ongoing/unspecified)",
}

# ---------------------------------------------------------------------------
# HIP concepts  —  pregnancy *outcome* concepts
# ---------------------------------------------------------------------------
# Each entry: concept_id -> {name, category, gest_value}
# ``gest_value`` is the gestational age in weeks implied by the concept
# (None when not applicable).

HIP_CONCEPTS: dict[int, dict] = {
    # ---- Live birth (LB) ----
    4014295: {"name": "Live birth", "category": "LB", "gest_value": 40},
    4302541: {"name": "Single live birth", "category": "LB", "gest_value": 40},
    4092289: {"name": "Delivery of live newborn", "category": "LB", "gest_value": 40},
    4128331: {"name": "Normal delivery", "category": "LB", "gest_value": 40},
    4324765: {"name": "Delivery by cesarean section", "category": "LB", "gest_value": 38},
    440059:  {"name": "Cesarean section", "category": "LB", "gest_value": 38},
    4032243: {"name": "Outcome of delivery - live birth", "category": "LB", "gest_value": 40},
    4195928: {"name": "Born alive", "category": "LB", "gest_value": 40},
    4129922: {"name": "Twin live born", "category": "LB", "gest_value": 37},
    4138969: {"name": "Premature delivery", "category": "LB", "gest_value": 34},
    # ---- Stillbirth (SB) ----
    4014296: {"name": "Stillbirth", "category": "SB", "gest_value": 28},
    4092290: {"name": "Delivery of stillborn", "category": "SB", "gest_value": 28},
    439389:  {"name": "Fetal death", "category": "SB", "gest_value": 28},
    4136750: {"name": "Intrauterine death", "category": "SB", "gest_value": 28},
    4067106: {"name": "Late fetal death", "category": "SB", "gest_value": 28},
    # ---- Abortion (AB) ----
    4067814: {"name": "Induced abortion", "category": "AB", "gest_value": None},
    4090551: {"name": "Elective termination of pregnancy", "category": "AB", "gest_value": None},
    4144921: {"name": "Legal abortion", "category": "AB", "gest_value": None},
    4264339: {"name": "Therapeutic abortion", "category": "AB", "gest_value": None},
    46273478: {"name": "Induced termination of pregnancy", "category": "AB", "gest_value": None},
    # ---- Spontaneous abortion (SA) ----
    4199459: {"name": "Spontaneous abortion", "category": "SA", "gest_value": None},
    4071712: {"name": "Miscarriage", "category": "SA", "gest_value": None},
    4082503: {"name": "Missed miscarriage", "category": "SA", "gest_value": None},
    4146945: {"name": "Incomplete miscarriage", "category": "SA", "gest_value": None},
    4012477: {"name": "Blighted ovum", "category": "SA", "gest_value": None},
    # ---- Delivery unspecified (DELIV) ----
    4063381: {"name": "Delivery procedure", "category": "DELIV", "gest_value": 39},
    4148250: {"name": "Forceps delivery", "category": "DELIV", "gest_value": 39},
    4127886: {"name": "Vacuum extraction delivery", "category": "DELIV", "gest_value": 39},
    4142115: {"name": "Vaginal delivery", "category": "DELIV", "gest_value": 39},
    4241044: {"name": "Induction of labor", "category": "DELIV", "gest_value": 39},
    # ---- Ectopic pregnancy (ECT) ----
    443213:  {"name": "Ectopic pregnancy", "category": "ECT", "gest_value": None},
    4060360: {"name": "Tubal pregnancy", "category": "ECT", "gest_value": None},
    4170147: {"name": "Cornual ectopic pregnancy", "category": "ECT", "gest_value": None},
    4148218: {"name": "Abdominal ectopic pregnancy", "category": "ECT", "gest_value": None},
    4141992: {"name": "Cervical ectopic pregnancy", "category": "ECT", "gest_value": None},
}

# Convenience: concept_id -> category
HIP_CONCEPT_CATEGORIES: dict[int, str] = {
    cid: info["category"] for cid, info in HIP_CONCEPTS.items()
}

# Convenience: set of all HIP concept IDs
HIP_CONCEPT_IDS: frozenset[int] = frozenset(HIP_CONCEPTS.keys())

# ---------------------------------------------------------------------------
# Matcho outcome limits (minimum days between two pregnancies by category)
# ---------------------------------------------------------------------------
# Key: (first_outcome_category, next_outcome_category) -> min_days

MATCHO_OUTCOME_LIMITS: dict[tuple[str, str], int] = {
    ("LB", "LB"): 168,
    ("LB", "SB"): 168,
    ("LB", "AB"): 56,
    ("LB", "SA"): 56,
    ("LB", "DELIV"): 168,
    ("LB", "ECT"): 56,
    ("SB", "LB"): 168,
    ("SB", "SB"): 168,
    ("SB", "AB"): 56,
    ("SB", "SA"): 56,
    ("SB", "DELIV"): 168,
    ("SB", "ECT"): 56,
    ("AB", "LB"): 56,
    ("AB", "SB"): 56,
    ("AB", "AB"): 42,
    ("AB", "SA"): 42,
    ("AB", "DELIV"): 56,
    ("AB", "ECT"): 42,
    ("SA", "LB"): 56,
    ("SA", "SB"): 56,
    ("SA", "AB"): 42,
    ("SA", "SA"): 42,
    ("SA", "DELIV"): 56,
    ("SA", "ECT"): 42,
    ("DELIV", "LB"): 168,
    ("DELIV", "SB"): 168,
    ("DELIV", "AB"): 56,
    ("DELIV", "SA"): 56,
    ("DELIV", "DELIV"): 168,
    ("DELIV", "ECT"): 56,
    ("ECT", "LB"): 56,
    ("ECT", "SB"): 56,
    ("ECT", "AB"): 42,
    ("ECT", "SA"): 42,
    ("ECT", "DELIV"): 56,
    ("ECT", "ECT"): 42,
}

# ---------------------------------------------------------------------------
# Matcho term durations (expected pregnancy length by outcome)
# ---------------------------------------------------------------------------
# category -> (min_days, max_days)

MATCHO_TERM_DURATIONS: dict[str, tuple[int, int]] = {
    "LB": (140, 308),
    "SB": (140, 308),
    "AB": (28, 308),
    "SA": (28, 168),
    "DELIV": (140, 308),
    "ECT": (28, 84),
    "PREG": (28, 308),
}

# ---------------------------------------------------------------------------
# PPS concepts  —  gestational-timing concepts
# ---------------------------------------------------------------------------
# Each entry: concept_id -> {name, min_month, max_month}
# min_month/max_month define the expected gestational month range for this
# observation to occur.

PPS_CONCEPTS: dict[int, dict] = {
    # Prenatal visit observations
    4047564: {"name": "First trimester screening", "min_month": 2, "max_month": 4},
    4048384: {"name": "Second trimester screening", "min_month": 4, "max_month": 7},
    4200046: {"name": "Third trimester pregnancy examination", "min_month": 7, "max_month": 10},
    4048098: {"name": "Prenatal visit - first", "min_month": 1, "max_month": 3},
    4230360: {"name": "Prenatal care supervision", "min_month": 1, "max_month": 10},
    # Ultrasound / imaging
    4098620: {"name": "Pregnancy ultrasound", "min_month": 2, "max_month": 9},
    4113990: {"name": "Dating ultrasound", "min_month": 2, "max_month": 5},
    4113553: {"name": "Fetal anatomy ultrasound", "min_month": 4, "max_month": 6},
    # Lab / blood tests
    4037340: {"name": "Beta-HCG measurement", "min_month": 1, "max_month": 4},
    4035205: {"name": "Alpha-fetoprotein measurement", "min_month": 3, "max_month": 6},
    # Prenatal conditions
    4103532: {"name": "Gestational diabetes screening", "min_month": 5, "max_month": 8},
    4218106: {"name": "Gestational hypertension", "min_month": 5, "max_month": 10},
    # Late pregnancy
    4238072: {"name": "Term pregnancy", "min_month": 9, "max_month": 10},
    4136460: {"name": "Post-term pregnancy", "min_month": 10, "max_month": 11},
    4044947: {"name": "Pregnancy-related condition", "min_month": 1, "max_month": 10},
}

# Convenience: set of all PPS concept IDs
PPS_CONCEPT_IDS: frozenset[int] = frozenset(PPS_CONCEPTS.keys())

# ---------------------------------------------------------------------------
# ESD concepts  —  gestational-week / gestational-range evidence
# ---------------------------------------------------------------------------
# Each entry: concept_id -> {name, domain, category}
# domain: "measurement" | "observation" | "condition"
# category: "GW" (gestational week) | "GR3m" (gestational range 3-month)

ESD_CONCEPTS: dict[int, dict] = {
    # Gestational week measurements
    4260747: {"name": "Gestational age in weeks", "domain": "measurement", "category": "GW"},
    3036277: {"name": "Gestational age at birth", "domain": "measurement", "category": "GW"},
    3013451: {"name": "Gestational age by LMP", "domain": "measurement", "category": "GW"},
    3018923: {"name": "Gestational age by ultrasound", "domain": "measurement", "category": "GW"},
    # Gestational range (trimester) observations
    4299535: {"name": "First trimester of pregnancy", "domain": "condition", "category": "GR3m"},
    4128160: {"name": "Second trimester of pregnancy", "domain": "condition", "category": "GR3m"},
    4219502: {"name": "Third trimester of pregnancy", "domain": "condition", "category": "GR3m"},
    # Additional GW measurements
    3017731: {"name": "Fetal biometry gestational age", "domain": "measurement", "category": "GW"},
    3004501: {"name": "Estimated gestational age", "domain": "measurement", "category": "GW"},
}

# Convenience: set of all ESD concept IDs
ESD_CONCEPT_IDS: frozenset[int] = frozenset(ESD_CONCEPTS.keys())

# GR3m month ranges (trimester -> month range)
GR3M_MONTH_RANGES: dict[int, tuple[int, int]] = {
    4299535: (1, 3),   # First trimester
    4128160: (4, 6),   # Second trimester
    4219502: (7, 9),   # Third trimester
}
