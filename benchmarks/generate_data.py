"""Generate synthetic 15k-row dataset for benchmarking."""

import json
import os
import random
import string

SEED = 42
N_SOURCE = 15000
N_TARGET = 500

COMPANY_BASES = [
    "Nexacore", "Vanteon", "Protillium", "Qualidyne", "Orizon",
    "Tremount", "Caldris", "Pelomar", "Syntriva", "Brevix",
    "Korvane", "Meridax", "Pinnova", "Straton", "Telvaris",
    "Auximed", "Cygnova", "Duraven", "Fontaris", "Glenvista",
    "Heliomar", "Invectis", "Junaris", "Kestova", "Bapienx",
    "Armitage", "Zentara", "Blivox", "Corenth", "Daxmore",
]

SUFFIXES = ["Inc", "LLC", "Corp", "Ltd", "Pty Ltd", "Group", "Co", "Services", "Solutions", "Holdings"]

STREETS = [
    "Main Street", "Broadway", "Market Street", "Commerce Blvd",
    "Technology Drive", "Industrial Pkwy", "Lakeside Dr",
    "Water Street", "Central Expressway", "Arch Street",
    "Sunset Boulevard", "Jorie Blvd", "Trinity Blvd",
    "Anton Blvd", "Liberty Plaza", "Peachtree St",
    "Interpace Pkwy", "Plano Pkwy", "Wayzata Blvd",
    "Louisiana Street", "Riverside Plaza", "Medical Center Dr",
    "Vesey Street", "Hope Street", "Loop West",
]

CITIES = [
    # US
    "New York NY 10005", "San Jose CA 95110", "Dallas TX 75201",
    "Philadelphia PA 19103", "Chicago IL 60601", "Los Angeles CA 90069",
    "Fort Worth TX 76155", "Atlanta GA 30309", "Houston TX 77002",
    # International
    "London EC2A 1AF", "Melbourne VIC 3000", "Toronto ON M5V 2T6",
    "Singapore 048583", "Frankfurt 60311", "Tokyo 100-0005",
    "Sydney NSW 2000", "Mumbai 400001", "Sao Paulo 01310-100",
]

RECENT_DATES = [f"2026-{m:02d}-{d:02d}" for m in range(1, 5) for d in [1, 10, 20]]
STALE_DATES = [f"2023-{m:02d}-{d:02d}" for m in range(1, 13) for d in [1, 15]]


def gen_company_name():
    return random.choice(COMPANY_BASES) + " " + random.choice(SUFFIXES)


def gen_address():
    num = random.randint(100, 9999)
    street = random.choice(STREETS)
    city = random.choice(CITIES)
    return f"{num} {street}", city


def add_name_noise(name):
    r = random.random()
    if r < 0.2:
        return name.lower()
    elif r < 0.35:
        return name.upper()
    elif r < 0.5:
        return name + ","
    elif r < 0.6:
        return name + "."
    elif r < 0.7:
        return "  " + name + "  "
    return name


def add_addr_noise(addr):
    r = random.random()
    if r < 0.15:
        return addr.lower()
    elif r < 0.30:
        # Clean-tier noise: abbreviate
        return addr.replace("Street", "St").replace("Boulevard", "Blvd").replace("Drive", "Dr").replace("Parkway", "Pkwy")
    elif r < 0.40:
        # Clean-tier noise: expand
        return addr.replace("N ", "North ").replace("W ", "West ")
    elif r < 0.55:
        # Normalized-tier noise: full expansion/contraction
        return addr.replace("Blvd", "Boulevard").replace("St", "Street").replace("Dr", "Drive").replace("Pkwy", "Parkway").replace("Ave", "Avenue")
    return addr


def generate():
    random.seed(SEED)

    target = []
    for i in range(N_TARGET):
        addr1, addr2 = gen_address()
        # ~20% of target records are stale (>2 years old) to test date gate
        is_stale = random.random() < 0.2
        target.append({
            "vendor_name": gen_company_name(),
            "vendor_id": f"V{300000 + i}",
            "address1": addr1,
            "address2": addr2,
            "updated": random.choice(STALE_DATES) if is_stale else random.choice(RECENT_DATES),
        })

    source = []
    for i in range(N_SOURCE):
        if random.random() < 0.7:
            ref = random.choice(target)
            name = add_name_noise(ref["vendor_name"])
            addr1 = add_addr_noise(ref["address1"])
            addr2 = add_addr_noise(ref["address2"]) if random.random() > 0.3 else ""
        else:
            name = gen_company_name() + " " + "".join(random.choices(string.ascii_letters, k=3))
            addr1, addr2 = gen_address()
            addr1 = add_addr_noise(addr1)

        is_stale = random.random() < 0.3
        source.append({
            "l3_fmly_nm": name,
            "vendor_id": f"V7{48000 + i}",
            "hq_addr1": addr1,
            "hq_addr2": addr2,
            "cntrct_cmpl_dt": random.choice(STALE_DATES) if is_stale else random.choice(RECENT_DATES),
        })

    os.makedirs("benchmarks/results", exist_ok=True)
    with open("benchmarks/results/source.json", "w") as f:
        json.dump(source, f)
    with open("benchmarks/results/target.json", "w") as f:
        json.dump(target, f)

    print(f"Generated {len(source)} source + {len(target)} target records")
    print(f"Written to benchmarks/results/source.json and target.json")


if __name__ == "__main__":
    generate()
