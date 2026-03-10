#!/usr/bin/env python3
"""
Classify mgnify_results/ FTP studies as SOIL / MARINE / OTHER
by sampling one run's SSU taxonomy file per study.
"""
import urllib.request, re, time

FTP_OLD = "https://ftp.ebi.ac.uk/pub/databases/metagenomics/mgnify_results"

SOIL_PHYLA = {
    "Acidobacteriota","Acidobacteria","Verrucomicrobiota","Verrucomicrobia",
    "Gemmatimonadota","Gemmatimonadetes","Chloroflexi","Myxococcota",
    "Planctomycetota","Planctomycetes","Armatimonadota","Nitrososphaerota",
}
MARINE_PHYLA = {
    "Cyanobacteriota","Candidatus_Marinimicrobia","Nitrospinota",
    "Kiritimatiellota","Balneolota","Rhodothermota",
}

STUDY_PREFIXES = {
    "ERP009703":"ERP009","ERP009907":"ERP009","ERP010712":"ERP010",
    "ERP016116":"ERP016","ERP104177":"ERP104","ERP106432":"ERP106",
    "ERP109069":"ERP109","ERP114190":"ERP114","ERP114458":"ERP114",
    "ERP115193":"ERP115","ERP117856":"ERP117","ERP119448":"ERP119",
    "ERP122587":"ERP122","ERP122862":"ERP122","ERP124431":"ERP124",
    "ERP130231":"ERP130","ERP132819":"ERP132","ERP135767":"ERP135",
    "ERP136383":"ERP136","ERP137177":"ERP137","ERP139415":"ERP139",
    "ERP147564":"ERP147","ERP147961":"ERP147","ERP148416":"ERP148",
    "ERP148470":"ERP148","ERP148499":"ERP148","ERP148607":"ERP148",
    "ERP152406":"ERP152","ERP156540":"ERP156","ERP157497":"ERP157",
    "ERP157504":"ERP157","ERP157514":"ERP157","ERP157559":"ERP157",
    "ERP159279":"ERP159","ERP160132":"ERP160","ERP172057":"ERP172",
    "ERP175331":"ERP175",
}


def get(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "soil-pipeline/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return None


def first_err(erp, prefix):
    """Get first ERR run accession within this ERP study."""
    html = get(f"{FTP_OLD}/{prefix}/{erp}/")
    if not html:
        return None
    # Sub-prefix directory level (e.g. ERR855/)
    subprefixes = re.findall(r'href="(ERR\d+)/"', html)
    if not subprefixes:
        return None
    html2 = get(f"{FTP_OLD}/{prefix}/{erp}/{subprefixes[0]}/")
    if not html2:
        return None
    errs = re.findall(r'href="(ERR\d+)/"', html2)
    return errs[0] if errs else None


def parse_ssu_txt(content):
    phyla = {}
    total = 0
    for line in content.splitlines():
        parts = line.split("\t")
        if not parts:
            continue
        try:
            count = int(parts[0])
        except ValueError:
            continue
        for tok in parts[1:]:
            tok = tok.strip()
            if tok.startswith("p__") and len(tok) > 3:
                p = tok[3:]
                phyla[p] = phyla.get(p, 0) + count
        total += count
    if not total:
        return {}
    return {k: round(v / total, 4) for k, v in phyla.items()}


def classify_study(erp, prefix):
    err = first_err(erp, prefix)
    if not err:
        return erp, "NO_ERR", {}

    # mgnify_results layout (re-verified 2026-03):
    #   {BASE}/{prefix}/{erp}/{err[:-3]}/{err}/V6/{type}/taxonomy-summary/SILVA-SSU/{err}_SILVA-SSU.txt
    # Sub-bucket = accession with last 3 digits dropped (e.g. ERR2640150 → ERR2640)
    # {type} is 'amplicon' or 'unknown' depending on the study
    err_short = err[:-3]
    base = f"{FTP_OLD}/{prefix}/{erp}/{err_short}/{err}/V6"

    content = None
    v6_exists = False
    for amp_type in ("amplicon", "unknown"):
        url = f"{base}/{amp_type}/taxonomy-summary/SILVA-SSU/{err}_SILVA-SSU.txt"
        c = get(url)
        if c and not c.startswith("FAIL") and not c.startswith("ERR:"):
            content = c
            break
        if not c or "404" not in c:
            v6_exists = True  # V6 dir might exist even if SILVA-SSU missing

    if not content:
        # Check if V6/ itself is reachable
        v6_html = get(f"{base}/")
        if not v6_html or "404" in v6_html:
            return erp, f"{err}:no_V6", {}
        return erp, f"{err}:no_SILVA-SSU", {}

    phyla = parse_ssu_txt(content)
    return erp, err, phyla


if __name__ == "__main__":
    print(f"{'ERP':<12} {'TAG':<8} {'ERR':<16} {'soil':>6} {'marine':>6}  top_phyla")
    print("-" * 90)

    soil_studies = []

    for erp, prefix in STUDY_PREFIXES.items():
        erp_id, err_or_msg, phyla = classify_study(erp, prefix)

        if not phyla:
            print(f"{erp:<12} {'UNKNOWN':<8} {err_or_msg:<16}")
            time.sleep(0.3)
            continue

        soil_score   = sum(phyla.get(p, 0) for p in SOIL_PHYLA)
        marine_score = sum(phyla.get(p, 0) for p in MARINE_PHYLA)
        top = sorted(phyla.items(), key=lambda x: -x[1])[:3]
        top_str = ", ".join(f"{p}={v:.2f}" for p, v in top)

        if soil_score > 0.05 and marine_score < 0.15:
            tag = "SOIL"
            soil_studies.append(erp)
        elif soil_score > 0.02 and marine_score < 0.2:
            tag = "SOIL?"
            soil_studies.append(erp)
        elif marine_score > 0.3:
            tag = "MARINE"
        else:
            tag = "OTHER"

        print(f"{erp:<12} {tag:<8} {err_or_msg:<16} {soil_score:>6.3f} {marine_score:>6.3f}  {top_str}")
        time.sleep(0.3)

    print()
    print("=" * 90)
    print(f"Soil/likely-soil studies ({len(soil_studies)}): {soil_studies}")
