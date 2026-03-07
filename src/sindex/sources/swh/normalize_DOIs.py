import json
import re
import urllib.parse

def normalize_and_reset(input_path, output_path):
    """
    This function cleans DOI strings by removing
    trailing noise and artifacts (e.g., appended URLs,
    query parameters, file extensions, badges, and
    other non-DOI fragments) to extract a normalized DOI.
    """
    def normalize(raw):
        if not isinstance(raw, str):
            return None

        # 1. Decode
        raw = urllib.parse.unquote(raw).strip()
        raw = raw.replace("\\\\", "").replace("\\", "")

        # Unicode smart quote temizleme
        raw = raw.replace("\u201c", '"').replace("\u201d", '"')
        raw = raw.replace("\u2018", "'").replace("\u2019", "'")

        # 2. Extract last DOI-like block
        all_dois = re.findall(
            r'10\.\d{4,9}/[^\s\"\'\[\]\<\>]+',
            raw
        )
        if all_dois:
            raw = all_dois[-1]

        # 3. Remove doi.org wrappers + appended URLs
        raw = re.sub(
            r'^https?://(dx\.)?doi\.org/',
            '',
            raw,
            flags=re.I
        )
        doi = re.sub(r'\.(https?|url).*$', '', raw, flags=re.I)
        doi = re.sub(r'\(https?:.*$', '', doi, flags=re.I)
        doi = re.sub(r'https?:/+.*$', '', doi, flags=re.I)

        # 4. Remove query / fragment
        doi = re.split(r'[?#";]', doi)[0]
        if "&" in doi:
            doi = doi.split("&")[0]

        # 5. Remove extensions
        doi = re.sub(
            r'\.(svg|png|jpg|jpeg|pdf|html?|epdf|zip|short|status|docx|java|exe|msi|dmg|apk)$',
            '',
            doi,
            flags=re.I
        )
        # 6. Remove badge suffixes (TÜM renkler)
        doi = re.sub(r'-[a-z]{4,20}$', '', doi, flags=re.I)

        # 7. Trailing punctuation and underscores
        doi = re.sub(
            r'[.,:?!\s\"\'\\\*`/>}\)\]_]+$',
            '',
            doi
        )
        # 8. Version / abstract cleanup
        doi = re.sub(r'\.abstract$', '', doi, flags=re.I)
        doi = re.sub(r'abstract$', '', doi, flags=re.I)
        doi = re.sub(r'(data|this|that|here|info|click|link)$', '', doi, flags=re.I)
        doi = re.sub(
            r'/(full|fulltext|html)(/html)?$',
            '',
            doi,
            flags=re.I
        )
        doi = re.sub(
            r'/(attachment|supplementary|media|content|mmc\d+|supporting_information)(/.*)?$',
            '',
            doi,
            flags=re.I
        )
        doi = re.sub(r'\.full-text$', '', doi, flags=re.I)
        doi = re.sub(r'\.full$', '', doi, flags=re.I)
        doi = re.sub(r'\.$', '', doi)

        # 9. Bracket balance
        brackets = [("(", ")"), ("{", "}"), ("[", "]"), ("<", ">")]
        for o, c in brackets:
            while doi.endswith(c) and doi.count(c) > doi.count(o):
                doi = doi[:-1]
            while doi.endswith(o) and doi.count(o) > doi.count(c):
                doi = doi[:-1]

        # 10. Leading junk
        doi = re.sub(r'^[\[\(\{\"\'<\s_]+', '', doi)
        doi = doi.strip().lower()

        # 11. BioRxiv / Authorea version koruma
        is_special = doi.startswith("10.1101/") or doi.startswith("10.22541/")
        if is_special:
            doi = re.sub(r'\.(abstract|full|pdf)$', '', doi, flags=re.I)
            doi = re.sub(r'\.supplementary.*$', '', doi, flags=re.I)

        # 12. Path cleanup
        parts = [p for p in doi.split("/") if p and not any(x in p for x in ['bluestacks', '_native.exe', '.exe'])]
        strong_web_markers = {
            "abstract", "full", "download", "supplementary",
            "epdf", "meta", "title", "references", "introduction",
            "methods", "results", "figures", "tables", "pdf", "summary"
        }
        while len(parts) >= 3:
            last = parts[-1]
            is_trash = (
                    last in strong_web_markers
                    or (last.isdigit() and len(last) > 6)
                    or re.match(r'^abstract\d+$', last, re.I)
                    or re.match(r"^[a-z\-]+-\d{10,}", last)
                    or last in ('asset', 'graphic', 'assets', 'binary', 'luennot-files', 'src', 'main', 'java', 'com',
                                'google', 'android')
                    or re.match(r'^(qo_|advances_)', last, re.I)
                    or 'supplementary' in last
                    or 'template' in last
                    or re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', last)
                    or (len(last) > 40 and "-" in last)
                    or last.startswith("acrefore-")
                    or last.startswith("oso-")
            )
            if is_trash:
                parts.pop()
            else:
                break
        if len(parts) >= 4 and parts[2].isdigit() and len(parts[2]) >= 5:
            parts = parts[:3]

        # 13. Repeated identifier cleanup
        if len(parts) == 5:
            if parts[2] == parts[4] and parts[3].isdigit():
                parts = parts[:3]
        doi = "/".join(parts)

        # 14. Final validation
        if (
                doi.count(".") > 10
                or "microsoft.azure" in doi
                or "2020/01/04" in doi
                or doi.startswith("10.6220/2014_reverse")
                or doi.startswith("10.19420/play-services")
        ):
            return None
        if not re.match(
                r'^10\.\d{4,9}/[a-z0-9().:_;\-/<>]+$',
                doi,
                re.I
        ):
            return None
        if len(doi) < 9:
            return None
        return doi

    # NDJSON processing
    with open(input_path, "r", encoding="utf-8") as fin, \
            open(output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            for k in list(row.keys()):
                if k.startswith("dois_normalized"):
                    row.pop(k, None)
            raw_dois = row.get("dois_mentioned", [])
            targets = raw_dois if isinstance(raw_dois, list) else [raw_dois]
            cleaned = []

            for d in targets:
                nd = normalize(d)
                if nd:
                    cleaned.append(nd)
            row["dois_mentioned"] = sorted(set(cleaned))
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
    print("Done. DOI normalization finished.")