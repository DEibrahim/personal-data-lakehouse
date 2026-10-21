import re

LINE = re.compile(
    r"^(?P<ex>.+?)\s+(?P<w>\d+(?:\.\d+)?)\s*kg\s+(?P<r>\d+)\s+set\s*(?P<s>\d+)\s*$",
    re.I,
)
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_notes(text):
    """Parse free-text workout notes into (rows, bad).

    rows: (date, exercise, set_no, weight_kg, reps)
    bad:  (line_number, line, reason)
    """
    rows, bad, cur_date = [], [], None
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        m = LINE.match(line)
        if m:
            if not cur_date:
                bad.append((n, line, "no date above"))
                continue
            rows.append((
                cur_date,
                " ".join(m["ex"].split()).title(),
                int(m["s"]),
                float(m["w"]),
                int(m["r"]),
            ))
        elif DATE.search(line):
            cur_date = DATE.search(line).group()
        else:
            bad.append((n, line, "unrecognized"))
    return rows, bad
