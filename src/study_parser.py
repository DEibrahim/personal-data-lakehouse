import re

LINE = re.compile(
    r"^(?P<topic>.+?)\s+(?P<m>\d+)\s*min(?:\s+(?P<note>.+))?$",
    re.I,
)
DONE = re.compile(r"^done\s+(?P<course>.+)$", re.I)
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def parse_study_notes(text):
    """Parse study sessions into (rows, bad).

    rows: (date, entry_no, topic, minutes, note)
    bad:  (line_number, line, reason)
    Lines starting with 'done' are completion markers, handled by parse_completions.
    """
    rows, bad, cur_date, entry_no = [], [], None, 0
    for n, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or DONE.match(line):
            continue
        m = LINE.match(line)
        if m:
            if not cur_date:
                bad.append((n, line, "no date above"))
                continue
            entry_no += 1
            rows.append((
                cur_date,
                entry_no,
                " ".join(m["topic"].split()),
                int(m["m"]),
                (m["note"] or "").strip(),
            ))
        elif DATE.fullmatch(line):
            cur_date = line
            entry_no = 0
        else:
            bad.append((n, line, "unrecognized"))
    return rows, bad


def parse_completions(text):
    """Return [(end_date, course)] from 'done <course>' lines."""
    out, cur_date = [], None
    for raw in text.splitlines():
        line = raw.strip()
        if DATE.fullmatch(line):
            cur_date = line
            continue
        m = DONE.match(line)
        if m and cur_date:
            out.append((cur_date, " ".join(m["course"].split())))
    return out
