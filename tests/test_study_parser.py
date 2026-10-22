from src.study_parser import parse_study_notes, parse_completions


def test_basic_line_with_note():
    rows, bad = parse_study_notes("2026-10-22\nSQL 90 min window functions\n")
    assert rows == [("2026-10-22", 1, "SQL", 90, "window functions")]
    assert bad == []


def test_line_without_note():
    rows, _ = parse_study_notes("2026-10-22\nPython 45 min\n")
    assert rows == [("2026-10-22", 1, "Python", 45, "")]


def test_entry_no_resets_each_day():
    text = "2026-10-22\nSQL 30 min\nPython 20 min\n2026-10-23\nGit 15 min\n"
    rows, _ = parse_study_notes(text)
    assert [r[1] for r in rows] == [1, 2, 1]


def test_line_without_date_is_skipped():
    rows, bad = parse_study_notes("SQL 30 min\n")
    assert rows == []
    assert bad[0][2] == "no date above"


def test_unrecognized_line():
    rows, bad = parse_study_notes("2026-10-22\nstudied a lot today\n")
    assert rows == []
    assert bad[0][2] == "unrecognized"


def test_done_line_is_not_a_bad_line():
    rows, bad = parse_study_notes("2026-10-28\ndone Associate SQL in Data Engineer\n")
    assert rows == []
    assert bad == []


def test_completions():
    text = "2026-10-19\nSQL 30 min\n2026-10-28\ndone Associate SQL in Data Engineer\n"
    assert parse_completions(text) == [("2026-10-28", "Associate SQL in Data Engineer")]
