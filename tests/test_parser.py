from src.parser import parse_notes


def test_basic_line():
    rows, bad = parse_notes("2026-10-21 Anterior A\nmachine chest press 50 kg 8 set 1\n")
    assert rows == [("2026-10-21", "Machine Chest Press", 1, 50.0, 8)]
    assert bad == []


def test_extra_spaces_and_case():
    rows, _ = parse_notes("2026-10-21 A\nMACHINE   chest press 50 kg 7 set 2\n")
    assert rows[0][1] == "Machine Chest Press"


def test_decimal_weight():
    rows, _ = parse_notes("2026-10-21 A\nleg curl 32.5 kg 10 set 1\n")
    assert rows[0][3] == 32.5


def test_missing_reps_is_skipped():
    rows, bad = parse_notes("2026-10-21 A\nincline machine chest press 20 kg set 1\n")
    assert rows == []
    assert bad[0][2] == "unrecognized"


def test_line_without_date_is_skipped():
    rows, bad = parse_notes("machine chest press 50 kg 8 set 1\n")
    assert rows == []
    assert bad[0][2] == "no date above"
