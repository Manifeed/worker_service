from pathlib import Path


MAX_PYTHON_LINES = 300


def test_worker_service_python_files_stay_small() -> None:
    root = Path(__file__).resolve().parents[1]
    oversized: list[str] = []
    for path in root.rglob("*.py"):
        if any(part in {"__pycache__", ".git", ".pytest_cache"} for part in path.parts):
            continue
        line_count = sum(1 for _ in path.open("r", encoding="utf-8"))
        if line_count > MAX_PYTHON_LINES:
            oversized.append(f"{path.relative_to(root)}:{line_count}")
    assert oversized == []
