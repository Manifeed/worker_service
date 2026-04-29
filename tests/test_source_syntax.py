from pathlib import Path


def test_python_sources_are_syntax_valid() -> None:
    service_root = Path(__file__).resolve().parents[1]
    for path in service_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        compile(path.read_text(encoding="utf-8"), str(path), "exec")
