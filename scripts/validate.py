"""Check that notebooks parse and repository metadata keys are unique."""
from pathlib import Path
import json

root = Path(__file__).resolve().parents[1]
for path in (root / "notebooks").glob("*.ipynb"):
    notebook = json.loads(path.read_text(encoding="utf-8"))
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            source = "".join(cell["source"])
            if source.startswith("%%configure"):
                json.loads(source.split("\n", 1)[1])
            else:
                compile(source, str(path), "exec")
metadata = json.loads((root / "metadata/examples.json").read_text())
for section, fields in {"connections": ("connection_id", "environment"), "datasets": ("dataset_id",)}.items():
    keys = [tuple(row[field] for field in fields) for row in metadata[section]]
    assert len(keys) == len(set(keys)), section
print("Notebook cells and metadata keys verified.")
