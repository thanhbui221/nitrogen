import os
import zipfile
import shutil
from pathlib import Path

# Create a temporary directory for packaging
temp_dir = Path("temp_package")
temp_dir.mkdir(exist_ok=True)

# Copy src directory contents (without the src prefix)
src_dir = Path("src")
for item in src_dir.iterdir():
    if item.is_dir():
        shutil.copytree(item, temp_dir / item.name, dirs_exist_ok=True)
    else:
        shutil.copy2(item, temp_dir)

# Copy required packages from venv
site_packages = Path("venv/Lib/site-packages")
required_packages = [
    "python_dotenv",
    "dotenv",
    "yaml",
    "dataclasses_json",
    "marshmallow",
    "typing_inspect",
    "mypy_extensions"
]

for package in required_packages:
    # Handle both directory and single file packages
    package_path = site_packages / package
    if package_path.is_dir():
        shutil.copytree(package_path, temp_dir / package, dirs_exist_ok=True)
    else:
        py_file = site_packages / f"{package}.py"
        if py_file.exists():
            shutil.copy2(py_file, temp_dir)

# Create the zip file
with zipfile.ZipFile("deps.zip", "w", zipfile.ZIP_DEFLATED) as zf:
    for root, _, files in os.walk(temp_dir):
        for file in files:
            file_path = Path(root) / file
            arc_name = str(file_path.relative_to(temp_dir))
            zf.write(file_path, arc_name)

# Clean up
shutil.rmtree(temp_dir)

print("Created deps.zip with source code and dependencies") 