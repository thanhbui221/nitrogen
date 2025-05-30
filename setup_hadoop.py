import os
import urllib.request
import zipfile
from pathlib import Path

# Create hadoop directories
hadoop_home = Path('venv/Lib/site-packages/pyspark/hadoop')
bin_dir = hadoop_home / 'bin'
bin_dir.mkdir(parents=True, exist_ok=True)

# Download winutils.exe
winutils_url = "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe"
winutils_path = bin_dir / "winutils.exe"

print(f"Downloading winutils.exe to {winutils_path}...")
urllib.request.urlretrieve(winutils_url, winutils_path)

print("Setup complete!")
print("\nPlease ensure your environment variables are set:")
print(f"HADOOP_HOME={hadoop_home.absolute()}")
print(f"PATH should include: {bin_dir.absolute()}") 