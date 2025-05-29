import os
import zipfile

def create_source_zip():
    """Create a zip file containing the source code for Spark distribution"""
    # Ensure we're in the project root directory
    if not os.path.exists('src'):
        raise ValueError("Please run this script from the project root directory")

    # Create zip file
    with zipfile.ZipFile('src.zip', 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the src directory
        for root, _, files in os.walk('src'):
            for file in files:
                if file.endswith('.py'):  # Only include Python files
                    file_path = os.path.join(root, file)
                    # Calculate path relative to src directory for proper import structure
                    arc_path = os.path.relpath(file_path)
                    zipf.write(file_path, arc_path)

if __name__ == '__main__':
    create_source_zip()
    print("Created src.zip for Spark distribution") 