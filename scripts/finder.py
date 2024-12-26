import os
import sys

def find_substring_in_files(substring):
    """
    Searches for a substring in all files within the current working directory and subdirectories.

    :param substring: The substring to search for.
    """
    current_directory = os.getcwd()

    for root, _, files in os.walk(current_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                    for line_number, line in enumerate(file, start=1):
                        if substring in line:
                            print(f"Found in {file_path} (Line {line_number}): {line.strip()}")
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

# Main execution
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py \"substring\"")
        sys.exit(1)

    substring_to_search = sys.argv[1]
    find_substring_in_files(substring_to_search)
