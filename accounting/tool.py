import os
import send2trash
import accounting.constant as c

def send_to_trash(files):
    """Moves files to trash."""
    for file in files:
        # Check if the file exists before attempting to delete
        if os.path.exists(file):
            # Move the file to the trash
            send2trash.send2trash(file)
            print(f"File '{file}' moved to trash successfully.")
        else:
            print(f"File '{file}' does not exist.")