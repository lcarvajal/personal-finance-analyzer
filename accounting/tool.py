import os
import send2trash
import accounting.constant as c

def send_to_trash(files):
    """Moves files to trash."""
    for file in files:
        file_path = c.TEMP_DIRECTORY_PATH + file

        # Check if the file exists before attempting to delete
        if os.path.exists(file_path):
            # Move the file to the trash
            send2trash.send2trash(file_path)
            print(f"File '{file_path}' moved to trash successfully.")
        else:
            print(f"File '{file_path}' does not exist.")