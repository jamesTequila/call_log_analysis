import os
import shutil
import glob
from datetime import datetime

def cleanup_data_folder():
    """
    Move processed files from 'data/' to 'archive/'.
    Keeps persistent files like 'combined_call_logs.csv' and reference CSVs.
    """
    data_dir = 'data'
    archive_dir = 'archive'
    
    # Create archive if needed
    os.makedirs(archive_dir, exist_ok=True)
    
    # Specific patterns to archive (Weekly Raw Files)
    patterns = [
        "CallLogLastWeek_*.csv",
        "AbandonedCallslastweek*.csv",
        "AbandonedCallslastweek*.xlsx",
        "CallLogLastWeek_*.xlsx",
        "InboundCallsLastWeek_*.csv",
        "AgentPerformance_*.csv"
    ]
    
    moved_count = 0
    
    print(f"[{datetime.now()}] Starting cleanup of '{data_dir}'...")
    
    for pattern in patterns:
        full_pattern = os.path.join(data_dir, pattern)
        files = glob.glob(full_pattern)
        
        for f in files:
            try:
                # Construct destination path
                filename = os.path.basename(f)
                dest = os.path.join(archive_dir, filename)
                
                # If file exists in archive, maybe timestamp it?
                # For now, simplistic overwrite/move
                if os.path.exists(dest):
                    # Rename old one in archive?
                    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                    base, ext = os.path.splitext(filename)
                    new_dest = os.path.join(archive_dir, f"{base}_{timestamp}{ext}")
                    shutil.move(dest, new_dest)
                    
                shutil.move(f, dest)
                print(f"Archived: {filename}")
                moved_count += 1
            except Exception as e:
                print(f"Error archiving {f}: {e}")
                
    print(f"Cleanup complete. Moved {moved_count} files to '{archive_dir}'.")

if __name__ == "__main__":
    cleanup_data_folder()
