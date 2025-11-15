"""Download files from the ESA CryoSat-2 Science Server via FTP with parallel processing and connection management."""

from ftplib import FTP
from getpass import getpass
import os
import platform
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
import psutil
from pathlib import Path
from datetime import datetime
import re
from calendar import monthrange
import time
from queue import Queue
import threading

# Global lock for thread-safe printing
print_lock = Lock()

# Configuration
DOWNLOAD_DIR = r"D:\phd\data\CS2_SIR_SAR_L2E_ANTARC_202008_202509"
CHUNK_SIZE = 8192 * 1024  # 8 MB chunks
MAX_WORKERS = 3  # Reduced from 10 to avoid "too many connections" error
MEMORY_THRESHOLD = 90  # Stop if memory usage exceeds this percentage
MAX_RETRIES = 3  # Number of retries for failed downloads
RETRY_DELAY = 5  # Seconds to wait before retry

# Date range configuration
START_DATE = datetime(2020, 8, 1)  # August 01, 2020
END_DATE = datetime(2025, 9, 30, 23, 59, 59)  # September 30, 2025

# FTP Server configuration
FTP_HOST = "science-pds.cryosat.esa.int"
FTP_BASE_PATH = "/SIR_SAR_L2"  # Base path for SIR SAR L2 data
FTP_TIMEOUT = 180  # 3 minutes timeout
CONNECTION_DELAY = 2  # Seconds to wait between creating new connections

# Global semaphore to limit concurrent FTP connections
ftp_semaphore = Semaphore(MAX_WORKERS)


class FTPConnectionPool:
    """Thread-safe FTP connection pool to manage connections efficiently."""
    
    def __init__(self, host, username, password, max_size=MAX_WORKERS):
        self.host = host
        self.username = username
        self.password = password
        self.max_size = max_size
        self.pool = Queue(maxsize=max_size)
        self.lock = Lock()
        self.created_connections = 0
        
    def get_connection(self):
        """Get an FTP connection from the pool or create a new one."""
        try:
            # Try to get existing connection from pool
            if not self.pool.empty():
                ftp = self.pool.get(block=False)
                # Test if connection is still alive
                try:
                    ftp.voidcmd("NOOP")
                    return ftp
                except:
                    # Connection is dead, create a new one
                    pass
        except:
            pass
        
        # Create new connection with rate limiting
        with self.lock:
            if self.created_connections > 0:
                time.sleep(CONNECTION_DELAY)  # Rate limit connection creation
            
            ftp = FTP(self.host, timeout=FTP_TIMEOUT)
            ftp.login(self.username, self.password)
            self.created_connections += 1
            
            with print_lock:
                print(f"[Connection] Created new FTP connection (Total: {self.created_connections})")
            
            return ftp
    
    def return_connection(self, ftp):
        """Return an FTP connection to the pool."""
        try:
            if self.pool.qsize() < self.max_size:
                self.pool.put(ftp, block=False)
            else:
                ftp.quit()
        except:
            try:
                ftp.quit()
            except:
                pass
    
    def close_all(self):
        """Close all connections in the pool."""
        while not self.pool.empty():
            try:
                ftp = self.pool.get(block=False)
                ftp.quit()
            except:
                pass


class DownloadStats:
    """Thread-safe download statistics tracker."""
    def __init__(self, total_files):
        self.lock = Lock()
        self.total_files = total_files
        self.completed = 0
        self.failed = 0
        self.skipped = 0
        self.total_bytes = 0

    def update(self, status, bytes_downloaded=0):
        with self.lock:
            if status == 'completed':
                self.completed += 1
                self.total_bytes += bytes_downloaded
            elif status == 'failed':
                self.failed += 1
            elif status == 'skipped':
                self.skipped += 1
                self.total_bytes += bytes_downloaded

    def get_progress(self):
        with self.lock:
            return self.completed, self.failed, self.skipped, self.total_bytes


def check_memory():
    """Check system memory usage."""
    memory = psutil.virtual_memory()
    return memory.percent


def print_memory_status():
    """Print current memory status."""
    memory = psutil.virtual_memory()
    with print_lock:
        print(f"\n[Memory] Used: {memory.percent:.1f}% | Available: {memory.available / (1024**3):.2f} GB | "
              f"Total: {memory.total / (1024**3):.2f} GB")


def get_padded_count(count: int, max_count: int) -> str:
    return str(count).zfill(len(str(max_count)))


def progress_bar(progress: int, total: int, prefix: str = "", size: int = 60) -> str:
    """Generate progress bar string."""
    if total != 0 and total > 0:
        x = int(size * progress / total)
        x_percent = int(100 * progress / total)
        return f" {prefix} [{'=' * x}{' ' * (size - x)}] {x_percent}%"
    return f" {prefix} [{'=' * size}] 100%"


def generate_month_folders(start_date: datetime, end_date: datetime) -> list[str]:
    """
    Generate list of year/month folder paths between start and end dates.
    Example: /SIR_SAR_L2/2020/08
    """
    folders = []
    current = start_date.replace(day=1)
    
    while current <= end_date:
        year = current.year
        month = current.month
        folder_path = f"{FTP_BASE_PATH}/{year}/{month:02d}"
        folders.append(folder_path)
        
        # Move to next month
        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)
    
    return folders


def list_nc_files_in_folder(ftp: FTP, folder_path: str) -> list[str]:
    """
    List all .nc files in a specific folder.
    """
    files = []
    
    try:
        items = []
        ftp.cwd(folder_path)
        ftp.retrlines('LIST', items.append)
        
        for item in items:
            parts = item.split()
            if len(parts) < 9:
                continue
            
            filename = parts[-1]
            is_dir = item.startswith('d')
            
            # Only get .nc files
            if not is_dir and filename.endswith('.nc'):
                full_path = f"{folder_path}/{filename}"
                files.append(full_path)
        
        with print_lock:
            print(f"[Scan] Found {len(files)} .nc files in {folder_path}")
        
    except Exception as e:
        with print_lock:
            print(f"[Warning] Could not access {folder_path}: {e}")
    
    return files


def discover_files_from_ftp(username: str, password: str, start_date: datetime, end_date: datetime) -> list[str]:
    """
    Connect to FTP server and discover all .nc files within date range.
    Uses a single connection to avoid "too many connections" error.
    """
    print(f"\n{'='*80}")
    print(f"Discovering files on FTP server...")
    print(f"  - Server: {FTP_HOST}")
    print(f"  - Base Path: {FTP_BASE_PATH}")
    print(f"  - Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  - File Type: .nc only")
    print(f"{'='*80}\n")
    
    # Generate list of folders to scan
    folders_to_scan = generate_month_folders(start_date, end_date)
    print(f"[Status] Will scan {len(folders_to_scan)} month folders\n")
    
    all_files = []
    
    try:
        # Use single connection for discovery to avoid connection limit
        ftp = FTP(FTP_HOST, timeout=FTP_TIMEOUT)
        ftp.login(username, password)
        
        print("[Status] Connected to FTP server. Scanning month folders...\n")
        
        # Scan each month folder
        for idx, folder in enumerate(folders_to_scan, 1):
            with print_lock:
                print(f"[Progress] Scanning folder {idx}/{len(folders_to_scan)}: {folder}")
            
            files_in_folder = list_nc_files_in_folder(ftp, folder)
            all_files.extend(files_in_folder)
            
            # Small delay between folder scans to be gentle on the server
            time.sleep(0.5)
        
        ftp.quit()
        
        print(f"\n{'='*80}")
        print(f"Discovery Complete:")
        print(f"  - Folders scanned: {len(folders_to_scan)}")
        print(f"  - Total .nc files found: {len(all_files)}")
        print(f"{'='*80}\n")
        
        return all_files
        
    except Exception as e:
        print(f"\n[ERROR] Failed to discover files: {e}")
        return []


def download_single_file(connection_pool: FTPConnectionPool, remote_path: str, 
                         local_dir: str, file_num: int, total_files: int,
                         stats: DownloadStats) -> tuple:
    """Download a single file via FTP with retry mechanism."""
    filename = os.path.basename(remote_path)
    local_path = os.path.join(local_dir, filename)
    
    # Skip if file already exists
    if os.path.exists(local_path):
        file_size = os.path.getsize(local_path)
        stats.update('skipped', file_size)
        with print_lock:
            print(f"[{get_padded_count(file_num, total_files)}/{total_files}] Skipped (exists): {filename}")
        return True, filename, file_size
    
    # Check memory before downloading
    mem_usage = check_memory()
    if mem_usage > MEMORY_THRESHOLD:
        with print_lock:
            print(f"\n[WARNING] Memory usage {mem_usage:.1f}% exceeded {MEMORY_THRESHOLD}%. Skipping this file...")
            print_memory_status()
        stats.update('failed')
        return False, filename, 0
    
    # Retry logic
    for attempt in range(MAX_RETRIES):
        ftp = None
        bytes_downloaded = 0
        
        try:
            # Acquire semaphore to limit concurrent connections
            with ftp_semaphore:
                # Get connection from pool
                ftp = connection_pool.get_connection()
                
                # Get file size
                try:
                    file_size = ftp.size(remote_path)
                except:
                    file_size = 0
                
                if attempt == 0:  # Only print on first attempt
                    with print_lock:
                        size_mb = file_size / (1024**2) if file_size > 0 else 0
                        print(f"[{get_padded_count(file_num, total_files)}/{total_files}] Downloading: {filename} ({size_mb:.2f} MB)")
                
                # Download file with progress tracking
                with open(local_path, "wb") as f:
                    def callback(data):
                        nonlocal bytes_downloaded
                        f.write(data)
                        bytes_downloaded += len(data)
                        
                        # Update progress every 10 MB
                        if file_size > 0 and bytes_downloaded % (10 * 1024 * 1024) < CHUNK_SIZE:
                            progress = progress_bar(bytes_downloaded, file_size, filename[:40])
                            with print_lock:
                                print(f"\r{progress}", end='', flush=True)
                    
                    ftp.retrbinary(f"RETR {remote_path}", callback, CHUNK_SIZE)
                
                # Return connection to pool
                connection_pool.return_connection(ftp)
            
            stats.update('completed', bytes_downloaded)
            
            with print_lock:
                print(f"\n[{get_padded_count(file_num, total_files)}/{total_files}] ✓ Completed: {filename} "
                      f"({bytes_downloaded / (1024**2):.2f} MB)")
            
            return True, filename, bytes_downloaded
            
        except Exception as e:
            # Close failed connection
            if ftp:
                try:
                    ftp.quit()
                except:
                    pass
            
            # Clean up partial download
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except:
                    pass
            
            if attempt < MAX_RETRIES - 1:
                with print_lock:
                    print(f"\n[{get_padded_count(file_num, total_files)}/{total_files}] ⚠ Retry {attempt + 1}/{MAX_RETRIES}: {filename} - {str(e)}")
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                stats.update('failed')
                with print_lock:
                    print(f"\n[{get_padded_count(file_num, total_files)}/{total_files}] ✗ Failed after {MAX_RETRIES} attempts: {filename} - {str(e)}")
                return False, filename, 0
    
    return False, filename, 0


def download_files_parallel(username: str, password: str, file_list: list[str]) -> None:
    """Download files in parallel with connection pooling and memory monitoring."""
    
    # Create download directory
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    print(f"\n{'='*80}")
    print(f"Download Configuration:")
    print(f"  - Target Directory: {DOWNLOAD_DIR}")
    print(f"  - Total Files: {len(file_list)}")
    print(f"  - Parallel Workers: {MAX_WORKERS}")
    print(f"  - Max Retries: {MAX_RETRIES}")
    print(f"  - Chunk Size: {CHUNK_SIZE / (1024**2):.2f} MB")
    print(f"  - Memory Threshold: {MEMORY_THRESHOLD}%")
    print(f"  - FTP Timeout: {FTP_TIMEOUT}s")
    print(f"  - Connection Delay: {CONNECTION_DELAY}s")
    print(f"{'='*80}\n")
    
    print_memory_status()
    
    stats = DownloadStats(len(file_list))
    
    # Create connection pool
    connection_pool = FTPConnectionPool(FTP_HOST, username, password, MAX_WORKERS)
    
    try:
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all download tasks
            future_to_file = {
                executor.submit(
                    download_single_file, 
                    connection_pool,
                    file_path, 
                    DOWNLOAD_DIR,
                    idx + 1,
                    len(file_list),
                    stats
                ): file_path 
                for idx, file_path in enumerate(file_list)
            }
            
            # Process completed downloads
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success, filename, size = future.result()
                    
                    # Print periodic memory status
                    completed, failed, skipped, total_bytes = stats.get_progress()
                    if (completed + failed + skipped) % 20 == 0:
                        print_memory_status()
                        with print_lock:
                            print(f"\n[Progress] Completed: {completed}, Skipped: {skipped}, Failed: {failed}, "
                                  f"Downloaded: {total_bytes / (1024**3):.2f} GB\n")
                    
                except Exception as e:
                    with print_lock:
                        print(f"\n[Error] Unexpected error for {file_path}: {str(e)}")
    
    finally:
        # Clean up connection pool
        connection_pool.close_all()
    
    # Final summary
    completed, failed, skipped, total_bytes = stats.get_progress()
    print(f"\n{'='*80}")
    print(f"Download Summary:")
    print(f"  - Total Files: {len(file_list)}")
    print(f"  - Completed: {completed}")
    print(f"  - Skipped (already exist): {skipped}")
    print(f"  - Failed: {failed}")
    print(f"  - Total Downloaded: {total_bytes / (1024**3):.2f} GB")
    print(f"{'='*80}\n")
    
    print_memory_status()


def main() -> None:
    print("\n" + "="*80)
    print("ESA CryoSat-2 SIR_SAR_L2 Data Download Script")
    print(f"Date Range: {START_DATE.strftime('%B %Y')} to {END_DATE.strftime('%B %Y')}")
    print("="*80 + "\n")
    
    if int(platform.python_version_tuple()[0]) < 3:
        exit("Your Python version is {}. Please use version 3.0 or higher.".format(platform.python_version()))

    print("Access to the CryoSat-2 Science Server requires a personal ESA FTP username and password.")
    print("If you don't have these yet, please request them by reaching out to the ESA EO Help Desk at eohelp@esa.int.\n")

    esa_username = input("Please enter your ESA FTP username: ").strip()
    esa_password = getpass("Please enter your ESA FTP password: ")

    # Discover files from FTP server
    print("\n[Step 1/2] Discovering files on FTP server...")
    esa_files = discover_files_from_ftp(esa_username, esa_password, START_DATE, END_DATE)
    
    if len(esa_files) == 0:
        print("\n[ERROR] No .nc files found within the specified date range. Exiting.")
        return
    
    # Download files
    print("\n[Step 2/2] Starting parallel download process...")
    download_files_parallel(esa_username, esa_password, esa_files)
    
    print("\n[SUCCESS] Download process completed!")


if __name__ == "__main__":
    # Check for required package
    try:
        import psutil
    except ImportError:
        print("\n[ERROR] psutil package is required for memory monitoring.")
        print("Please install it using: pip install psutil\n")
        sys.exit(1)
    
    main()
