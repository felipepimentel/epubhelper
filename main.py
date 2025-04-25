#!/usr/bin/env python3
"""
EPUB to PDF Batch Converter
--------------------------
This script recursively searches a directory and converts all EPUB files
to PDF, preserving the original directory structure.

Requirements:
- Python 3.6+
- Calibre (for the ebook-convert command)

Usage:
    python main.py [directory_path] [options]

If no directory is specified, the script will use the current directory.
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from tqdm import tqdm  # For progress bar

# Default conversion settings
DEFAULT_CONFIG = {
    "jobs": 2,  # Number of parallel conversions
    "force_overwrite": False,  # Whether to overwrite existing PDFs
    "max_retries": 3,  # Maximum number of retries per file
    "quarantine_dir": "_corrupted_epubs",  # Directory for corrupted files
    "pdf_options": {
        "margin-left": 72,  # 72pt = 1 inch
        "margin-right": 72,
        "margin-top": 72,
        "margin-bottom": 72,
        "pdf-default-font-size": 11,
        "pdf-mono-font-size": 9,
        "pdf-page-numbers": True,
        "pdf-page-margin-bottom": 36,
    },
}


class ConversionReport:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.corrupted_files = []
        self.failed_conversions = []
        self.successful_conversions = []
        self.report_file = os.path.join(base_dir, "conversion_report.txt")

    def add_corrupted(self, file_path, error):
        self.corrupted_files.append((file_path, str(error)))

    def add_failed(self, file_path, error):
        self.failed_conversions.append((file_path, str(error)))

    def add_success(self, file_path, time_taken):
        self.successful_conversions.append((file_path, time_taken))

    def save(self):
        with open(self.report_file, "w", encoding="utf-8") as f:
            f.write("EPUB to PDF Conversion Report\n")
            f.write("===========================\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("Summary\n")
            f.write("-------\n")
            f.write(
                f"Total files processed: {len(self.corrupted_files) + len(self.failed_conversions) + len(self.successful_conversions)}\n"
            )
            f.write(f"Successfully converted: {len(self.successful_conversions)}\n")
            f.write(f"Failed conversions: {len(self.failed_conversions)}\n")
            f.write(f"Corrupted files: {len(self.corrupted_files)}\n\n")

            if self.corrupted_files:
                f.write("Corrupted Files\n")
                f.write("--------------\n")
                for path, error in self.corrupted_files:
                    f.write(f"File: {path}\n")
                    f.write(f"Error: {error}\n\n")

            if self.failed_conversions:
                f.write("Failed Conversions\n")
                f.write("-----------------\n")
                for path, error in self.failed_conversions:
                    f.write(f"File: {path}\n")
                    f.write(f"Error: {error}\n\n")

            if self.successful_conversions:
                f.write("Successful Conversions\n")
                f.write("---------------------\n")
                for path, time_taken in self.successful_conversions:
                    f.write(f"File: {path}\n")
                    f.write(f"Time taken: {time_taken:.2f}s\n\n")

        logging.info(f"Conversion report saved to: {self.report_file}")


def move_to_quarantine(file_path, quarantine_dir):
    """
    Move a corrupted file to the quarantine directory.

    Args:
        file_path (str): Path to the corrupted file
        quarantine_dir (str): Path to the quarantine directory

    Returns:
        str: New path of the file in quarantine
    """
    if not os.path.exists(quarantine_dir):
        os.makedirs(quarantine_dir, exist_ok=True)

    # Create a unique filename to avoid conflicts
    base_name = os.path.basename(file_path)
    quarantine_path = os.path.join(quarantine_dir, base_name)

    # If file already exists in quarantine, add timestamp
    if os.path.exists(quarantine_path):
        name, ext = os.path.splitext(base_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantine_path = os.path.join(quarantine_dir, f"{name}_{timestamp}{ext}")

    shutil.move(file_path, quarantine_path)
    logging.info(f"Moved corrupted file to quarantine: {quarantine_path}")
    return quarantine_path


def is_valid_epub(epub_path):
    """
    Check if an EPUB file is a valid ZIP archive and has required EPUB structure.

    Args:
        epub_path (str): Path to the EPUB file

    Returns:
        tuple: (bool, str) - (is_valid, error_message)
    """
    try:
        with zipfile.ZipFile(epub_path, "r") as zf:
            # Check if the EPUB has the required files
            container = any("container.xml" in name.lower() for name in zf.namelist())
            opf = any(".opf" in name.lower() for name in zf.namelist())

            if not container:
                return False, "Missing container.xml"
            if not opf:
                return False, "Missing .opf file"

            return True, ""

    except zipfile.BadZipFile as e:
        return False, f"Invalid ZIP file: {str(e)}"
    except Exception as e:
        return False, f"Error checking EPUB: {str(e)}"


def convert_epub_to_pdf(
    epub_file,
    output_dir=None,
    overwrite=False,
    pdf_options=None,
    max_retries=3,
    report=None,
    quarantine_dir=None,
):
    """
    Converts an EPUB file to PDF using Calibre's ebook-convert tool.

    Args:
        epub_file (str): Full path to the EPUB file
        output_dir (str, optional): Output directory for the PDF file
        overwrite (bool): Whether to overwrite existing PDF files
        pdf_options (dict, optional): Additional options for PDF conversion
        max_retries (int): Maximum number of retry attempts
        report (ConversionReport): Report object to track conversion status
        quarantine_dir (str): Directory for corrupted files

    Returns:
        bool: True if conversion was successful, False otherwise
    """
    if not os.path.exists(epub_file):
        logging.error(f"File not found: {epub_file}")
        if report:
            report.add_failed(epub_file, "File not found")
        return False

    # Validate EPUB file
    is_valid, error_msg = is_valid_epub(epub_file)
    if not is_valid:
        logging.error(f"Invalid EPUB file: {epub_file} - {error_msg}")
        if report:
            report.add_corrupted(epub_file, error_msg)
        if quarantine_dir:
            move_to_quarantine(epub_file, quarantine_dir)
        return False

    # Define output directory (same as EPUB if not specified)
    if output_dir is None:
        output_dir = os.path.dirname(epub_file)

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Define PDF output filename
    base_name = os.path.splitext(os.path.basename(epub_file))[0]
    pdf_file = os.path.join(output_dir, f"{base_name}.pdf")

    # Check if PDF already exists
    if os.path.exists(pdf_file) and not overwrite:
        logging.info(f"PDF already exists, skipping: {pdf_file}")
        if report:
            report.add_success(epub_file, 0)
        return True

    # Set environment variables to avoid GPU/Vulkan issues
    env = os.environ.copy()
    env["QTWEBENGINE_DISABLE_SANDBOX"] = "1"
    env["QTWEBENGINE_CHROMIUM_FLAGS"] = "--disable-gpu"
    env["QT_QPA_PLATFORM"] = "offscreen"

    last_error = None
    for attempt in range(max_retries):
        try:
            # Build ebook-convert command with additional options
            cmd = ["ebook-convert", epub_file, pdf_file]

            # Add PDF options if provided
            if pdf_options:
                for option, value in pdf_options.items():
                    if isinstance(value, bool):
                        if value:
                            cmd.append(f"--{option}")
                    else:
                        cmd.extend([f"--{option}", str(value)])

            # Run ebook-convert
            logging.info(
                f"Converting: {epub_file} (attempt {attempt + 1}/{max_retries})"
            )
            start_time = time.time()
            process = subprocess.run(
                cmd, capture_output=True, text=True, check=True, env=env
            )
            elapsed_time = time.time() - start_time

            logging.info(f"Converted successfully in {elapsed_time:.2f}s: {pdf_file}")
            if report:
                report.add_success(epub_file, elapsed_time)
            return True

        except subprocess.CalledProcessError as e:
            last_error = f"Conversion error: {e}\nOutput: {e.stdout}\nError: {e.stderr}"
            logging.error(
                f"Error converting {epub_file} (attempt {attempt + 1}/{max_retries}): {e}"
            )
            logging.error(f"Output: {e.stdout}")
            logging.error(f"Error: {e.stderr}")

            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retrying
            else:
                if report:
                    report.add_failed(epub_file, last_error)
                return False

        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"
            logging.error(f"Unexpected error converting {epub_file}: {e}")
            if report:
                report.add_failed(epub_file, last_error)
            return False


def find_epubs(root_directory):
    """
    Recursively searches a directory and returns all EPUB files found.

    Args:
        root_directory (str): Root directory to start the search

    Returns:
        list: List of full paths to EPUB files
    """
    epub_files = []

    for root, dirs, files in os.walk(root_directory):
        for file in files:
            if file.lower().endswith(".epub"):
                full_path = os.path.join(root, file)
                epub_files.append(full_path)

    return epub_files


def setup_logging(log_file):
    """
    Set up logging configuration.

    Args:
        log_file (str): Path to the log file
    """
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )


def create_config_template(config_path):
    """
    Create a template configuration file.

    Args:
        config_path (str): Path where to save the config template
    """
    with open(config_path, "w") as f:
        json.dump(DEFAULT_CONFIG, f, indent=4)
    print(f"Configuration template created at: {config_path}")
    print("You can edit this file to customize the conversion settings.")


def load_config(config_path=None):
    """
    Load configuration from a JSON file or use defaults.

    Args:
        config_path (str, optional): Path to config file

    Returns:
        dict: Configuration settings
    """
    config = DEFAULT_CONFIG.copy()

    # If config file exists, load and merge with defaults
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                user_config = json.load(f)

            # Update PDF options
            if "pdf_options" in user_config:
                config["pdf_options"].update(user_config["pdf_options"])
                del user_config["pdf_options"]

            # Update other settings
            config.update(user_config)
            logging.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logging.error(f"Error loading config from {config_path}: {e}")

    return config


def main():
    # Configure argument parser
    parser = argparse.ArgumentParser(description="EPUB to PDF Batch Converter")
    parser.add_argument(
        "directory",
        nargs="?",
        default=os.getcwd(),
        help="Directory containing EPUB files (default: current directory)",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=f"Number of parallel conversions (default: {DEFAULT_CONFIG['jobs']})",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force overwrite existing PDF files",
    )
    parser.add_argument(
        "--config",
        help="Path to configuration file (JSON format)",
    )
    parser.add_argument(
        "--create-config",
        help="Create a template configuration file at the specified path",
    )
    parser.add_argument(
        "--log",
        help="Log file path (default: epub_conversion_<timestamp>.log)",
    )
    parser.add_argument(
        "--quarantine-dir",
        help="Directory for corrupted EPUB files",
    )
    args = parser.parse_args()

    # Create configuration template if requested
    if args.create_config:
        create_config_template(args.create_config)
        return 0

    # Set up logging
    log_file = (
        args.log
        if args.log
        else f"epub_conversion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    setup_logging(log_file)

    # Check dependencies
    if not shutil.which("ebook-convert"):
        logging.error("Error: ebook-convert not found. Please install Calibre first.")
        logging.error("Visit: https://calibre-ebook.com/download")
        return 1

    # Load configuration
    config = load_config(args.config)

    # Override config with command-line arguments if provided
    if args.jobs is not None:
        config["jobs"] = args.jobs
    if args.force:
        config["force_overwrite"] = True
    if args.quarantine_dir:
        config["quarantine_dir"] = args.quarantine_dir

    root_directory = args.directory
    num_jobs = config["jobs"]
    quarantine_dir = os.path.join(root_directory, config["quarantine_dir"])

    # Check if directory exists
    if not os.path.isdir(root_directory):
        logging.error(f"Error: {root_directory} is not a valid directory.")
        return 1

    # Find all EPUB files
    logging.info(f"Searching for EPUB files in {root_directory}...")
    epub_files = find_epubs(root_directory)

    if not epub_files:
        logging.info("No EPUB files found.")
        return 0

    logging.info(f"Found {len(epub_files)} EPUB files.")
    logging.info(f"Starting conversion with {num_jobs} parallel processes...")

    # Initialize conversion report
    report = ConversionReport(root_directory)

    # Convert files using multiple threads
    with ThreadPoolExecutor(max_workers=num_jobs) as executor:
        # Submit conversion tasks
        futures = []
        for epub_file in epub_files:
            futures.append(
                executor.submit(
                    convert_epub_to_pdf,
                    epub_file,
                    None,
                    config["force_overwrite"],
                    config["pdf_options"],
                    config["max_retries"],
                    report,
                    quarantine_dir,
                )
            )

        # Process results with progress bar
        for future in tqdm(futures, total=len(epub_files), desc="Converting"):
            future.result()  # Wait for completion

    # Save conversion report
    report.save()

    return 0


if __name__ == "__main__":
    sys.exit(main())
