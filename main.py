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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from tqdm import tqdm  # For progress bar

# Default conversion settings
DEFAULT_CONFIG = {
    "jobs": 2,  # Number of parallel conversions
    "force_overwrite": False,  # Whether to overwrite existing PDFs
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


def check_dependencies():
    """
    Check if the required dependencies are installed.

    Returns:
        bool: True if all dependencies are available, False otherwise
    """
    if not shutil.which("ebook-convert"):
        print("Error: ebook-convert not found. Please install Calibre first.")
        print("Visit: https://calibre-ebook.com/download")
        return False
    return True


def convert_epub_to_pdf(epub_file, output_dir=None, overwrite=False, pdf_options=None):
    """
    Converts an EPUB file to PDF using Calibre's ebook-convert tool.

    Args:
        epub_file (str): Full path to the EPUB file
        output_dir (str, optional): Output directory for the PDF file
        overwrite (bool): Whether to overwrite existing PDF files
        pdf_options (dict, optional): Additional options for PDF conversion

    Returns:
        bool: True if conversion was successful, False otherwise
    """
    if not os.path.exists(epub_file):
        logging.error(f"File not found: {epub_file}")
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
        return True

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
        logging.info(f"Converting: {epub_file}")
        start_time = time.time()
        process = subprocess.run(cmd, capture_output=True, text=True, check=True)
        elapsed_time = time.time() - start_time

        logging.info(f"Converted successfully in {elapsed_time:.2f}s: {pdf_file}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error converting {epub_file}: {e}")
        logging.error(f"Output: {e.stdout}")
        logging.error(f"Error: {e.stderr}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error converting {epub_file}: {e}")
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
    if not check_dependencies():
        return 1

    # Load configuration
    config = load_config(args.config)

    # Override config with command-line arguments if provided
    if args.jobs is not None:
        config["jobs"] = args.jobs
    if args.force:
        config["force_overwrite"] = True

    root_directory = args.directory
    num_jobs = config["jobs"]

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

    # Convert files using multiple threads
    successes = 0
    failures = 0

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
                )
            )

        # Process results with progress bar
        for future in tqdm(futures, total=len(epub_files), desc="Converting"):
            if future.result():
                successes += 1
            else:
                failures += 1

    logging.info("\nSummary:")
    logging.info(f"Total files processed: {len(epub_files)}")
    logging.info(f"Successful conversions: {successes}")
    logging.info(f"Failed conversions: {failures}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
