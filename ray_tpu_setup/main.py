#!/usr/bin/env python3
"""CLI for ray-tpu-setup."""

import argparse
import logging
from .tpu_setup import create_tpu_cluster
from .disk_manager import create_image_from_gcs
import sys
import subprocess


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_gcloud_installed() -> bool:
    """
    Check if the Google Cloud SDK (gcloud) is installed and accessible.

    Returns:
        bool: True if gcloud is installed and accessible, False otherwise.
    """
    logger.info("Checking if gcloud is installed...")
    try:
        subprocess.run(
            ["gcloud", "--version"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.info("gcloud is installed and accessible.")
        return True
    except subprocess.CalledProcessError:
        logger.error("gcloud is installed but returned a non-zero exit code.")
        return False
    except FileNotFoundError:
        logger.error("gcloud is not found in the system PATH.")
        return False


def main():
    if not check_gcloud_installed():
        logger.error(
            "Please install the Google Cloud SDK and ensure it's in your PATH."
        )
        logger.error(
            "Visit https://cloud.google.com/sdk/docs/install for installation instructions."
        )
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Ray TPU Setup CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # TPU setup command
    tpu_parser = subparsers.add_parser("setup", help="Set up a Ray cluster on TPU")
    tpu_parser.add_argument("name", help="Unique name used for TPU name and QR ID")
    tpu_parser.add_argument("--dockerfile", help="Path to the Dockerfile")
    tpu_parser.add_argument("--project", required=True, help="GCP project ID")
    tpu_parser.add_argument("--zone", required=True, help="GCP zone")
    tpu_parser.add_argument(
        "--accelerator_type", required=True, help="TPU accelerator type"
    )
    tpu_parser.add_argument("--version", required=True, help="TPU software version")
    tpu_parser.add_argument(
        "--use-google-proxy",
        action="store_true",
        help="Use Google corporate proxy for SSH connections",
    )
    tpu_parser.add_argument(
        "--image-name", help="Optional name of the image to create a disk from"
    )
    tpu_parser.add_argument(
        "--disk-name",
        help="Optional name for the disk to be created and attached to the TPU, OR the name of an existing disk.",
    )

    # Disk creation command
    disk_parser = subparsers.add_parser(
        "create-image", help="Create a GCE image from a GCS bucket"
    )
    disk_parser.add_argument("gcs_path", help="GCS path to download from")
    disk_parser.add_argument("--project", required=True, help="Google Cloud project ID")
    disk_parser.add_argument("--zone", required=True, help="Google Cloud zone")
    disk_parser.add_argument(
        "--image-name", required=True, help="Name for the image to create"
    )
    disk_parser.add_argument(
        "--machine-type", default="n2-standard-8", help="VM machine type"
    )
    disk_parser.add_argument(
        "--use-google-proxy",
        action="store_true",
        help="Use corp-ssh-helper for SSH connections",
    )
    disk_parser.add_argument(
        "--vm_name", required=False, help="Name of the VM to be created."
    )
    disk_parser.add_argument(
        "--disk_name", required=False, help="Name of the disk to be created."
    )
    disk_parser.add_argument(
        "--disk_size_gb", required=False, help="The size of the disk to be created."
    )

    args = parser.parse_args()

    if args.command == "setup":
        create_tpu_cluster(args)
    elif args.command == "create-image":
        create_image_from_gcs(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
