"""Utility script to simplify converting a GCS path into a GCE image."""

import argparse
import dataclasses
import logging
import os
import subprocess
import time
from typing import Optional, Tuple, List, Dict, Any


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class VMConfig:
    name: str
    zone: str
    project: str
    machine_type: str = "n2-standard-8"


@dataclasses.dataclass
class DiskConfig:
    name: str
    size_gb: int
    type: str = "pd-balanced"


class GCSVMManager:
    def __init__(self, project: str, zone: str):
        self.project = project
        self.zone = zone

    def run_command(self, command: str) -> Tuple[str, str, int]:
        """
        Execute a shell command and capture its output.

        Args:
            command (str): The shell command to execute.

        Returns:
            Tuple[str, str, int]: A tuple containing the command's stdout, stderr, and return code.
        """
        logger.info(f"Running command: {command}")
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        output, error = process.communicate()
        return output.decode("utf-8"), error.decode("utf-8"), process.returncode

    def run_on_vm(
        self, vm_name: str, command: str, use_google_proxy: bool = False
    ) -> Tuple[str, str, int]:
        """
        Run a command on the specified VM.

        Args:
            vm_name (str): Name of the VM to run the command on.
            command (str): The command to run on the VM.
            use_google_proxy (bool): Whether to use the Google proxy for SSH.

        Returns:
            Tuple[str, str, int]: A tuple containing the command's stdout, stderr, and return code.
        """
        ssh_command = (
            f"gcloud compute ssh {vm_name} --zone={self.zone} --project={self.project}"
        )
        vm_command = f'{ssh_command} --command="{command}"'
        if use_google_proxy:
            vm_command += " -- -o ProxyCommand='corp-ssh-helper %h %p'"
        return self.run_command(vm_command)

    def get_bucket_size(self, gcs_path: str) -> int:
        """
        Get the size of a GCS bucket or path.

        Args:
            gcs_path (str): The GCS path to check.

        Returns:
            int: The size of the bucket or path in GB.

        Raises:
            Exception: If the command fails.
        """
        command = f"gsutil du -s {gcs_path}"
        output, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to get bucket size: {error}")
        size_bytes = int(output.split()[0])
        return (
            size_bytes // (1024 * 1024 * 1024) + 5
        )  # Convert to GB and add 5GB buffer

    def get_bucket_region(self, gcs_path: str) -> str:
        """
        Get the region of a GCS bucket.

        Args:
            gcs_path (str): The GCS path to check.

        Returns:
            str: The region of the bucket.

        Raises:
            Exception: If the command fails.
        """
        bucket_name = gcs_path.split("/")[2]
        command = f"gsutil ls -L -b gs://{bucket_name} | grep 'Location constraint:'"
        output, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to get bucket region: {error}")
        return output.split(":")[1].strip()

    def create_disk(self, **kwargs: Any) -> None:
        """
        Create a new disk.

        Args:
            **kwargs: Arbitrary keyword arguments.
                Required:
                    name (str): Name of the disk.
                    size_gb (int): Size of the disk in GB.
                Optional:
                    type (str): Type of the disk. Default is 'pd-balanced'.

        Raises:
            Exception: If the command fails.
        """
        disk_config = DiskConfig(**kwargs)
        command = f"""
        gcloud compute disks create {disk_config.name} \
        --zone={self.zone} --project={self.project} \
        --size={disk_config.size_gb}GB --type={disk_config.type}
        """
        _, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to create disk: {error}")

    def create_vm(self, **kwargs: Any) -> None:
        """
        Create a new VM.

        Args:
            **kwargs: Arbitrary keyword arguments.
                Required:
                    name (str): Name of the VM.
                Optional:
                    machine_type (str): Type of the machine. Default is 'n2-standard-8'.

        Raises:
            Exception: If the command fails.
        """
        vm_config = VMConfig(zone=self.zone, project=self.project, **kwargs)
        command = f"""
        gcloud compute instances create {vm_config.name} \
        --zone={vm_config.zone} --project={vm_config.project} \
        --machine-type={vm_config.machine_type}
        """
        _, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to create VM: {error}")

    def attach_disk(self, vm_name: str, disk_name: str) -> None:
        """
        Attach a disk to a VM.

        Args:
            vm_name (str): Name of the VM.
            disk_name (str): Name of the disk to attach.

        Raises:
            Exception: If the command fails.
        """
        command = f"""
        gcloud compute instances attach-disk {vm_name} \
        --disk={disk_name} --zone={self.zone} --project={self.project}
        """
        _, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to attach disk: {error}")

    def format_and_mount_disk(self, vm_name: str, use_google_proxy: bool) -> None:
        """
        Format and mount a disk on a VM.

        Args:
            vm_name (str): Name of the VM.
            use_google_proxy (bool): Whether to use the Google proxy for SSH.

        Raises:
            Exception: If any command fails.
        """
        commands: List[str] = [
            "sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb",
            "sudo mkdir -p /mnt/disks/persist",
            "sudo mount -o discard,defaults /dev/sdb /mnt/disks/persist",
            "sudo chmod a+w /mnt/disks/persist",
        ]
        for command in commands:
            _, error, rc = self.run_on_vm(vm_name, command, use_google_proxy)
            if rc != 0:
                raise Exception(f"Failed to format and mount disk: {error}")

    def download_from_gcs(
        self, vm_name: str, gcs_path: str, use_google_proxy: bool
    ) -> None:
        """
        Download data from GCS to a VM.

        Args:
            vm_name (str): Name of the VM.
            gcs_path (str): GCS path to download from.
            use_google_proxy (bool): Whether to use the Google proxy for SSH.

        Raises:
            Exception: If the download fails.
        """
        command = f"gsutil -m cp -R {gcs_path} /mnt/disks/persist/"
        ssh_command = (
            f"gcloud compute ssh {vm_name} --zone={self.zone} --project={self.project}"
        )
        full_command = f'{ssh_command} --command="{command}"'
        if use_google_proxy:
            full_command += " -- -o ProxyCommand='corp-ssh-helper %h %p'"

        process = subprocess.Popen(
            full_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )

        for line in process.stdout:
            logger.info(line.strip())

        process.wait()
        if process.returncode != 0:
            raise Exception(
                f"Failed to download from GCS. Return code: {process.returncode}"
            )

    def detach_disk(self, vm_name: str, disk_name: str) -> None:
        """
        Detach a disk from a VM.

        Args:
            vm_name (str): Name of the VM.
            disk_name (str): Name of the disk to detach.

        Raises:
            Exception: If the command fails.
        """
        command = f"""
        gcloud compute instances detach-disk {vm_name} \
        --disk={disk_name} --zone={self.zone} --project={self.project}
        """
        _, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to detach disk: {error}")

    def create_image(self, disk_name: str, image_name: str) -> None:
        """
        Create an image from a disk.

        Args:
            disk_name (str): Name of the source disk.
            image_name (str): Name for the new image.

        Raises:
            Exception: If the command fails.
        """
        command = f"""
        gcloud compute images create {image_name} \
        --source-disk={disk_name} --source-disk-zone={self.zone} \
        --project={self.project}
        """
        _, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to create image: {error}")

    def verify_image_exists(self, image_name: str) -> bool:
        """
        Verify if an image exists.

        Args:
            image_name (str): Name of the image to verify.

        Returns:
            bool: True if the image exists, False otherwise.
        """
        command = (
            f"gcloud compute images describe {image_name} --project={self.project}"
        )
        _, _, rc = self.run_command(command)
        return rc == 0

    def delete_vm(self, vm_name: str) -> None:
        """
        Delete a VM.

        Args:
            vm_name (str): Name of the VM to delete.

        Raises:
            Exception: If the command fails.
        """
        command = f"""
        gcloud compute instances delete {vm_name} \
        --zone={self.zone} --project={self.project} --quiet
        """
        _, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to delete VM: {error}")

    def delete_disk(self, disk_name: str) -> None:
        """
        Delete a disk.

        Args:
            disk_name (str): Name of the disk to delete.

        Raises:
            Exception: If the command fails.
        """
        command = f"""
        gcloud compute disks delete {disk_name} \
        --zone={self.zone} --project={self.project} --quiet
        """
        _, error, rc = self.run_command(command)
        if rc != 0:
            raise Exception(f"Failed to delete disk: {error}")


def create_image_from_gcs(args: argparse.ArgumentParser):
    """Create an image from a given GCS bucket."""
    manager = GCSVMManager(project=args.project, zone=args.zone)

    try:
        # Generate default names if not provided
        if not args.vm_name:
            args.vm_name = f"{os.getlogin()}-temp-vm-{int(time.time())}"
        if not args.disk_name:
            args.disk_name = f"{os.getlogin()}-temp-disk-{int(time.time())}"

        # Check if the specified args.zone matches the bucket's region
        bucket_region = manager.get_bucket_region(gcs_path=args.gcs_path)
        if bucket_region.lower() not in args.zone.lower():
            logger.warning(
                f"The specified args.zone ({args.zone}) is not in the same region as the bucket ({bucket_region}). "
                f"This may incur additional costs and increase transfer time."
            )

        logger.info("Calculating required disk size...")
        bucket_size = manager.get_bucket_size(gcs_path=args.gcs_path)

        if args.disk_size_gb and int(args.disk_size_gb) < bucket_size:
            raise ValueError(
                f"Provided disk size ({args.disk_size_gb}) is less than the esitmated bucket size ({bucket_size})."
            )
        else:
            user_confirm = input(
                f"Estimated required disk size: {bucket_size}GB. Proceed? (y/n): "
            )
            if user_confirm.lower() != "y":
                logger.info("Operation cancelled by user.")
                return
            args.disk_size_gb = bucket_size

        logger.info("Creating VM...")
        manager.create_vm(name=args.vm_name, machine_type=args.machine_type)

        logger.info("Creating disk...")
        manager.create_disk(name=args.disk_name, size_gb=args.disk_size_gb)

        logger.info("Attaching disk to VM...")
        manager.attach_disk(vm_name=args.vm_name, disk_name=args.disk_name)

        logger.info("Formatting and mounting disk...")
        manager.format_and_mount_disk(
            vm_name=args.vm_name, use_google_proxy=args.use_google_proxy
        )

        logger.info("Starting download from GCS...")
        manager.download_from_gcs(
            vm_name=args.vm_name,
            gcs_path=args.gcs_path,
            use_google_proxy=args.use_google_proxy,
        )
        logger.info("Quick wait to avoid any race conditions...")
        time.sleep(30)

        logger.info("Detaching disk...")
        manager.detach_disk(vm_name=args.vm_name, disk_name=args.disk_name)

        if args.image_name:
            logger.info(f"Creating image '{args.image_name}' from disk...")
            manager.create_image(disk_name=args.disk_name, image_name=args.image_name)

            logger.info(f"Verifying image '{args.image_name}' exists...")
            if manager.verify_image_exists(image_name=args.image_name):
                logger.info(f"Image '{args.image_name}' created successfully.")
            else:
                logger.error(
                    f"Failed to verify the existence of image '{args.image_name}'."
                )

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        logger.info("Cleaning up resources...")
        manager.delete_vm(vm_name=args.vm_name)
        manager.delete_disk(disk_name=args.disk_name)
