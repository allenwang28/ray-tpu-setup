#!/usr/bin/env python3
"""A simple script to create a TPU cluster and start a Ray cluster where worker 0 is the head node."""

from typing import Tuple, List, Optional, Any
import argparse
import subprocess
import shlex
import time
import os
import sys
import logging
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import tempfile
import base64
import re


# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class TPUPod:
    def __init__(self, name: str, project: str, zone: str, use_google_proxy: bool):
        self.name = name
        self.project = project
        self.zone = zone
        self.use_google_proxy = use_google_proxy
        self.worker_ips = []

    def run_command(self, command: str, timeout: int = 60) -> Tuple[str, str, int]:
        logger.info(f"Running command: {command}")
        try:
            result = subprocess.run(
                shlex.split(command), capture_output=True, text=True, timeout=timeout
            )
            return result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired:
            return "", "Command timed out", 1

    def ssh_command(
        self, worker: int, command: str, timeout: int = 60
    ) -> Tuple[str, str, int]:
        proxy_command = (
            " -- -o ProxyCommand='corp-ssh-helper %h %p'"
            if self.use_google_proxy
            else ""
        )
        full_command = f"gcloud compute tpus tpu-vm ssh {self.name} --worker={worker} --zone={self.zone} --project={self.project} --command='{command}'{proxy_command}"
        return self.run_command(full_command, timeout)

    def exists(self) -> bool:
        command = f"gcloud compute tpus tpu-vm describe {self.name} --project={self.project} --zone={self.zone}"
        _, _, returncode = self.run_command(command)
        return returncode == 0

    def get_worker_ips(self) -> List[str]:
        if self.worker_ips:
            return self.worker_ips
        command = f"gcloud compute tpus tpu-vm describe {self.name} --zone={self.zone} --project={self.project} --format='get(networkEndpoints[].ipAddress)'"
        output, err, returncode = self.run_command(command)
        if returncode != 0:
            logger.error(f"Error getting worker IPs ({err})")
            return []
        self.worker_ips = output.strip().split(";")
        return self.worker_ips

    def wait_until_ready(self):
        while True:
            command = f"gcloud compute tpus tpu-vm describe {self.name} --zone={self.zone} --project={self.project}"
            output, _, _ = self.run_command(command)
            if "state: READY" in output:
                logger.info("TPU is ready")
                break
            logger.info("Waiting for TPU to be ready...")
            time.sleep(30)

    def create(
        self,
        accelerator_type: str,
        version: str,
        dockerfile: Optional[str] = None,
        disk_name: Optional[str] = None,
    ) -> bool:
        command = f"gcloud compute tpus tpu-vm create {self.name} --accelerator-type={accelerator_type} --version={version} --zone={self.zone} --project={self.project}"
        if dockerfile:
            startup_script = create_startup_script(dockerfile, disk_name)
            command += f" --metadata-from-file='startup-script={startup_script}'"
        if disk_name:
            command += f" --data-disk source=projects/{self.project}/zones/{self.zone}/disks/{disk_name},mode=read-only"
        _, error, returncode = self.run_command(command)
        if returncode != 0:
            logger.error(f"Error creating TPU pod: {error}")
            return False
        logger.info(f"TPU pod '{self.name}' created successfully")
        return True

    def setup_ray(self, dockerfile: Optional[str] = None) -> bool:
        worker_ips = self.get_worker_ips()
        worker_count = len(worker_ips)

        if not dockerfile:
            logger.info("Installing Ray on all workers")
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                results = list(executor.map(self.install_ray, range(worker_count)))
            if not all(results):
                logger.error("Failed to install Ray on all workers")
                return False

        logger.info("Starting Ray on the head node (worker 0)")
        if not self.start_ray_on_worker(
            0, worker_ips[0], is_head_node=True, dockerfile=dockerfile
        ):
            logger.error("Failed to start Ray on the head node")
            return False

        if worker_count > 1:
            logger.info("Starting Ray on other workers")
            with ThreadPoolExecutor(max_workers=worker_count - 1) as executor:
                results = list(
                    executor.map(
                        lambda w: self.start_ray_on_worker(
                            w, worker_ips[0], is_head_node=False, dockerfile=dockerfile
                        ),
                        range(1, worker_count),
                    )
                )
            if not all(results):
                logger.error("Failed to start Ray on all workers")
                return False

        logger.info("Ray cluster setup completed successfully")
        return True

    def install_ray(self, worker: int) -> bool:
        logger.info(f"Installing Ray on worker {worker}")
        _, error, returncode = self.ssh_command(worker, "pip install 'ray[default]'")
        if returncode != 0:
            logger.error(f"Error installing Ray on worker {worker}: {error}")
            return False
        return True

    def start_ray_on_worker(
        self, worker: int, head_ip: str, is_head_node: bool, dockerfile: Optional[str]
    ) -> bool:
        logger.info(f"Starting Ray on worker {worker}")
        ray_command = (
            "sudo docker exec ray_container ray "
            if dockerfile
            else "/home/$(whoami)/.local/bin/ray "
        )
        ray_command += (
            "start --head --port=6379"
            if is_head_node
            else f"start --address={head_ip}:6379"
        )
        _, error, returncode = self.ssh_command(worker, ray_command)
        if returncode != 0:
            logger.error(f"Error starting Ray on worker {worker}: {error}")
            return False
        return True

    def attach_disk(self, disk_name: str) -> bool:
        logger.info(
            f"Checking if disk '{disk_name}' is already attached to TPU pod '{self.name}'"
        )

        # Check if disk is already attached
        describe_command = f"gcloud compute tpus tpu-vm describe {self.name} --project={self.project} --zone={self.zone} --format='value(dataDisks)'"
        output, error, returncode = self.run_command(describe_command)

        if returncode != 0:
            logger.error(f"Error checking disk attachment: {error}")
            return False

        if disk_name in output:
            logger.info(
                f"Disk '{disk_name}' is already attached to TPU pod '{self.name}'"
            )
            return True

        logger.info(f"Attaching disk '{disk_name}' to TPU pod '{self.name}'")
        command = f"gcloud alpha compute tpus tpu-vm attach-disk {self.name} --disk {disk_name} --mode read-only --project {self.project} --zone {self.zone}"
        _, error, returncode = self.run_command(command)
        if returncode != 0:
            logger.error(f"Error attaching disk: {error}")
            return False
        logger.info(
            f"Disk '{disk_name}' attached successfully to TPU pod '{self.name}'"
        )
        return True

    def setup_existing_pod(
        self, dockerfile: Optional[str] = None, disk_name: Optional[str] = None
    ) -> bool:
        logger.info(f"Setting up existing TPU pod '{self.name}'")

        if disk_name:
            if not self.attach_disk(disk_name):
                return False

        worker_count = len(self.get_worker_ips())

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_worker = {
                executor.submit(self.setup_worker, w, dockerfile, disk_name): w
                for w in range(worker_count)
            }

            for future in concurrent.futures.as_completed(future_to_worker):
                worker = future_to_worker[future]
                try:
                    success = future.result()
                    if not success:
                        logger.error(f"Failed to set up worker {worker}")
                        return False
                except Exception as exc:
                    logger.error(f"Worker {worker} generated an exception: {exc}")
                    return False

        logger.info("All workers set up successfully")
        return True

    def transfer_file(self, local_path: str, worker: int, remote_path: str) -> bool:
        logger.info(f"Transferring file to worker {worker}")

        filename = os.path.basename(local_path)
        proxy_flag = (
            '--scp-flag="-o ProxyCommand=/usr/bin/corp-ssh-helper %h %p"'
            if self.use_google_proxy
            else ""
        )

        command = f"gcloud compute tpus tpu-vm scp {proxy_flag} {filename} {self.name}:{remote_path} --worker={worker} --zone={self.zone} --project={self.project}"

        _, error, returncode = self.run_command(command)

        if returncode != 0:
            logger.error(f"Error transferring file to worker {worker}: {error}")
            return False

        logger.info(f"File transferred successfully to worker {worker}")
        return True

    def setup_worker(
        self, worker: int, dockerfile: Optional[str], disk_name: Optional[str]
    ) -> bool:
        logger.info(f"Setting up worker {worker}")

        # Mount disk if specified
        if disk_name:
            if not self.mount_disk(worker, disk_name):
                return False

        # Handle Dockerfile if specified
        if dockerfile:
            if not self.build_and_run_docker(worker, dockerfile, disk_name):
                return False

        return True

    def mount_disk(self, worker: int, disk_name: str) -> bool:
        logger.info(
            f"Checking if disk '{disk_name}' is already mounted on worker {worker}"
        )
        check_mount_cmd = "ls /mnt/disks/persist"
        output, error, returncode = self.ssh_command(worker, check_mount_cmd)
        if returncode == 0:
            logger.info(f"Disk '{disk_name}' is already mounted on worker {worker}")
            return True

        logger.info(f"Mounting disk '{disk_name}' on worker {worker}")
        mount_commands = [
            "sudo mkdir -p /mnt/disks/persist",
            "sudo mount -o ro,noload /dev/sdb /mnt/disks/persist",
        ]

        for command in mount_commands:
            _, error, returncode = self.ssh_command(worker, command)
            if returncode != 0:
                logger.error(f"Error mounting disk on worker {worker}: {error}")
                return False

        logger.info(f"Disk '{disk_name}' mounted successfully on worker {worker}")
        return True

    def build_and_run_docker(
        self, worker: int, dockerfile_path: str, disk_name: Optional[str] = None
    ) -> bool:
        logger.info(f"Building and running Docker container on worker {worker}")

        # Transfer Dockerfile
        if not self.transfer_file(dockerfile_path, worker, "~/Dockerfile"):
            logger.warning(f"Failed to transfer Dockerfile for worker {worker}.")
            return False

        # Build Docker image
        build_cmd = "sudo docker build -t ray_image -f ~/Dockerfile ."
        # Timeout after 5 min
        _, error, returncode = self.ssh_command(worker, build_cmd, timeout=300)
        if returncode != 0:
            logger.error(f"Error building Docker image on worker {worker}: {error}")
            return False

        # Run Docker container
        run_cmd = "sudo docker run -d --privileged --name ray_container --network host"
        if disk_name:
            run_cmd += " -v /mnt/disks/persist:/mnt/disks/persist"
        run_cmd += " ray_image"

        _, error, returncode = self.ssh_command(worker, run_cmd)
        if returncode != 0:
            logger.error(f"Error running Docker container on worker {worker}: {error}")
            return False

        logger.info(
            f"Docker container built and running successfully on worker {worker}"
        )
        return True


def create_startup_script(
    dockerfile: Optional[str] = None, disk_name: Optional[str] = None
) -> str:
    script_content = """
#!/bin/bash
sudo sed -i 's/#$nrconf{restart} = '"'"'i'"'"';/$nrconf{restart} = '"'"'a'"'"';/g' /etc/needrestart/needrestart.conf
if ! command -v docker &> /dev/null; then
    sudo apt-get update
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
fi
curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.10/docker-credential-gcr_linux_amd64-2.1.10.tar.gz" | tar xz docker-credential-gcr && chmod +x docker-credential-gcr && sudo mv docker-credential-gcr /usr/bin/
docker-credential-gcr configure-docker
    """
    if disk_name:
        script_content += """
sudo mkdir -p /mnt/disks/persist
sudo mount -o ro,noload /dev/sdb /mnt/disks/persist
sudo chmod a+r /mnt/disks/persist
        """
    if dockerfile:
        script_content += f"""
cat << 'EEOF' > /tmp/Dockerfile
{dockerfile.strip()}
EEOF
docker build -t ray_image -f /tmp/Dockerfile .
        """

        docker_run_command = "docker run -d"
        if disk_name:
            docker_run_command += " -v /mnt/disks/persist:/mnt/disks/persist"
        docker_run_command += (
            " --privileged --name ray_container --network host ray_image"
        )

        script_content += docker_run_command + "\n"
    else:
        script_content += "pip install 'ray[default]'"
    return script_content


def disk_exists(disk_name: str, project: str, zone: str) -> bool:
    """Check if a disk with the given name exists in the specified project and zone."""
    command = (
        f"gcloud compute disks describe {disk_name} --project={project} --zone={zone}"
    )
    process = subprocess.run(command.split(), capture_output=True, text=True)
    return process.returncode == 0


def create_disk_from_image(
    disk_name: str, image_name: str, project: str, zone: str
) -> bool:
    if disk_exists(disk_name, project, zone):
        logger.info(f"Disk '{disk_name}' already exists. Skipping disk creation.")
        return True
    else:
        logger.info(f"Disk '{disk_name}' does not exist. Creating now...")

    command = f"gcloud compute disks create {disk_name} --project={project} --zone={zone} --image={image_name} --type pd-balanced"
    process = subprocess.run(command.split(), capture_output=True, text=True)

    if process.returncode != 0:
        logger.error(f"Error creating disk: {process.stderr}")
        return False

    logger.info(f"Disk '{disk_name}' created successfully")
    return True


def setup_ray_tpu_cluster(args: argparse.Namespace):
    tpu_pod = TPUPod(args.name, args.project, args.zone, args.use_google_proxy)

    dockerfile_content = None
    if args.dockerfile:
        with open(args.dockerfile, "r") as f:
            dockerfile_content = f.read()

    if args.image_name:
        if not args.disk_name:
            args.disk_name = args.name
        if not create_disk_from_image(
            args.disk_name, args.image_name, args.project, args.zone
        ):
            logger.error(
                f"Failed to create disk '{args.disk_name}' from image '{args.image_name}'"
            )
            return

    if tpu_pod.exists():
        logger.info(f"TPU pod '{args.name}' already exists. Setting up existing pod.")
        tpu_pod.wait_until_ready()
        # Generate and run the startup script for the existing TPU
        success = tpu_pod.setup_existing_pod(args.dockerfile, args.disk_name)
        if success:
            success = tpu_pod.setup_ray(args.dockerfile)
        else:
            raise RuntimeError("Failed to set up the pod slice.")
    else:
        logger.info(f"Creating new TPU pod '{args.name}'.")
        success = tpu_pod.create(
            args.accelerator_type, args.version, dockerfile_content, args.disk_name
        )
        if success:
            tpu_pod.wait_until_ready()
            success = tpu_pod.setup_ray(dockerfile_content)

    if success:
        logger.info(f"Ray cluster on TPU pod '{args.name}' is set up and ready.")
    else:
        logger.error(f"Failed to set up Ray cluster on TPU pod '{args.name}'.")
