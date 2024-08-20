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
import re


# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_command_with_timeout(command: str, timeout: int = 60) -> Tuple[str, str, int]:
    """
    Execute a shell command with a specified timeout.

    Args:
        command (str): The shell command to execute.
        timeout (int): The maximum time in seconds to wait for the command to complete.

    Returns:
        Tuple[str, str, int]: A tuple containing the command's stdout, stderr, and return code.
    """
    try:
        result = subprocess.run(
            shlex.split(command), capture_output=True, text=True, timeout=timeout
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "Command timed out", 1


def ssh_to_tpu(
    tpu_name: str,
    worker: int,
    zone: str,
    project: str,
    command: str,
    use_google_proxy: bool = False,
    timeout: int = 60,
) -> Tuple[str, str, int]:
    """
    Execute a command on a TPU worker via SSH.

    Args:
        tpu_name (str): The name of the TPU.
        worker (int): The worker number to SSH into.
        zone (str): The GCP zone where the TPU is located.
        project (str): The GCP project ID.
        command (str): The command to execute on the TPU.
        use_google_proxy (bool): Whether to use the Google corporate proxy for SSH.
        timeout (int): The maximum time in seconds to wait for the SSH command to complete.

    Returns:
        Tuple[str, str, int]: A tuple containing the command's stdout, stderr, and return code.
    """
    proxy_command = (
        " -- -o ProxyCommand='corp-ssh-helper %h %p'" if use_google_proxy else ""
    )
    full_command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker={worker} --zone={zone} --project={project} --command='{command}'{proxy_command}"

    logger.info(f"Running SSH command: {full_command}")
    output, error, returncode = run_command_with_timeout(full_command, timeout)

    if returncode != 0 and "Command timed out" in error:
        logger.error(
            "SSH command timed out. If you're a Googler, make sure you're setting --use-google-proxy"
        )
        logger.error(
            "For non-Googlers, check your network connection and firewall settings"
        )

    return output, error, returncode


def run_command(command: str) -> Tuple[str, str, int]:
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


def create_startup_script(
    dockerfile: Optional[str] = None, disk_name: Optional[str] = None
) -> str:
    """
    Generate a startup script for the TPU, optionally including Docker setup and disk mounting.

    Args:
        dockerfile (Optional[str]): The content of the Dockerfile, if any.
        disk_name (Optional[str]): The name of the disk to mount, if any.

    Returns:
        str: The path to the generated startup script.
    """
    logger.info("Generating startup script content...")
    script_content = """
    #!/bin/bash
    # Don't stall on ubuntu graphic
    sudo sed -i 's/#$nrconf{restart} = '"'"'i'"'"';/$nrconf{restart} = '"'"'a'"'"';/g' /etc/needrestart/needrestart.conf

    if ! command -v docker &> /dev/null; then
        sudo apt-get update
        curl -fsSL https://get.docker.com -o get-docker.sh
        sudo sh get-docker.sh
    else
        echo "Docker is already installed."
    fi
    # Provides auth + access to GCR
    curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.10/docker-credential-gcr_linux_amd64-2.1.10.tar.gz" | tar xz docker-credential-gcr && chmod +x docker-credential-gcr && sudo mv docker-credential-gcr /usr/bin/
    echo "Configuring docker"
    docker-credential-gcr configure-docker
    echo "Finished configuring docker!"
    """

    if disk_name:
        script_content += f"""
# Mount the attached disk

echo "Mounting disk."
sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb
sudo mkdir -p /mnt/disks/persist
sudo mount -o discard,defaults /dev/sdb /mnt/disks/persist
sudo chmod a+r /mnt/disks/persist
"""

    if dockerfile:
        logger.info("Adding Dockerfile content to startup script...")
        if disk_name:
            docker_command = "docker run -d -v /mnt/disks/persist:/mnt/disks/persist --privileged --name ray_container --network host ray_image"
        else:
            docker_command = "docker run -d --privileged --name ray_container --network host ray_image"
        script_content += """
echo "Creating Dockerfile."
cat << 'EEOF' > /tmp/Dockerfile
{}
EEOF

echo "Building Docker image"
docker build -t ray_image -f /tmp/Dockerfile .

echo "Starting the Docker container"
""".format(dockerfile.strip())
        script_content += docker_command
    else:
        script_content += """
pip install "ray[default]"
"""

    script_path = "/tmp/tpu_startup_script.sh"
    with open(script_path, "w") as f:
        f.write(script_content)
    logger.info(f"Startup script created at {script_path}")
    return script_path


def create_disk_from_image(
    disk_name: str, image_name: str, project: str, zone: str
) -> bool:
    """
    Create a disk from an existing image.

    Args:
        disk_name (str): The name for the new disk.
        image_name (str): The name of the source image.
        project (str): The GCP project ID.
        zone (str): The GCP zone for the disk.

    Returns:
        bool: True if the disk was created successfully, False otherwise.
    """
    if disk_exists(disk_name, project, zone):
        logger.info(f"Disk '{disk_name}' already exists. Skipping disk creation.")
        return True
    logger.info(f"Creating disk '{disk_name}' from image '{image_name}'...")
    command = f"""
    gcloud compute disks create {disk_name} \
    --project={project} \
    --zone={zone} \
    --image={image_name}
    """
    output, error, returncode = run_command(command)
    if returncode != 0:
        logger.error(f"Error creating disk: {error}")
        return False
    logger.info(f"Disk '{disk_name}' created successfully")
    return True


def disk_exists(disk_name: str, project: str, zone: str) -> bool:
    """
    Check if a disk with the given name already exists in the specified project and zone.

    Args:
        disk_name (str): The name of the disk to check.
        project (str): The GCP project ID.
        zone (str): The GCP zone.

    Returns:
        bool: True if the disk exists, False otherwise.
    """
    command = (
        f"gcloud compute disks describe {disk_name} --project={project} --zone={zone}"
    )
    _, _, returncode = run_command(command)
    return returncode == 0


def create_tpu_pod(
    tpu_name: str,
    project: str,
    zone: str,
    accelerator_type: str,
    version: str,
    dockerfile: Optional[str] = None,
    disk_name: Optional[str] = None,
) -> bool:
    """
    Create a TPU pod with the specified configuration.

    Args:
        tpu_name (str): The name for the TPU pod.
        project (str): The GCP project ID.
        zone (str): The GCP zone for the TPU.
        accelerator_type (str): The TPU accelerator type.
        version (str): The TPU software version.
        dockerfile (Optional[str]): The content of the Dockerfile, if any.
        disk_name (Optional[str]): The name of the disk to attach, if any.

    Returns:
        bool: True if the TPU pod was created successfully, False otherwise.
    """
    logger.info(f"Creating TPU pod '{tpu_name}'...")
    command = f"""
    gcloud compute tpus tpu-vm create {tpu_name} \
    --accelerator-type={accelerator_type} \
    --project={project} \
    --zone {zone} \
    --version {version}"""
    if dockerfile:
        startup_script = create_startup_script(dockerfile, disk_name)
        command += f" --metadata-from-file='startup-script={startup_script}'"
    if disk_name:
        command += f" --data-disk source=projects/{project}/zones/{zone}/disks/{disk_name},mode=read-only"

    logger.info("Creating startup script...")

    output, error, returncode = run_command(command)
    if returncode != 0:
        logger.error(f"Error creating TPU pod: {error}")
        return False
    logger.info(f"TPU pod created successfully: {output}")
    return True


def stream_startup_logs(
    tpu_name: str, zone: str, project: str, use_google_proxy: bool, timeout: int = 600
) -> bool:
    """
    Stream and log the startup script execution from the TPU's worker 0.

    Args:
        tpu_name (str): The name of the TPU.
        zone (str): The GCP zone where the TPU is located.
        project (str): The GCP project ID.
        use_google_proxy (bool): Whether to use the Google corporate proxy for SSH.
        timeout (int): The maximum time in seconds to wait for the startup script to complete.

    Returns:
        bool: True if the startup script completed successfully, False otherwise.
    """
    logger.info("Streaming startup script logs from worker 0...")
    start_time = time.time()
    last_log_time = time.time()
    last_log_count = 0
    no_new_logs_count = 0

    while time.time() - start_time < timeout:
        command = f"sudo cat /var/log/syslog | grep startup-script"
        output, error, returncode = ssh_to_tpu(
            tpu_name, 0, zone, project, command, use_google_proxy, timeout=30
        )

        if returncode == 0:
            new_logs = output.splitlines()
            if len(new_logs) > last_log_count:
                for log in new_logs[last_log_count:]:
                    logger.info(f"Startup log: {log}")
                last_log_count = len(new_logs)
                last_log_time = time.time()
                no_new_logs_count = 0
            else:
                no_new_logs_count += 1

            # Check if no new logs for a while, assume completion
            if time.time() - last_log_time > 15:  # No new logs for 15s
                logger.info(
                    "No new startup logs for 15s. Assuming startup script completed."
                )
                return True

            # If we've seen no new logs for several checks, but it hasn't been a full minute, keep waiting
            if no_new_logs_count >= 5:
                logger.info("No new logs recently. Continuing to wait...")

        elif "Command timed out" in error:
            logger.warning("SSH command timed out while streaming logs. Retrying...")
        else:
            logger.error(f"Error streaming logs: {error}")

        time.sleep(10)  # Wait 10 seconds before next check

    logger.error("Timed out waiting for startup script to complete")
    return False


def start_ray_on_worker(
    tpu_name: str,
    worker_index: int,
    head_ip: str,
    zone: str,
    project: str,
    dockerfile: Optional[str],
    use_google_proxy: bool,
    is_head_node: bool,
) -> bool:
    """
    Start a Ray process on a specific TPU worker.

    Args:
        tpu_name (str): The name of the TPU.
        worker_index (int): The index of the worker to start Ray on.
        head_ip (str): The IP address of the Ray head node.
        zone (str): The GCP zone where the TPU is located.
        project (str): The GCP project ID.
        dockerfile (Optional[str]): The content of the Dockerfile, if any.
        use_google_proxy (bool): Whether to use the Google corporate proxy for SSH.
        is_head_node (bool): Whether this worker is the head node of the Ray cluster.

    Returns:
        bool: True if Ray was started successfully on the worker, False otherwise.
    """
    logger.info(f"Starting Ray on worker {worker_index}")

    if dockerfile:
        ray_command = "sudo docker exec ray_container ray "
    else:
        ray_command = "/home/$(whoami)/.local/bin/ray "

    if is_head_node:
        ray_command += "start --head --port=6379"
    else:
        ray_command += f"start --address={head_ip}:6379"

    output, error, returncode = ssh_to_tpu(
        tpu_name, worker_index, zone, project, ray_command, use_google_proxy
    )
    if returncode != 0:
        logger.error(f"Error starting Ray on worker {worker_index}: {error}")
        return False
    logger.info(f"Ray started successfully on worker {worker_index}")
    return True


def install_ray_on_worker(
    tpu_name: str, worker_index: int, zone: str, project: str, use_google_proxy: bool
) -> bool:
    """
    Install Ray on a specific TPU worker.

    Args:
        tpu_name (str): The name of the TPU.
        worker_index (int): The index of the worker to install Ray on.
        zone (str): The GCP zone where the TPU is located.
        project (str): The GCP project ID.
        use_google_proxy (bool): Whether to use the Google corporate proxy for SSH.

    Returns:
        bool: True if Ray was installed successfully on the worker, False otherwise.
    """
    logger.info(f"Installing ray on worker {worker_index}.")
    cmd = "pip install 'ray[default]'"
    output, error, returncode = ssh_to_tpu(
        tpu_name, worker_index, zone, project, cmd, use_google_proxy
    )
    if returncode != 0:
        logger.error(f"Error installing Ray on worker {worker_index}: {error}")
        return False
    logger.info(f"Ray installed successfully on worker {worker_index}")
    return True


def wait_for_tpu_and_setup_ray(
    tpu_name: str,
    project: str,
    zone: str,
    dockerfile: Optional[str],
    use_google_proxy: bool = False,
) -> bool:
    """
    Wait for the TPU to be ready and set up a Ray cluster on it.

    Args:
        tpu_name (str): The name of the TPU.
        project (str): The GCP project ID.
        zone (str): The GCP zone where the TPU is located.
        dockerfile (Optional[str]): The content of the Dockerfile, if any.
        use_google_proxy (bool): Whether to use the Google corporate proxy for SSH.

    Returns:
        bool: True if the Ray cluster was set up successfully, False otherwise.
    """
    logger.info("Waiting for TPU to be ready...")
    while True:
        command = f"gcloud compute tpus tpu-vm describe {tpu_name} --zone={zone} --project={project}"
        output, error, returncode = run_command(command)
        if returncode != 0:
            logger.error(f"Error describing TPU: {error}")
            return False

        # Extract the current state of the TPU
        state_match = re.search(r"state:\s+(\S+)", output, re.IGNORECASE)
        if state_match:
            current_state = state_match.group(1)
            if current_state.upper() == "READY":
                logger.info("TPU is ready.")
                break
            else:
                logger.info(
                    f"Current TPU state: {current_state}. Waiting 30 seconds..."
                )
        else:
            logger.warning(
                "Couldn't determine TPU state from output. Waiting 30 seconds..."
            )

        time.sleep(30)

    logger.info("Getting worker IP addresses...")
    output, error, returncode = run_command(
        f"gcloud compute tpus tpu-vm describe {tpu_name} --zone={zone} --project={project} --format='get(networkEndpoints[].ipAddress)'"
    )
    if returncode != 0:
        logger.error(f"Error getting worker IPs: {error}")
        return False
    worker_ips = output.strip().split(";")
    logger.info(f"Worker IPs: {worker_ips}")

    # Stream startup logs from worker 0
    if not stream_startup_logs(tpu_name, zone, project, use_google_proxy):
        logger.error("Startup script failed or timed out")
        return False

    if not dockerfile:
        # need to set up Ray manually
        logger.info("Setting up Ray on all workers.")
        with ThreadPoolExecutor(max_workers=len(worker_ips)) as executor:
            future_to_worker = {
                executor.submit(
                    install_ray_on_worker,
                    tpu_name,
                    i,
                    zone,
                    project,
                    use_google_proxy,
                ): i
                for i in range(0, len(worker_ips))
            }
            for future in concurrent.futures.as_completed(future_to_worker):
                worker = future_to_worker[future]
                if not future.result():
                    logger.error(f"Failed to install Ray on worker {worker}")
                    return False

    logger.info("Starting Ray on the head node (worker 0)")
    head_node_success = start_ray_on_worker(
        tpu_name,
        0,  # worker index for head node
        worker_ips[0],  # Head node IP
        zone,
        project,
        dockerfile,
        use_google_proxy,
        True,  # is_head_node
    )

    if not head_node_success:
        logger.error("Failed to start Ray on the head node")
        return False

    # Start Ray on other workers in parallel
    if len(worker_ips) > 1:
        logger.info(
            "Ray started successfully on the head node. Starting Ray on other workers."
        )
        with ThreadPoolExecutor(max_workers=len(worker_ips) - 1) as executor:
            future_to_worker = {
                executor.submit(
                    start_ray_on_worker,
                    tpu_name,
                    i,
                    worker_ips[0],  # Head node IP
                    zone,
                    project,
                    dockerfile,
                    use_google_proxy,
                    False,  # not head node
                ): i
                for i in range(1, len(worker_ips))  # Start from 1 to skip head node
            }

            for future in concurrent.futures.as_completed(future_to_worker):
                worker = future_to_worker[future]
                if not future.result():
                    logger.error(f"Failed to start Ray on worker {worker}")
                    return False

    logger.info("Ray cluster setup completed successfully")
    if dockerfile:
        ray_command = "sudo docker exec ray_container ray "
    else:
        ray_command = "/home/$(whoami)/.local/bin/ray "
    ray_command += "status"
    proxy_command = (
        " -- -o ProxyCommand='corp-ssh-helper %h %p'" if use_google_proxy else ""
    )
    full_command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker=0 --zone={zone} --project={project} --command='{ray_command}'{proxy_command}"
    logger.info(f"Confirm this yourself with: \n {full_command}")

    if dockerfile:
        full_command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker=0 --zone={zone} --project={project}{proxy_command}"
        logger.info(f"SSH to the machine with:\n{full_command}")
        logger.info("then: 'sudo docker exec -it ray_container /bin/bash'")
    else:
        full_command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker=0 --zone={zone} --project={project}{proxy_command}"
        logger.info(f"SSH to the machine with:\n{full_command}")
    full_command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker=0 --zone={zone} --project={project} --command='{ray_command}'{proxy_command}"
    return True


def create_tpu_cluster(args: argparse.ArgumentParser):
    """Create the Ray TPU cluster."""
    logger.info(f"Creating TPU cluster with args: {args}.")
    dockerfile_content = None
    if args.dockerfile:
        with open(args.dockerfile, "r") as f:
            dockerfile_content = f.read()

    if args.image_name:
        if not args.disk_name:
            args.disk_name = args.name
            logger.info(
                f"Disk name not provided for image creation, using {args.disk_name}."
            )
        if not create_disk_from_image(
            args.disk_name, args.image_name, args.project, args.zone
        ):
            logger.warning(
                f"Failed to create disk '{args.disk_name}' from image '{args.image_name}'"
            )
            sys.exit(1)

    success = create_tpu_pod(
        tpu_name=args.name,
        project=args.project,
        zone=args.zone,
        accelerator_type=args.accelerator_type,
        version=args.version,
        dockerfile=dockerfile_content,
        disk_name=args.disk_name,
    )

    if success:
        logger.info(f"TPU pod '{args.name}' created successfully.")
        wait_for_tpu_and_setup_ray(
            tpu_name=args.name,
            project=args.project,
            zone=args.zone,
            dockerfile=dockerfile_content,
            use_google_proxy=args.use_google_proxy,
        )
        if success:
            logger.info(f"Ray cluster on TPU pod '{args.name}' is set up and ready.")
        else:
            logger.error(f"Failed to set up Ray cluster on TPU pod '{args.name}'.")
    else:
        logger.error(f"Failed to create TPU pod '{args.name}'.")
