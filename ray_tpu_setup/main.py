#!/usr/bin/env python3
"""A simple script to create a TPU cluster and start a Ray cluster where worker 0 is the head node."""

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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_command_with_timeout(command, timeout=60):
    try:
        result = subprocess.run(shlex.split(command), capture_output=True, text=True, timeout=timeout)
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "Command timed out", 1


def ssh_to_tpu(tpu_name, worker, zone, project, command, use_google_proxy=False, timeout=60):
    proxy_command = " -- -o ProxyCommand='corp-ssh-helper %h %p'" if use_google_proxy else ""
    full_command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker={worker} --zone={zone} --project={project} --command='{command}'{proxy_command}"
    
    logger.info(f"Running SSH command: {full_command}")
    output, error, returncode = run_command_with_timeout(full_command, timeout)

    if returncode != 0 and "Command timed out" in error:
        logger.error("SSH command timed out. If you're a Googler, make sure you're setting --use-google-proxy")
        logger.error("For non-Googlers, check your network connection and firewall settings")
    
    return output, error, returncode


def run_command(command):
    logger.info(f"Running command: {command}")
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output.decode('utf-8'), error.decode('utf-8'), process.returncode


def check_gcloud_installed():
    logger.info("Checking if gcloud is installed...")
    try:
        subprocess.run(["gcloud", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info("gcloud is installed and accessible.")
        return True
    except subprocess.CalledProcessError:
        logger.error("gcloud is installed but returned a non-zero exit code.")
        return False
    except FileNotFoundError:
        logger.error("gcloud is not found in the system PATH.")
        return False


def create_startup_script(dockerfile=None):
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

    if dockerfile:
        logger.info("Adding Dockerfile content to startup script...")
        script_content += """
echo "Creating Dockerfile."
cat << 'EEOF' > /tmp/Dockerfile
{}
EEOF

echo "Building Docker image"
docker build -t ray_image -f /tmp/Dockerfile .

echo "Starting the Docker container"
docker run -d --privileged --name ray_container --network host ray_image
""".format(dockerfile.strip())
    else:
        script_content += """
pip install "ray[default]"
"""

    script_path = "/tmp/tpu_startup_script.sh"
    with open(script_path, "w") as f:
        f.write(script_content)
    logger.info(f"Startup script created at {script_path}")
    return script_path


def create_tpu_pod(tpu_name, project, zone, accelerator_type, version, use_qr=False, dockerfile=None):
    logger.info(f"Creating TPU pod '{tpu_name}'...")
    if use_qr:
        command = f"""
        gcloud compute tpus queued-resources create {tpu_name} \
        --node-id {tpu_name} \
        --project {project} \
        --zone {zone} \
        --accelerator-type {accelerator_type} \
        --runtime-version {version}"""
        if dockerfile:
            startup_script = create_startup_script(dockerfile)
            command += f" --metadata startup-script={startup_script}"
    else:
        command = f"""
        gcloud compute tpus tpu-vm create {tpu_name} \
        --accelerator-type={accelerator_type} \
        --project={project} \
        --zone {zone} \
        --version {version}"""
        if dockerfile:
            startup_script = create_startup_script(dockerfile)
            command += f" --metadata-from-file='startup-script={startup_script}'"
    
    logger.info("Creating startup script...")

    output, error, returncode = run_command(command)
    if returncode != 0:
        logger.error(f"Error creating TPU pod: {error}")
        return False
    logger.info(f"TPU pod created successfully: {output}")
    return True


def stream_startup_logs(tpu_name, zone, project, use_google_proxy, timeout=600):
    logger.info("Streaming startup script logs from worker 0...")
    start_time = time.time()
    last_log_time = time.time()
    last_log_count = 0
    no_new_logs_count = 0

    while time.time() - start_time < timeout:
        command = f"sudo cat /var/log/syslog | grep startup-script"
        output, error, returncode = ssh_to_tpu(tpu_name, 0, zone, project, command, use_google_proxy, timeout=30)
        
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
                logger.info("No new startup logs for 15s. Assuming startup script completed.")
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


def enable_docker_ssh(tpu_name, worker, zone, project, use_google_proxy):
    command = "sudo usermod -aG docker $USER && sudo systemctl restart docker -f"
    output, error, returncode = ssh_to_tpu(tpu_name, worker, zone, project, command, use_google_proxy)
    return True


def check_docker_started(tpu_name, worker, zone, project, use_google_proxy):
    command = "docker ps | grep ray_container"
    output, error, returncode = ssh_to_tpu(tpu_name, worker, zone, project, command, use_google_proxy)
    print(output, error, returncode)
    return returncode == 0 and "ray_container" in output


def start_ray_on_worker(tpu_name, worker_index, head_ip, zone, project, dockerfile, use_google_proxy, is_head_node):
    logger.info(f"Starting Ray on worker {worker_index}")
    
    if dockerfile:
        ray_command = "docker exec ray_container "
    else:
        ray_command = ""
    
    if is_head_node:
        ray_command += "ray start --head --port=6379"
    else:
        ray_command += f"ray start --address={head_ip}:6379"
    
    output, error, returncode = ssh_to_tpu(tpu_name, worker_index, zone, project, ray_command, use_google_proxy)
    if returncode != 0:
        logger.error(f"Error starting Ray on worker {worker_index}: {error}")
        return False
    logger.info(f"Ray started successfully on worker {worker_index}")
    return True

def wait_for_tpu_and_setup_ray(tpu_name, project, zone, dockerfile, use_google_proxy: bool = False):
    logger.info("Waiting for TPU to be ready...")
    while True:
        command = f"gcloud compute tpus tpu-vm describe {tpu_name} --zone={zone} --project={project}"
        output, error, returncode = run_command(command)
        if returncode != 0:
            logger.error(f"Error describing TPU: {error}")
            return False
        
        # Extract the current state of the TPU
        state_match = re.search(r'state:\s+(\S+)', output, re.IGNORECASE)
        if state_match:
            current_state = state_match.group(1)
            if current_state.upper() == "READY":
                logger.info("TPU is ready.")
                break
            else:
                logger.info(f"Current TPU state: {current_state}. Waiting 30 seconds...")
        else:
            logger.warning("Couldn't determine TPU state from output. Waiting 30 seconds...")
        
        time.sleep(30)

    logger.info("Getting worker IP addresses...")
    output, error, returncode = run_command(f"gcloud compute tpus tpu-vm describe {tpu_name} --zone={zone} --project={project} --format='get(networkEndpoints[].ipAddress)'")
    if returncode != 0:
        logger.error(f"Error getting worker IPs: {error}")
        return False
    worker_ips = output.strip().split(';')
    logger.info(f"Worker IPs: {worker_ips}")

    # Stream startup logs from worker 0
    if not stream_startup_logs(tpu_name, zone, project, use_google_proxy):
        logger.error("Startup script failed or timed out")
        return False

    # Enable docker access on the VMs
    with ThreadPoolExecutor(max_workers=len(worker_ips)) as executor:
        future_to_worker = {executor.submit(enable_docker_ssh, tpu_name, i, zone, project, use_google_proxy): i for i in range(len(worker_ips))}
        for future in concurrent.futures.as_completed(future_to_worker):
            worker = future_to_worker[future]
            if not future.result():
                logger.error(f"Could not enable docker permissions on worker {worker}")
                return False

    # Quick check if Docker started on all workers
    with ThreadPoolExecutor(max_workers=len(worker_ips)) as executor:
        future_to_worker = {executor.submit(check_docker_started, tpu_name, i, zone, project, use_google_proxy): i for i in range(len(worker_ips))}
        for future in concurrent.futures.as_completed(future_to_worker):
            worker = future_to_worker[future]
            if not future.result():
                logger.error(f"Docker container not started on worker {worker}")
                return False

    logger.info("Docker containers are ready on all workers")

    logger.info("Starting Ray on the head node (worker 0)")
    head_node_success = start_ray_on_worker(
        tpu_name,
        0,  # worker index for head node
        worker_ips[0],  # Head node IP
        zone,
        project,
        dockerfile,
        use_google_proxy,
        True  # is_head_node
    )

    if not head_node_success:
        logger.error("Failed to start Ray on the head node")
        return False

    # Start Ray on other workers in parallel
    if len(worker_ips) > 1:
        logger.info("Ray started successfully on the head node. Starting Ray on other workers.")
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
                    False  # not head node
                ): i for i in range(1, len(worker_ips))  # Start from 1 to skip head node
            }

            for future in concurrent.futures.as_completed(future_to_worker):
                worker = future_to_worker[future]
                if not future.result():
                    logger.error(f"Failed to start Ray on worker {worker}")
                    return False

    logger.info("Ray cluster setup completed successfully")
    ray_command = ""
    if dockerfile:
        ray_command = "docker exec ray_container "
    ray_command += "ray status"
    proxy_command = " -- -o ProxyCommand='corp-ssh-helper %h %p'" if use_google_proxy else ""
    full_command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker={worker} --zone={zone} --project={project} --command='{ray_command}'{proxy_command}"
    logger.info(f"Confirm this yourself with: \n {full_command}")
    return True


def main():
    if not check_gcloud_installed():
        logger.error("Please install the Google Cloud SDK and ensure it's in your PATH.")
        logger.error("Visit https://cloud.google.com/sdk/docs/install for installation instructions.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Create a TPU pod as a Ray cluster")
    parser.add_argument("name", help="Unique name used for TPU name and QR ID")
    parser.add_argument("--use_qr", action="store_true", help="Use Queued Resource")
    parser.add_argument("--dockerfile", help="Path to the Dockerfile")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--zone", required=True, help="GCP zone")
    parser.add_argument("--accelerator_type", required=True, help="TPU accelerator type")
    parser.add_argument("--version", required=True, help="TPU software version")
    parser.add_argument("--use-google-proxy", action="store_true", help="Use Google corporate proxy for SSH connections")

    args = parser.parse_args()

    dockerfile_content = None
    if args.dockerfile:
        with open(args.dockerfile, 'r') as f:
            dockerfile_content = f.read()

    success = create_tpu_pod(
        args.name,
        args.project,
        args.zone,
        args.accelerator_type,
        args.version,
        args.use_qr,
        dockerfile_content,
    )

    if success:
        logger.info(f"TPU pod '{args.name}' created successfully.")
        success = wait_for_tpu_and_setup_ray(args.name, args.project, args.zone, dockerfile_content, args.use_google_proxy)
        if success:
            logger.info(f"Ray cluster on TPU pod '{args.name}' is set up and ready.")
        else:
            logger.error(f"Failed to set up Ray cluster on TPU pod '{args.name}'.")
    else:
        logger.error(f"Failed to create TPU pod '{args.name}'.")


if __name__ == "__main__":
    main()