#!/usr/bin/env python3
"""A very simple script to create a TPU cluster and start a Ray cluster where worker 0 is the head node."""

import argparse
import subprocess
import time
import os
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    else:
        command = f"""
        gcloud compute tpus tpu-vm create {tpu_name} \
        --accelerator-type={accelerator_type} \
        --project={project} \
        --zone {zone} \
        --version {version}"""
    
    logger.info("Creating startup script...")
    if dockerfile:
        startup_script = create_startup_script(dockerfile)
        command += f"\ --metadata-from-file startup-script={startup_script}"

    output, error, returncode = run_command(command)
    if returncode != 0:
        logger.error(f"Error creating TPU pod: {error}")
        return False
    logger.info(f"TPU pod created successfully: {output}")
    return True

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

    sudo usermod -aG docker $USER
    sudo systemctl restart docker -f
    # Provides auth + access to GCR
    curl -fsSL "https://github.com/GoogleCloudPlatform/docker-credential-gcr/releases/download/v2.1.10/docker-credential-gcr_linux_amd64-2.1.10.tar.gz" | tar xz docker-credential-gcr && chmod +x docker-credential-gcr && sudo mv docker-credential-gcr /usr/bin/
    docker-credential-gcr configure-docker

    # Get the worker number
    WORKER_NUM=$(hostname | grep -o '[0-9]*$')
    echo $WORKER_NUM > /tmp/worker_num
    """

    if dockerfile:
        logger.info("Adding Dockerfile content to startup script...")
        script_content += f"""
        # Copy Dockerfile to the VM
        cat << EOF > /tmp/Dockerfile
        {dockerfile}
        EOF

        # Build Docker image
        docker build -t custom_image -f /tmp/Dockerfile .

        # Run Docker container
        docker run -d --name ray_container --network host custom_image
        """

    script_path = "/tmp/tpu_startup_script.sh"
    with open(script_path, "w") as f:
        f.write(script_content)
    logger.info(f"Startup script created at {script_path}")
    return script_path

def wait_for_tpu_and_setup_ray(tpu_name, project, zone, dockerfile, use_google_proxy: bool = False):
    logger.info("Waiting for TPU to be ready...")
    while True:
        command = f"gcloud compute tpus tpu-vm describe {tpu_name} --zone={zone} --project={project}"
        output, error, returncode = run_command(command)
        if "STATE: READY" in output:
            logger.info("TPU is ready.")
            break
        logger.info("TPU not ready yet, waiting 30 seconds...")
        time.sleep(30)

    proxy_command = " -- -o ProxyCommand='corp-ssh-helper %h %p'" if use_google_proxy else ""
    logger.info("Getting worker IP addresses...")
    command = f"gcloud compute tpus tpu-vm describe {tpu_name} --zone={zone} --project={project} --format='get(networkEndpoints[].ipAddress)'"
    output, error, returncode = run_command(command)
    if returncode != 0:
        logger.error(f"Error getting worker IPs: {error}")
        return False
    worker_ips = output.strip().split('\n')

    logger.info(f"Worker IPs: {worker_ips}")

    for i, ip in enumerate(worker_ips):
        logger.info(f"Starting Ray on worker {i}")
        if i == 0:
            command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker={i} --zone={zone} --command='"
            if dockerfile:
                command += "docker exec ray_container "
            command += "ray start --head --port=6379'{proxy_command}"
        else:
            command = f"gcloud compute tpus tpu-vm ssh {tpu_name} --worker={i} --zone={zone} --command='"
            if dockerfile:
                command += "docker exec ray_container "
            command += f"ray start --address={worker_ips[0]}:6379'{proxy_command}"

        output, error, returncode = run_command(command)
        if returncode != 0:
            logger.error(f"Error starting Ray on worker {i}: {error}")
            return False
        logger.info(f"Ray started successfully on worker {i}")

    return True

def main():
    if not check_gcloud_installed():
        logger.error("gcloud is not installed or not in the system PATH.")
        logger.error("Please install the Google Cloud SDK and ensure it's in your PATH.")
        logger.error("Visit https://cloud.google.com/sdk/docs/install for installation instructions.")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Create a TPU pod as a Ray cluster")
    parser.add_argument("name", help="Unique name used for TPU name and QR ID")
    parser.add_argument("--use_qr", action="store_true", help="Use Queued Resource")
    parser.add_argument("--dockerfile", help="Content of the Dockerfile")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--zone", required=True, help="GCP zone")
    parser.add_argument("--accelerator_type", required=True, help="TPU accelerator type")
    parser.add_argument("--version", required=True, help="TPU software version")
    parser.add_argument("--use-google-proxy", action="store_true", help="Use Google corporate proxy for SSH connections")

    args = parser.parse_args()

    logger.info("Starting TPU pod creation process...")
    success = create_tpu_pod(
        args.name,
        args.project,
        args.zone,
        args.accelerator_type,
        args.version,
        args.use_qr,
        args.dockerfile,
    )

    if success:
        logger.info(f"TPU pod '{args.name}' created successfully.")
        logger.info("Setting up Ray cluster...")
        success = wait_for_tpu_and_setup_ray(args.name, args.project, args.zone, args.dockerfile, args.use_google_proxy)
        if success:
            logger.info(f"Ray cluster on TPU pod '{args.name}' is set up and ready.")
        else:
            logger.error(f"Failed to set up Ray cluster on TPU pod '{args.name}'.")
    else:
        logger.error(f"Failed to create TPU pod '{args.name}'.")

if __name__ == "__main__":
    main()
