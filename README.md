# ray-tpu-setup

`ray-tpu-setup` is a command-line tool for creating and setting up Ray clusters on Google Cloud TPU pods. It simplifies the process of provisioning TPU resources and configuring them for distributed computing with Ray.


## What does this script do?

This script automates the process of setting up a Ray cluster on Google Cloud TPU pods. Here's a step-by-step breakdown of what it does:

### TPU Pod Creation:

Creates a TPU pod in your specified Google Cloud project and zone.
Can use either on-demand or queued resources based on your preference.


### Environment Setup:

Installs necessary software on each TPU VM, including Docker if not already present.
If a Dockerfile is provided, it builds a custom Docker image and starts a container on each VM.


### Ray Cluster Configuration:

Waits for all TPU VMs to be ready and accessible.
Starts a Ray head node on the first worker (worker 0).
Connects all other workers to the head node, forming a Ray cluster.


### Monitoring and Logging:

Provides detailed logs of the setup process.
Monitors the startup script execution and Docker container status.
Handles potential issues like SSH timeouts or Docker failures.


### Google-specific Features:

Includes an option for Google employees to use the corporate proxy for SSH connections.


The end result is a fully configured Ray cluster running on TPU pods, ready for distributed computing tasks. The script aims to make this process as smooth and error-free as possible, with ample logging to help troubleshoot any issues that may arise.

## Features
- Create TPU pods with on-demand or queued resources
- Set up Ray clusters automatically on TPU pods
- Support for custom Docker environments
- Attach and mount persistent disks to TPU pods
- Easy-to-use command-line interface

## Pre-requisites
- Python 3.8 or higher
- Google Cloud SDK (gcloud) installed and configured
- Access to Google Cloud TPU resources


## Installation
You can either `git clone` this repo and `pip install` it, or install it directly via:

```
pip install git+https://github.com/allenwang28/ray-tpu-setup.git
```


## Usage
The basic syntax for using `ray-tpu-setup` is:

```
ray-tpu-setup name [options]
```

Options:
- `--use-qr`: Use Queued Resource instead of on-demand TPU
- `--dockerfile <path>`: Path to a Dockerfile for custom environments
- `--project <project-id>`: Google Cloud project ID (required)
- `--zone <zone>`: Google Cloud zone for the TPU (required)
- `--accelerator-type <type>`: TPU accelerator type (required)
- `--version <version>`: TPU software version (required)
- `--use-google-proxy`: for Googlers, use Google corp proxy for SSH connections
- `--image-name <image-name>`: Name of an existing GCE image, used to create a persistent disk.
- `--disk-name <disk-name>`: Either the name of an existing persistent disk, or the name of a persistent disk to be created using `--image-name`.


## Example

```
ray-tpu-setup my-tpu-cluster \
    --project my-project-id \
    --zone us-central2-b \
    --accelerator_type v4-8 \
    --version tpu-ubuntu2204-base \
    --dockerfile Dockerfile \
    --disk-name my-data-disk \
    --image llama2-70b
```

# Working with Persistent Disks and GCS buckets
The `ray-tpu-setup` script supports attaching existing persistent disks to your TPU pod. This is useful for storing and accessing large datasets / model checkpoints / preserving state between TPU pod recreations. Here is the workflow:

1. *Create an image from GCS bucket data*: Use the disk.py script to create a GCE image from your GCS bucket. This script automates the process of turning your GCS bucket data into a disk image. Example:
```
python disk.py gs://your-bucket/path \
    --project your-project-id \
    --zone us-central1-a \
    --image-name your-custom-image
```
2. *Provide the image name to `ray-tpu-setup`*: When running `ray-tpu-setup`, use the `--image-name` option to specify the image you just created.
3. *Let `ray-tpu-setup` handle the rest*: The `ray-tpu-setup`` script will automatically:
- Create a persistent disk from the specified image
- Attach the disk to your TPU pod
- Mount the disk on all workers of the TPU pod at /mnt/disks/<disk-name>


# Troubleshooting

- Ensure that you have the necessary permissions in your Google Cloud project to create and manage TPU resources.
- If you encounter issues with gcloud not being found, make sure it's installed and added to your system PATH.
- For issues with the startup script, 
- For problems with Ray setup, check the Google Cloud Console logs for the TPU VMs for more detailed error messages.
- If you're using the `--use-google-proxy` option and encounter SSH issues, ensure that you're on the Google corporate network or VPN.


# Contributing
Contributions to `ray-tpu-setup` are welcome! Please feel free to submit pull requests or create issues for bugs and feature requests.


# License
This project is licensed under the MIT License - see the LICENSE file for detail