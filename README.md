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
- Create TPU pods with on-demand resources 
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
The `ray-tpu-setup` CLI supports two main commands:
1. `setup-disk`: For setting up a disk and/or GCE image from a GCS bucket.
2. `setup`: For creating and setting up a Ray cluster on TPU pods.


### Working with Persistent Disks and GCS buckets
The `ray-tpu-setup` script supports attaching existing persistent disks to your TPU pod. This is useful for storing and accessing large datasets / model checkpoints / preserving state between TPU pod recreations. Here is the workflow:

```
ray-tpu-setup setup-disk gs://your-bucket/path \
    --project your-project-id \
    --zone us-central1-a \
    --image-name your-custom-image
```

This image can then be used in the ray cluster setup.


### Setting up a Ray cluster on TPU
To set up a Ray cluster on TPUs, the CLI looks like this:

```
ray-tpu-setup setup name [options]
```

Options:
- `--dockerfile <path>`: Path to a Dockerfile for custom environments
- `--project <project-id>`: Google Cloud project ID (required)
- `--zone <zone>`: Google Cloud zone for the TPU (required)
- `--accelerator-type <type>`: TPU accelerator type (required)
- `--version <version>`: TPU software version (required)
- `--use-google-proxy`: for Googlers, use Google corp proxy for SSH connections
- `--image-name <image-name>`: Name of an existing GCE image, used to create a persistent disk.
- `--disk-name <disk-name>`: Either the name of an existing persistent disk, or the name of a persistent disk to be created using `--image-name`.


## End-to-end Example

Create an image for Llama 70b
```
ray-tpu-setup setup-disk gs://my-bucket/llama_ckpt/llama-2-70b \
    --project=my-project-id \
    --zone=us-central2-b \
    --image-name=llama2-70b
```

Create the Ray TPU cluster
```
ray-tpu-setup setup my-tpu-cluster \
    --project my-project-id \
    --zone us-central2-b \
    --accelerator_type v4-8 \
    --version tpu-ubuntu2204-base \
    --dockerfile Dockerfile \
    --disk-name my-data-disk \
    --image llama2-70b
```

# Troubleshooting

- Ensure that you have the necessary permissions in your Google Cloud project to create and manage TPU resources.
- If you encounter issues with gcloud not being found, make sure it's installed and added to your system PATH.
- For issues with the startup script, 
- For problems with Ray setup, check the Google Cloud Console logs for the TPU VMs for more detailed error messages.
- If you're using the `--use-google-proxy` option and encounter SSH issues, ensure that you're on the Google corporate network or VPN.


# Contributing
Contributions to `ray-tpu-setup` are welcome! Please feel free to submit pull requests or create issues for bugs and feature requests.


# License
This project is licensed under the MIT License - see the LICENSE file for detail.
