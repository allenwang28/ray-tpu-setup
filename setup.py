from setuptools import setup, find_packages

setup(
    name="ray-tpu-setup",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # Add any dependencies here
    ],
    entry_points={
        "console_scripts": [
            "ray-tpu-setup=ray_tpu_setup.main:main",
        ],
    },
    author="Allen Wang",
    author_email="allencwang@google.com",
    description="A tool to create Ray clusters on TPU pods",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/allenwang28/ray-tpu-cluster",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
