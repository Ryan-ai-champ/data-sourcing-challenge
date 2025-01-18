from setuptools import setup, find_packages

setup(
    name="nasa-data-retrieval",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests>=2.25.1",
        "pandas>=1.2.0",
        "numpy>=1.19.0",
        "matplotlib>=3.3.0",
        "seaborn>=0.11.0",
        "pyyaml>=5.4.1",
    ],
    entry_points={
        "console_scripts": [
            "nasa-data=cli:main",
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="A tool for retrieving and analyzing NASA CME and GST data",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/nasa-data-retrieval",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    python_requires=">=3.8",
)

