from setuptools import setup, find_packages

setup(
    name="csvw-safe",
    version="0.1.0",
    author="Your Name / Org",
    author_email="dscc@example.com",
    description="Python library for CSVW-SAFE metadata generation, validation, and dummy data generation",
    long_description=open("README.md", "r", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/dsccadminch/csvw-safe-library/csvw-safe",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pandas>=1.3",
        "numpy>=1.21",
        "pyshacl>=0.17.2",
    ],
    entry_points={
        "console_scripts": [
            "csvw-make-metadata=scripts.make_metadata:main",
            "csvw-make-dummy=scripts.make_dummy:main",
            "csvw-validate=scripts.validate_metadata:main",
            "csvw-assert-structure=scripts.assert_structure:main",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)