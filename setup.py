import setuptools

setuptools.setup(
    name="S3Streamer-dsubak", # Replace with your own username
    version="0.0.1",
    description="Fixed memory footprint streaming to S3",
    packages=setuptools.find_packages(exclude=['stream_to_s3.py']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)