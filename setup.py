import setuptools

with open("keep/README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="repartition",
    version="0.0.1",
    author="Tristan Glatard, Timoth\'ee Gu\'edon, Val\'erie Hayot-Sasson",
    author_email="tristan.glatard@concordia.ca",
    description="A library to efficiently repartition large multidimensional arrays",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/big-data-lab-team/paper-repartition",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    entry_points = {
        'console_scripts': ['repartition=keep.repartition:main'],
    }
)
