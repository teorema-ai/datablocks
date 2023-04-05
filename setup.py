import logging
import os
import setuptools


def readlines(fname: str) -> list:
    """Load a local filename."""
    lines = open(os.path.join(os.path.dirname(__file__), fname)).readlines()
    return [line.strip() for line in lines]

def get_requirements():
    requirements = '\n'.join(readlines('requirements.txt'))
    return requirements
    

setuptools.setup(
    name="datablocks",
    version="0.0.1",
    author="Dmitry Karpeyev",
    author_email="dmitry.karpeyev@gmail.com",
    description="Data experiment management hub",
    packages=setuptools.find_packages(exclude=("test,")),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    entry_points={'console_scripts': ['datablocks.exec=datablocks:exec',
                                      'datablocks.run=datablocks:run']},
    python_requires='>=3.8',
    install_requires=get_requirements(),
)
