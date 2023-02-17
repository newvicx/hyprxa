import re
from pathlib import Path

from setuptools import setup


def get_version(package):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    version = Path(package, "__version__.py").read_text()
    return re.search("__version__ = ['\"]([^'\"]+)['\"]", version).group(1)


def get_long_description():
    """
    Return the README.
    """
    long_description = ""
    return long_description
    with open("README.md", encoding="utf8") as f:
        long_description += f.read()
    long_description += "\n\n"
    with open("CHANGELOG.md", encoding="utf8") as f:
        long_description += f.read()
    return long_description


def get_packages(package):
    """
    Return root package and all sub-packages.
    """
    return [str(path.parent) for path in Path(package).glob("**/__init__.py")]

def get_requirements():
    """
    Return list of requirements
    """
    with open("requirements.txt", encoding="utf-8") as f:
        requirements = f.read()
    return requirements.splitlines()


setup(
    name="hyprxa",
    python_requires=">=3.10",
    version=get_version("hyprxa"),
    url="",
    project_urls={
        "Changelog": "",
        "Source": "",
    },
    license="MIT",
    description="PI Batch and PI Web API client tools",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Chris Newville",
    author_email="christopher.newville@abbvie.com",
    package_data={"hyprxa": ["py.typed"]},
    packages=get_packages("hyprxa"),
    include_package_data=True,
    zip_safe=False,
    #install_requires=get_requirements(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Internet :: WWW/HTTP",
        "Framework :: AsyncIO",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
