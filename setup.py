from distutils.core import setup
import re
import typing


def get_version() -> str:
    with open("aiozk/__init__.py", "r") as f:
        return re.search(r"__version__\s*=\s*\"(\d+\.\d+\.\d+)\"", f.read())[1]


def get_install_requires() -> typing.List[str]:
    install_requires: typing.List[str] = []

    with open("requirements.txt", "r") as f:
        for line in f.readlines():
            line = line.strip()

            if line.startswith("#"):
                continue

            install_requires.append(line)

    return install_requires


setup(
    name="aiozk",
    version=get_version(),
    description="AsyncIO client for ZooKeeper",
    packages=["aiozk"],
    python_requires=">=3.6.0",
    install_requires=get_install_requires(),
)
