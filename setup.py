from os import path

from setuptools import setup, find_packages

with open(
    path.join(path.abspath(path.dirname(__file__)), "README"), encoding="utf-8"
) as f:
    README = f.read()
with open(
    path.join(path.abspath(path.dirname(__file__)), "VERSION"), encoding="utf-8"
) as f:
    version = f.read()

with open(
    path.join(path.abspath(path.dirname(__file__)), "README"), encoding="utf-8"
) as f:
    long_description = f.read()

setup(
    name="zaku",
    packages=find_packages(),
    install_requires=[
        "asyncio",
        "aiohttp",
        "aiohttp-cors",
        "killport",
        "msgpack",
        "params-proto",
        "numpy",
        "websockets",
    ],
    description="Zaku Task Queue for Machine Learning Workloads",
    long_description=README,
    author="Ge Yang<ge.ike.yang@gmail.com>",
    url="https://github.com/geyang/zaku",
    author_email="ge.ike.yang@gmail.com",
    package_data={"zaku": ["zaku", "zaku/*.*"]},
    version=version,
)
