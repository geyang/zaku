from os import path

from setuptools import setup, find_packages

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
        "killport",
        "msgpack",
        "params-proto",
        "numpy",
        "websockets",
        "aiohttp-cors",
        "pillow",
    ],
    description="long_description",
    long_description=long_description,
    author="Ge Yang<ge.ike.yang@gmail.com>",
    url="https://github.com/geyang/zaku",
    author_email="ge.ike.yang@gmail.com",
    package_data={"zaku": ["zaku", "zaku/*.*"]},
    version=version,
)
