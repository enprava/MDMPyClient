import io
import os
import re

from setuptools import find_packages
from setuptools import setup


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding='utf-8') as fd:
        return re.sub(text_type(r':[a-z]+:`~?(.*?)`'), text_type(r'``\1``'), fd.read())


setup(
    name="MDMPyClient",
    version="1.0.2",
    url="https://github.com/enprava/MDMPyClient",
    license='MIT',

    author="enprava",
    author_email="epradavazquez@gmail.com",

    description="Cliente para el Metadatamanager",
    long_description_content_type="text/markdown",
    long_description=read("README.md"),

    packages=find_packages(exclude=('tests',)),
    package_data={
        "": ["configuracion/*.yaml"],
    },

    install_requires=['requests==2.28.2', 'PyYAML==6.0', 'pandas==1.4.4', 'ckanapi==4.7', 'beautifulsoup4==4.11.1',
                      'selenium==4.4.0', 'deepl==1.9.0', 'ftfy==6.1.1'],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.10',
    ],
)
