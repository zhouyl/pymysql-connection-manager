#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages

setup(
    name='pymysql_manager',
    version='0.0.1',
    description='pymysql connection & pool manager for python3',
    long_description=open('README.rst').read(),
    author='zhouyl',
    author_email='81438567@qq.com',
    license='MIT',
    packages=find_packages(),
    install_requires = [
        'PyMySQL>=0.8.0',
        'connection_pool>=0.0.1',
    ],
    url='https://github.com/zhouyl/pymysql-connection-manager',
    classifiers=[
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        "License :: OSI Approved :: MIT License",
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database :: Front-Ends',
    ],
    keywords=["mysql", "pymysql", "mysql-connection", "mysql-pool", "pool", "connection-manager"]
)
