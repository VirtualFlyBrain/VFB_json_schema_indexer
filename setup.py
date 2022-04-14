from setuptools import setup, find_packages

setup(
    name='vfb_json_schema_indexer',
    version='1.0.0',
    description='Solr based caching solution for the Virtual Flybrain json_schema project',
    url='https://github.com/VirtualFlyBrain/VFB_json_schema_indexer',

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Virtual Fly Brain',
        'License :: Apache License Version 2.0',
        'Programming Language :: Python :: 3.8',
    ],

    keywords='vfb_json, vfb_json_schema',

    packages=find_packages(),

    install_requires=['jsonschema', 'requests', 'vfb_connect', 'tqdm'],
)
