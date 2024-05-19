from setuptools import setup, find_packages

setup(
    name='mgindb',
    version='0.1.0',
    packages=find_packages(),
    description='In-Memory, Schema-less and Limitless.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='MginDB',
    author_email='info@mgindb.com',
    url='https://mgindb.com',
    install_requires=[
        'click',
        'asyncio',
        'nest_asyncio',
        'websockets',
        'uuid',
        'croniter',
        'requests'
    ],
    entry_points={
        'console_scripts': [
            'mgindb=mgindb.cli:cli',
        ],
    },
    classifiers=[
        'Development Status :: 0.1.0 - Public Release',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ],
)
