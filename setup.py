import os

from setuptools import setup, find_packages


def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'easyjoblite/version.py')) as f:
        locals = {}
        exec (f.read(), locals)
        return locals['VERSION']
    raise RuntimeError('No version info found.')


setup(
    name='easyjoblite',
    version=get_version(),
    packages=find_packages(exclude=['tests', 'samples']),
    install_requires=[
        'kombu==3.0.35',
        'importlib',
        'click>=3.0.0',
        'PyYaml'
    ],
    url='',
    license='BSD',
    author='bkp',
    author_email='bibek.padhy@treebohotels.com',
    entry_points={
        'console_scripts': [
            'easyjoblite = easyjoblite.cli:main',

        ],
    },
    test_suite="tests",
    description='A lite weight job scheduler which supports both automated and manual retry.'
)
