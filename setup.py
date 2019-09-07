# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from re import search
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# requirements
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.readlines()
    requirements = [package.strip() for package in requirements]

# version
with open(path.join(here, 'dsdbmanager', '__init__.py'), encoding='utf-8') as f:
    pattern = r"__version__ = '([0-9]\.[0-9]\.[0-9])'"
    try:
        version = search(pattern, f.read()).group(1)
    except AttributeError as _:
        version = '0.0.0'

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='dsdbmanager',
    version=version,
    description='Manage your data science databases',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jojoduquartier/dsdbmanager',
    author='jojoduquartier',
    author_email='',  # Optional

    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Database :: Database Engines/Servers',

        # Pick your license as you wish
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        # These classifiers are *not* checked by 'pip install'. See instead
        # 'python_requires' below.
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],

    keywords='sqlalchemy data-science database-connections pandas',  # Optional

    packages=find_packages(),
    python_requires='>=3.6, <4',

    install_requires=requirements,

    # List additional groups of dependencies here (e.g. development
    # dependencies). Users will be able to install these using the "extras"
    # syntax, for example:
    #
    #   $ pip install sampleproject[dev]
    #
    # Similar to `install_requires` above, these must be valid existing
    # projects.
    # extras_require={  # Optional
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },

    entry_points={
        'console_scripts': [
            'dsdbmanager=dsdbmanager.cli:main',
        ],
    },
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/jojoduquartier/dsdbmanager/issues',
        'Source': 'https://github.com/jojoduquartier/dsdbmanager/',
    },
)
