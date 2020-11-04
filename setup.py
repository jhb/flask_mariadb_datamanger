"""
see README.md
"""
from setuptools import setup
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()



setup(
    name='Flask-MariaDB-Datamanager',
    version='0.0 dev1',
    url='http://github.com/jhb/flask_mariadb_datamanger',
    license='GPLv3',
    author='Joerg Baach',
    author_email='flask@baach.de',
    description='mariadb datamanger for transaction in flask',
    long_description=long_description,
    long_description_content_type='text/markdown',
    # py_modules=['flask_transaction','filedm'],
    # if you would be using a package instead use packages instead
    # of py_modules:
    packages=['flask_mariadb_datamanager'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=[
        'Flask',
        'flask-transaction',
        'pytest',
        'mariadb'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GPL License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)