from setuptools import setup, find_packages

setup(
    name='plnx-grabber',
    version='1.2', # MAJOR.MINOR
    description='Grabber of trade history from Poloniex exchange',
    url='https://github.com/polakowo/plnx-grabber',
    author='polakowo',
    license='GPL v3',
    packages=find_packages(),
    install_requires=['numpy', 'pandas', 'poloniex', 'pymongo', 'pytz', 'bson'],
    python_requires='>=3'
)
