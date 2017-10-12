from setuptools import setup

setup(
    name='plnx-grabber',
    version='1.5.1', # MAJOR.MINOR
    description='Grabber of trade history from Poloniex exchange',
    url='https://github.com/polakowo/plnx-grabber',
    author='polakowo',
    license='GPL v3',
    packages=['plnxgrabber'],
    install_requires=['pandas', 'poloniex', 'pymongo', 'pytz', 'bson'],
    python_requires='>=3'
)
