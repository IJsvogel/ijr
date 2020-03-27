from setuptools import setup


__version__ = '0.0.14'

setup(
    name='ijr',
    version=__version__,
    packages=['ijr'],
    url='https://github.com/IJsvogel/ijr',
    license='MIT',
    author='Herman Holterman',
    author_email='herman.holterman@gmail.com',
    description='IJsvogel Package',
    install_requires=['pymongo', 'google-cloud-pubsub']
)
