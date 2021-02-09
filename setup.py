from setuptools import setup



__version__ = '0.0.20'

setup(
    name='ijr',
    version=__version__,
    packages=['ijr'],
    url='https://github.com/IJsvogel/ijr',
    license='MIT',
    author='IT team IJsvogel Retail',
    author_email='it@ijsvogelretail.nl',
    description='IJsvogel Package',
    install_requires=['pymongo', 'google-cloud-pubsub', 'google-cloud-secret-manager']
)
