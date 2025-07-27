from setuptools import setup, find_packages
setup(
    name='MongoMessageMQ_Python',
    version='0.1.1',
    packages=find_packages(),
    author = "Bleak Prestiger",
    author_email = "bleakprestiger435@gmail.com",
    description="Message Queue Implementation in MongoDB.",
    long_description="""A robust, production-ready message queue/service bus implementation using MongoDB as the backing store."""
    )