#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Setup function for the package."""

from setuptools import setup, find_namespace_packages

setup(
  name='gbj_mqtt',
  version='1.0.1',
  description='Python package for module mqtt.',
  long_description='Processing MQTT messages by MQTT clients.',
  classifiers=[
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.8',
    'Topic :: System :: Monitoring',
  ],
  keywords='mqtt',
  url='http://github.com/mrkalePythonLib/gbj_mqtt',
  author='Libor Gabaj',
  author_email='libor.gabaj@gmail.com',
  license='MIT',
  packages=find_namespace_packages(),
  install_requires=['paho-mqtt'],
  include_package_data=True,
  zip_safe=False
)
