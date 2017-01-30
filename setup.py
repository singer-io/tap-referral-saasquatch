#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name='stream-referral-saasquatch',
      version='0.1.0',
      description='Streams Referral SaaSquatch data',
      author='Stitch',
      url='https://github.com/stitchstreams/stream-referral-saasquatch',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['stream_referral_saasquatch'],
      install_requires=[
          'stitchstream-python>=0.4.1',
          'requests==2.12.4',
          'backoff==1.3.2',
          'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          stream-referral-saasquatch=stream_referral_saasquatch:main
      ''',
      packages=['stream_referral_saasquatch'],
      package_data = {
          'stream_referral_saasquatch': [
              "accounts.json",
              "balances.json",
              "codes.json",
              "referrals.json",
              "rewards.json",
              "users.json",
          ]
      }
)
