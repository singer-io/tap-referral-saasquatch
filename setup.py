#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-referral-saasquatch',
      version='0.5.5',
      description='Singer.io tap for extracting data from the Referral SaaSquatch API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_referral_saasquatch'],
      install_requires=[
          'singer-python>=0.1.0',
          'requests==2.12.4',
          'backoff==1.3.2',
      ],
      entry_points='''
          [console_scripts]
          tap-referral-saasquatch=tap_referral_saasquatch:main
      ''',
      packages=['tap_referral_saasquatch'],
      package_data = {
          'tap_referral_saasquatch/schemas': [
              "referrals.json",
              "reward_balances.json",
              "users.json",
          ]
      },
      include_package_data=True,
)
