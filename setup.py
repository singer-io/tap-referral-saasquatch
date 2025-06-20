#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-referral-saasquatch',
      version='1.0.6',
      description='Singer.io tap for extracting data from the Referral SaaSquatch API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap-referral-saasquatch'],
      install_requires=[
          'singer-python==6.1.1',
          'requests==2.32.4',
          'backoff==2.2.1',
          'pytz==2025.2'
      ],
      extras_require={
        'dev': [
            'pylint==2.4.4',
        ]
      },
      entry_points='''
          [console_scripts]
          tap-referral-saasquatch=tap-referral-saasquatch:main
      ''',
      packages=['tap-referral-saasquatch'],
      package_data = {
          'tap-referral-saasquatch/schemas': [
              "referrals.json",
              "reward_balances.json",
              "users.json",
          ]
      },
      include_package_data=True,
)
