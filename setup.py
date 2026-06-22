#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-referral-saasquatch',
    version='2.2.0',
      description='Singer.io tap for extracting data from the Referral SaaSquatch API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_referral_saasquatch'],
      install_requires=[
          'singer-python==6.8.0',
          'requests==2.32.5',
          'backoff==2.2.1',
      ],
      extras_require={
        'dev': [
            'pylint==4.0.5',
            'pytest==8.4.1'
        ]
      },
      entry_points='''
          [console_scripts]
          tap-referral-saasquatch=tap_referral_saasquatch:main
      ''',
      packages=find_packages(),
      package_data = {
          'tap_referral_saasquatch/schemas': [
              "referrals.json",
              "reward_balances.json",
              "users.json",
          ]
      },
      include_package_data=True,
)
