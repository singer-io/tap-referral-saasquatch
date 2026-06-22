# Changelog

## 2.2.0
  * Streams the credentials cannot access (403) are now excluded from the catalog during discovery instead of raising an error.
  * Added/updated unit tests for discovery access checks and sync/bookmark behavior.

## 2.1.0
  * Bumps singer-python, requests dependency [#23](https://github.com/singer-io/tap-referral-saasquatch/pull/23)