#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import logging.config
import os
import sys

import backoff
import dateutil.parser
import requests
import stitchstream


QUIET = False
API_KEY = None
ALIAS = None
BASE_URL = "https://app.referralsaasquatch.com/api/v1/{alias}"
GET_COUNT = 0
PERSIST_COUNT = 0
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

state = {
    "users": None,
    "accounts": None,
    "codes": None,
    "referrals": None,
    "balances": None,
    "rewards": None,
}

endpoints = {
    "users": "/users",
    "accounts": "/account/{account_id}",
    "codes": "/code/{code_id}",
    "referrals": "/referrals",
    "balances": "/reward/balance",
    "rewards": "/reward",
}

logging.config.fileConfig("/etc/stitch/logging.conf")
logger = logging.getLogger("stitch.streamer")
session = requests.Session()


def stream_state():
    if not QUIET:
        stitchstream.write_state(state)
    else:
        logger.debug("Stream state")


def stream_schema(entity, schema):
    if not QUIET:
        stitchstream.write_schema(entity, schema)
    else:
        logger.debug("Stream schema {}".format(entity))


def stream_records(entity, records):
    if not QUIET:
        stitchstream.write_records(entity, records)
    else:
        logger.debug("Stream records {} ({})".format(entity, len(records)))


def load_config(config_file):
    global API_KEY
    global ALIAS
    global BASE_URL

    with open(config_file) as f:
        config = json.load(f)

    API_KEY = config['api_key']
    ALIAS = config['tenant_alias']
    BASE_URL = BASE_URL.format(alias=ALIAS)

    logger.debug("Loaded config. alias={}".format(ALIAS))


def load_state(state_file):
    with open(state_file) as f:
        data = json.load(f)

    state.update(data)


def load_schema(entity):
    with open("stream_referral_saasquatch/{}.json".format(entity)):
        return json.load(f)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
def request(url, params=None):
    global GET_COUNT
    params = params or {}

    logger.debug("Making request: GET {} {}".format(url, params))
    response = session.get(url, params=params, auth=("notused", API_KEY))
    logger.debug("Got response code: {}".format(response.status_code))

    GET_COUNT += 1
    response.raise_for_status()
    return response


def get_url_and_params(entity, **kwargs):
    url = BASE_URL + endpoints[entity]
    params = {k: v for k, v in kwargs.items() if k not in url}
    url = url.format(**kwargs)
    return (url, params)


def get_paged_list(entity, **kwargs):
    url, params = get_url_and_params(entity, **kwargs)

    params['limit'] = 100
    params['offset'] = 0

    count = 0
    items = []
    while True:
        resp = request(url, params)
        data = resp.json()

        count += len(data[entity])
        items.extend(data[entity])
        if count >= data['count']:
            break
        else:
            params['offset'] += params['limit']

    return items


def get(entity, **kwargs):
    url, params = get_url_and_params(entity, **kwargs)
    resp = request(url, params)
    return resp.json()


def do_check():
    try:
        pass
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using {e.request.url}; "
                     "received status {e.response.status_code}: {e.response.test}".format(e=e))
        sys.exit(-1)


def transform_timestamp(timestamp, iso=True):
    dt = datetime.datetime.utcfromtimestamp(int(timestamp) * 0.001)
    if iso:
        dt = dt.strftime(DATETIME_FMT)

    return dt


def transform_array(values, schema):
    return [transform(value, schema) for value in values]


def transform_object(value, schema):
    return {field_name: transform(value[field_name], field_schema)
            for field_name, field_schema in schema.items()
            if field_name in value}


def transform(value, schema):
    if not value:
        if "null" in schema['type']:
            return value
        else:
            raise ValueError("Value is null and null is not all allowed type")

    if schema['type'] == "array" or "array" in schema['type']:
        return transform_array(value, schema['items'])

    if schema['type'] == "object" or "object" in schema['type']:
        return transform_object(value, schema['properties'])

    if "integer" in schema['type']:
        value = int(value)

    if "number" in schema['type']:
        value = float(value)

    if "format" in schema:
        if schema['format'] == "date-time":
            value = transform_timestamp(value)

    return value


def _flat(d, new_key, old_key, data_key):
    data = d.pop(old_key, {})
    d[new_key] = data[data_key]


def do_sync():
    schemas = {entity: load_schema(entity) for entity in endpoints.keys()}

    users = get_paged_list("users")
    account_ids = set()
    referral_codes = set()

    transformed_users = []
    for user in users:
        u = transform(user, schemas["users"])
        account_ids.add(u['accountId'])
        referral_codes.add(u['referralCode'])
        transformed_users.append(u)

    stream_schema("users", schemas["users"])
    stream_records("users", transformed_users)

    accounts = []
    balances = []
    rewards = []
    for account_id in account_ids:
        account = get("accounts", account_id=account_id)
        account['referral'] = account.get('referral', {}).get('code')
        account = transform(account, schemas["account"])
        accounts.append(account)

        account_balances = get("balances", accountId=account_id)
        balances.extend(transform(i, schema["balances"]) for i in account_balances)

        account_rewards = get("rewards", accountId=account_id)
        rewards.extend(transform(i, schema["rewards"]) for i in account_rewards)

    stream_schema("accounts", schema["accounts"])
    stream_records("accounts", accounts)

    stream_schema("balances", schema["balances"])
    stream_records("balances", balances)

    stream_schema("rewards", schema["rewards"])
    stream_records("rewards", rewards)

    codes = []
    for code_id in referral_codes:
        code = get("codes", code_id=code_id)
        code = transform(code, schemas["codes"])
        codes.append(code)

    stream_schema("codes", schema["codes"])
    stream_records("codes", codes)

    referrals = get_paged_list("referrals")
    transformed_referrals = []
    for referral in referrals:
        referral['referredUserId'] = referral.pop('referredUser')['id']
        referral['referrerUserId'] = referral.pop('referrerUser')['id']
        referral['referredRewardId'] = referral.pop('referredReward')['id']
        referral['referrerRewardId'] = referral.pop('referrerReward')['id']
        r = transform(referral, schema["referrals"])
        transformed_referrals.append(r)

    stream_schema("referrals", schema["referrals"])
    stream_records("referrals", transformed_referrals)


def main():
    global QUIET

    parser = argparse.ArgumentParser()
    parser.add_argument('func', choices=['check', 'sync'])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Sets the log level to DEBUG (default INFO)')
    parser.add_argument('-q', '--quiet', dest='quiet', action='store_true',
                        help='Do not output to stdout (no persisting)')
    parser.set_defaults(debug=False, quiet=False)
    args = parser.parse_args()

    QUIET = args.quiet

    if args.debug:
        logger.setLevel(logging.DEBUG)

    load_config(args.config)
    if args.state:
        logger.info("Loading state from " + args.state)
        load_state(args.state)

    if args.func == "check":
        do_check()
    else:
        do_sync()


if __name__ == '__main__':
    main()
