#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import time

import requests
import stitchstream

API_KEY = None
TENANT_ALIAS = None
BASE_URL = "https://app.referralsaasquatch.com/api/v1/{tenant_alias}"
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_START_DATE = datetime.datetime(2016, 1, 1).strftime(DATETIME_FMT)
PERSISTED_COUNT = 0

state = {
    "users": DEFAULT_START_DATE,
    "reward_balances": DEFAULT_START_DATE,
    "referrals": DEFAULT_START_DATE,
}

entity_export_types = {
    "users": "USER",
    "reward_balances": "REWARD_BALANCE",
    "referrals": "REFERRAL",
}

logger = stitchstream.get_logger()


def load_schema(entity):
    path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "tap_referral_saasquatch",
        "{}.json".format(entity))

    with open(path) as f:
        return json.load(f)


def export_ready(export_id):
    url = BASE_URL + "/export/{}".format(export_id)
    auth = ("", API_KEY)
    headers = {'Content-Type': "application/json"}
    resp = requests.get(url, auth=auth, headers=headers)
    result = resp.json()
    return result['status'] == 'COMPLETED'


def request_export(entity):
    url = BASE_URL + "/export"
    auth = ("", API_KEY)
    headers = {'Content-Type': "application/json"}
    data = {
        "type": entity_export_types[entity],
        "format": "CSV",
        "name": "Stitch Streams {}:{}".format(entity, datetime.datetime.utcnow()),
        "params": {
            "createdOrUpdatedSince": state[entity],
        },
    }

    resp = requests.post(url, auth=auth, headers=headers, json=data)
    result = resp.json()

    if 'id' in result:
        waited = 0
        while waited <= 3600:
            if export_ready(result['id']):
                return result['id']

            time.sleep(5)
            waited += 5

        raise Exception("{} export took over an hour to complete. Aborting."
                        .format(entity))

    else:
        raise Exception("Request to create {} export failed: {} - {}"
                        .format(entity, resp.status_code, resp.content))


def stream_export(entity, export_id):
    url = BASE_URL + "/export/{}/download".format(export_id)
    auth = ("", API_KEY)
    headers = {'Content-Type': "application/json"}
    resp = requests.get(url, auth=auth, headers=headers, stream=True)

    fields = None
    rows = []
    for line in resp.iter_lines():
        line = line.decode("utf-8")
        split = line.split(",")

        if not fields:
            fields = split
        else:
            row = dict(zip(fields, split))
            rows.append(row)

    return rows


def transform_timestamp(value):
    if not value:
        return None

    dt = datetime.datetime.utcfromtimestamp(int(value) * 0.001)
    return dt.strftime(DATETIME_FMT)


TRANSFORMS = {
    "users": {
        "dateCreated": transform_timestamp,
    },
    "reward_balances": {
        "amount": int,
    },
    "referrals": {
        "dateReferralStarted": transform_timestamp,
        "dateReferralPaid": transform_timestamp,
        "dateReferralEnded": transform_timestamp,
        "dateModerated": transform_timestamp,
    },
}


def transform_field(entity, field, value):
    if field in TRANSFORMS[entity]:
        return TRANSFORMS[entity][field](value)
    else:
        return value


def transform_row(entity, row):
    return {field: transform_field(entity, field, value)
            for field, value in row.items()}


def sync_entity(entity, key_properties):
    global PERSISTED_COUNT
    logger.info("{}: Starting sync from {}".format(entity, state[entity]))

    schema = load_schema(entity)
    stitchstream.write_schema(entity, schema, key_properties)
    logger.info("{}: Sent schema".format(entity))

    logger.info("{}: Requesting export".format(entity))
    export_start = datetime.datetime.utcnow().strftime(DATETIME_FMT)
    export_id = request_export(entity)

    logger.info("{}: Export ready".format(entity))

    rows = stream_export(entity, export_id)
    logger.info("{}: Got {} records".format(entity, len(rows)))

    for row in rows:
        transformed_row = transform_row(entity, row)
        stitchstream.write_record(entity, transformed_row)
        PERSISTED_COUNT += 1

    state[entity] = export_start
    stitchstream.write_state(state)
    logger.info("{}: State synced to {}".format(entity, export_start))


def do_sync():
    logger.info("Starting Referral Saasquatch sync")

    sync_entity("users", "id")
    sync_entity("reward_balances", ["userId", "accountId"])
    sync_entity("referrals", "id")

    logger.info("Completed Referral Saasquatch sync. {} rows synced in total"
                .format(PERSISTED_COUNT))


def main():
    global API_KEY
    global TENANT_ALIAS
    global BASE_URL

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    API_KEY = config['api_key']
    TENANT_ALIAS = config['tenant_alias']
    BASE_URL = BASE_URL.format(tenant_alias=TENANT_ALIAS)

    if args.state:
        logger.info("Loading state from " + args.state)
        with open(args.state) as f:
            data = json.load(f)

        state.update(data)
        logger.info("State loaded. {}".format(state))

    do_sync()


if __name__ == '__main__':
    main()
