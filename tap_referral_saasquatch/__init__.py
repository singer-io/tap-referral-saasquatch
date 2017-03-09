#!/usr/bin/env python3

import datetime
import sys
import time

import backoff
import requests
import singer

from . import utils


BASE_URL = "https://app.referralsaasquatch.com/api/v1/{}"
CONFIG = {
    'api_key': None,
    'tenant_alias': None,
    'start_date': None,
}
STATE = {}
entity_export_types = {
    "users": "USER",
    "reward_balances": "REWARD_BALANCE",
    "referrals": "REFERRAL",
}

logger = singer.get_logger()
session = requests.Session()


def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def export_ready(export_id):
    url = BASE_URL.format(CONFIG['tenant_alias']) + "/export/{}".format(export_id)
    auth = ("", CONFIG['api_key'])
    headers = {'Content-Type': "application/json"}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    resp = requests.get(url, auth=auth, headers=headers)
    result = resp.json()
    return result['status'] == 'COMPLETED'


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
def request_export(entity):
    url = BASE_URL.format(CONFIG['tenant_alias']) + "/export"
    auth = ("", CONFIG['api_key'])
    headers = {'Content-Type': "application/json"}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    data = {
        "type": entity_export_types[entity],
        "format": "CSV",
        "name": "Stitch Streams {}:{}".format(entity, datetime.datetime.utcnow()),
        "params": {
            "createdOrUpdatedSince": get_start(entity),
        },
    }

    req = requests.Request('POST', url, auth=auth, headers=headers, json=data).prepare()
    logger.info("POST {} body={}".format(req.url, data))
    resp = session.send(req)
    if resp.status_code >= 400:
        logger.error("POST {} [{} - {}]".format(req.url, resp.status_code, resp.content))
        sys.exit(1)

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
    url = BASE_URL.format(CONFIG['tenant_alias']) + "/export/{}/download".format(export_id)
    auth = ("", CONFIG['api_key'])
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

    return utils.strftime(datetime.datetime.utcfromtimestamp(int(value) * 0.001))


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
    return value


def transform_row(entity, row):
    return {field: transform_field(entity, field, value) for field, value in row.items()}


def sync_entity(entity, key_properties):
    start_date = get_start(entity)
    logger.info("{}: Starting sync from {}".format(entity, start_date))

    schema = utils.load_schema(entity)
    singer.write_schema(entity, schema, key_properties)
    logger.info("{}: Sent schema".format(entity))

    logger.info("{}: Requesting export".format(entity))
    export_start = utils.strftime(datetime.datetime.utcnow())
    export_id = request_export(entity)

    logger.info("{}: Export ready".format(entity))

    rows = stream_export(entity, export_id)
    logger.info("{}: Got {} records".format(entity, len(rows)))

    for row in rows:
        transformed_row = transform_row(entity, row)
        singer.write_record(entity, transformed_row)

    utils.update_state(STATE, entity, export_start)
    singer.write_state(STATE)
    logger.info("{}: State synced to {}".format(entity, export_start))


def do_sync():
    logger.info("Starting Referral Saasquatch sync")

    sync_entity("users", "id")
    sync_entity("reward_balances", ["userId", "accountId"])
    sync_entity("referrals", "id")

    logger.info("Sync complete")


def main():
    args = utils.parse_args()

    config = utils.load_json(args.config)
    utils.check_config(config, ['api_key', 'tenant_alias', 'start_date'])
    CONFIG.update(config)

    if args.state:
        STATE.update(utils.load_json(args.state))

    do_sync()


if __name__ == '__main__':
    main()
