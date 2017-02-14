#!/usr/bin/env python3

import datetime
import time

import requests
import singer

from . import utils


CONFIG = {
    'base_url': "https://app.referralsaasquatch.com/api/v1/{}",
    'default_start_date': utils.strftime(datetime.datetime.utcnow() - datetime.timedelta(days=365)),

    # in config.json
    'api_key': None,
    'tenant_alias': None,
}
STATE = {}
entity_export_types = {
    "users": "USER",
    "reward_balances": "REWARD_BALANCE",
    "referrals": "REFERRAL",
}

logger = singer.get_logger()


def export_ready(export_id):
    url = CONFIG['base_url'].format(CONFIG['tenant_alias']) + "/export/{}".format(export_id)
    auth = ("", CONFIG['api_key'])
    headers = {'Content-Type': "application/json"}
    resp = requests.get(url, auth=auth, headers=headers)
    result = resp.json()
    return result['status'] == 'COMPLETED'


def request_export(entity):
    url = CONFIG['base_url'].format(CONFIG['tenant_alias']) + "/export"
    auth = ("", CONFIG['api_key'])
    headers = {'Content-Type': "application/json"}
    data = {
        "type": entity_export_types[entity],
        "format": "CSV",
        "name": "Stitch Streams {}:{}".format(entity, datetime.datetime.utcnow()),
        "params": {
            "createdOrUpdatedSince": STATE.get(entity, CONFIG['default_start_date']),
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
    url = CONFIG['base_url'].format(CONFIG['tenant_alias']) + "/export/{}/download".format(export_id)
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

    dt = datetime.datetime.utcfromtimestamp(int(value) * 0.001)
    return utils.strftime(dt)


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
    start_date = STATE.get(entity, CONFIG['default_start_date'])
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

    utils.update_state(STATE, "entity", export_start)
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
    CONFIG.update(utils.load_json(args.config))
    if args.state:
        STATE.update(utils.load_json(args.state))
    do_sync()


if __name__ == '__main__':
    main()
