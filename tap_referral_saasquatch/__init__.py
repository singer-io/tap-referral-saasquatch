#!/usr/bin/env python3

import datetime
import os
import sys
import time
import pytz

import backoff
import requests
import singer
import csv

from singer import utils
import signal
signal.signal(signal.SIGPIPE, signal.SIG_DFL)


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

ITER_CHUNK_SIZE = 512

def get_start(entity):
    if entity not in STATE:
        STATE[entity] = CONFIG['start_date']

    return STATE[entity]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity_name):
    return utils.load_json(get_abs_path('schemas/{}.json'.format(entity_name)))


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
        logger.critical("Error submitting request for export: POST {}: [{} - {}]".format(req.url, resp.status_code, resp.content))
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

    rows = []
    f = (line.decode('utf-8') for line in iter_lines(resp))
    linereader = csv.reader(f)
    fields = next(linereader)

    for row in linereader:
        row = dict(zip(fields, row))
        rows.append(row)

    return rows

def iter_lines(response, chunk_size=ITER_CHUNK_SIZE, decode_unicode=None, delimiter=None):
    """This funcitoin is derived from Response.iter_lines in python requests library

        .. note:: This method is not reentrant safe.
        """
    pending = None
    carriage_return = u'\r' if decode_unicode else b'\r'
    line_feed = u'\n' if decode_unicode else b'\n'
    last_chunk_ends_with_cr = False

    for chunk in response.iter_content(chunk_size=chunk_size, decode_unicode=decode_unicode):

        if pending is not None:
            chunk = pending + chunk

        if delimiter:
            lines = chunk.split(delimiter)
        else:
            skip_first_char = last_chunk_ends_with_cr and chunk.startswith(line_feed)
            last_chunk_ends_with_cr = chunk.endswith(carriage_return)
            if skip_first_char:
                chunk = chunk[1:]
                if not chunk:
                    continue
            lines = chunk.splitlines()

        if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
            pending = lines.pop()
        else:
            pending = None

        for line in lines:
            yield line

    if pending is not None:
        yield pending

def transform_timestamp(value):
    if not value:
        return None

    return utils.strftime(datetime.datetime.utcfromtimestamp(int(value) * 0.001).replace(tzinfo=pytz.utc))


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

    schema = load_schema(entity)
    singer.write_schema(entity, schema, key_properties)
    logger.info("{}: Sent schema".format(entity))

    logger.info("{}: Requesting export".format(entity))
    export_start = utils.strftime(datetime.datetime.utcnow().replace(tzinfo=pytz.utc))
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

    sync_entity("users", ["id", "accountId"])
    sync_entity("reward_balances", ["userId", "accountId"])
    sync_entity("referrals", "id")

    logger.info("Sync complete")


def main_impl():
    args = utils.parse_args(['api_key', 'tenant_alias', 'start_date'])
    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    do_sync()


def main():
    try:
        main_impl()
    except Exception as exc:
        logger.critical(exc)
        raise exc


if __name__ == '__main__':
    main()
