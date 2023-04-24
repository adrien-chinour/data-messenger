import json
import os
import re
from datetime import datetime
from functools import partial

import bigquery
import files

# Fix bad encoding from Messenger JSON files
fix_mojibake_escapes = partial(re.compile(rb'\\u00([\da-f]{2})').sub, lambda m: bytes.fromhex(m[1].decode()))


def load(conversation):
    bigquery.init()
    folder = "../data/messages/inbox/{conversation}/".format(conversation=conversation)
    for file in [f for f in os.listdir(folder) if re.match(r'.*\.json$', f)]:
        _import_json_file(folder + file)


def _import_json_file(file):
    print('Importing file : ', file)
    with open(file, 'rb') as binary_data:
        data = json.loads(fix_mojibake_escapes(binary_data.read()), strict=False)
        _import_text_messages(data['messages'])
        _import_reactions(data['messages'])
        _import_medias(data['messages'])


def _import_text_messages(messages):
    rows = []
    for message in messages:
        if 'content' in message and 'sender_name' in message and 'timestamp_ms' in message:
            rows.append([
                message['sender_name'],
                datetime.fromtimestamp(message['timestamp_ms'] / 1000),
                message['content'],
                len(message['reactions'] if 'reactions' in message else [])
            ])
    bigquery.insert(bigquery.Schema.messages, rows)
    print('Rows inserted on messages : ', len(rows))


def _import_reactions(messages):
    rows = []
    for message in messages:
        if 'reactions' in message:
            for reaction in message['reactions']:
                rows.append([
                    reaction['actor'],
                    reaction['reaction'],
                ])
    bigquery.insert(bigquery.Schema.reactions, rows)
    print('Rows inserted on reactions : ', len(rows))


def _import_medias(messages):
    rows = []
    for message in messages:
        for media_type in files.media_types:
            if media_type == 'audio':
                media_type = 'audio_files'
            if media_type in message:
                for media in message[media_type]:
                    rows.append([
                        message['sender_name'],
                        media_type,
                        files.get_url(media['uri']),
                        datetime.fromtimestamp(message['timestamp_ms'] / 1000),
                        len(message['reactions'] if 'reactions' in message else [])
                    ])
    bigquery.insert(bigquery.Schema.files, rows)
    print('Rows inserted on files : ', len(rows))
