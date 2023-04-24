#!/usr/bin/env python3

import data
import files

# Enter conversation folder from data/messages/inbox/
conversation = input()

# Upload all files on Minio server
files.upload(conversation)

# Load all conversation on BigQuery
data.load(conversation)
