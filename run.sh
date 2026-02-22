#!/bin/bash
cd /opt/email-digest
.venv/bin/python main.py >> /opt/email-digest/cron.log 2>&1
