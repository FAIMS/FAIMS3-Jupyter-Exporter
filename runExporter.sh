#!/usr/bin/env bash

voila exporter.ipynb --VoilaConfiguration.file_whitelist="['.*']" \
  --VoilaConfiguration.file_blacklist="['private.*', '.*\.(ipynb)']"