#!/usr/bin/env bash

jq '.[].repo.full_name' < data/traces.json | sort | uniq | wc -l
