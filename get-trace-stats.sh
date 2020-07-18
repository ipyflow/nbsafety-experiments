#!/usr/bin/env bash

echo -n "Number of repositories: "
jq '.[].repo.full_name' < data/traces.json | sort | uniq | wc -l

echo -n "Number of history.sqlite files: "
sqlite3 data/traces.sqlite 'select count(distinct trace) from cell_execs'

echo -n "Number of sessions: "
sqlite3 data/traces.sqlite 'select count() from (select distinct trace, session from cell_execs)'
