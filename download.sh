#!/usr/bin/env bash

for i in {1..1000}; do
    for retries in {1..10}; do
        if curl -u smacke:$(lpass show github-cmdline-access-token --notes) https://api.github.com/search/code\?q\=filename:history.sqlite\&page=$i\&per_page=100 | jq '[.items[] | {path, url, html_url, git_url, repo: .repository | {id, full_name, html_url, description} }]' > temp.$i.json; then
            break
        fi
        # exponential backoff for rate limit
        sleep $((2 ** $retries))
    done
done

jq -s '[.[]]|flatten' temp.*.json | jq '[.[] | select(.path | endswith("history.sqlite"))]'
rm temp.*.json
