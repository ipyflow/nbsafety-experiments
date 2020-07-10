#!/usr/bin/env python
import json

traces_json_file = './data/traces.json'
with open(traces_json_file) as f:
    trace_json = json.loads(f.read())
for i, entry in enumerate(trace_json):
    entry['id'] = i
with open(traces_json_file, 'w') as f:
    f.write(json.dumps(trace_json, indent=2))
