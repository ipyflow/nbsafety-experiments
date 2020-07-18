#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import pickle
import sys


failing_imports = 0
for fname in os.listdir('pickled_imports'):
    with open(os.path.join('pickled_imports', fname), 'rb') as f:
        try:
            eval(compile(pickle.loads(f.read()), filename='', mode='exec'))
        except:
            failing_imports += 1
sys.exit(failing_imports)
