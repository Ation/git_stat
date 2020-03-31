#! /bin/bash

echo 'Loading data from git'

echo '[' > git_info.data
git log --no-merges --pretty=format:'{ "hash" :" %H", "author_name" : "%aN", "author_email" : "%aE", "date" : "%aI"' --shortstat >> git_info.data
echo '{}]' >> git_info.data

echo '[' > git_merges.json
git log --merges --pretty=format:'{ "hash" :" %H", "author_name" : "%aN", "author_email" : "%aE", "date" : "%aI"},'>> git_merges.json
echo '{}]' >> git_merges.json

echo 'Processing'

python process_to_json.py
