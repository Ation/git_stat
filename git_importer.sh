#! /bin/bash

if [ -z "$GITHUB_TOKEN" ]
then
    echo  "GitHub token not set"
    exit 1
fi

if [ -z "$REPO_URL" ]
then
    echo  "Repo URL not set"
    exit 1
fi

REPO_PATH=/usr/app/repo

git config --global url."https://$GITHUB_TOKEN:@github.com/".insteadOf "https://github.com/"

if [ -z "$(ls -A $REPO_PATH)" ]
then
    echo "Repo not cloned to $REPO_PATH"

    # add random sleep
    # if there are many importers started it could generate spike in load
    # there is no rush
    sleep $((1 + $RANDOM + 300))

    git clone $REPO_URL $REPO_PATH
    if [ $? -ne 0 ]; then
       echo "ERROR: failed to clone repo"
       exit 1
    fi
fi

while :
do
    # loop infinitely
    pushd $REPO_PATH
    git pull
    if [ $? -ne 0 ]; then
       echo "ERROR: failed to pull repo"
       break
    fi
    popd

    python collect_stats.py $REPO_PATH
    if [ $? -ne 0 ]; then
       echo "ERROR: failed to collect stats"
       break
    fi

    sleep $((3600 + $RANDOM + 300))
done
