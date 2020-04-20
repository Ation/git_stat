#! /bin/bash

# 1 - dir
# 2 - branch name
# 3 - repo name

if (( $# != 3 )); then
   echo "Illegal number of parameters"
   exit 1
fi

if [ ! -d $1 ]; then
   echo "No such dir as $1"
   exit 1
fi

pwd=$(pwd)

pushd $1

if [ $? -ne 0 ]; then
   echo "Failed to navigate to $1"
   exit 1
fi

git checkout $2

if [ $? -ne 0 ]; then
   echo "Failed to checkout branch $2"
   exit 1
fi

outputDataFile=$2.data
outputJsonFile=$2.json
outputMergesFile=$2_merges.json

echo '[' > $outputDataFile
git log --no-merges --pretty=format:'{ "hash" :" %H", "author_name" : "%aN", "author_email" : "%aE", "date" : "%aI",' --shortstat >> $outputDataFile
echo '{}]' >> $outputDataFile

echo '[' > $outputMergesFile
git log --merges --pretty=format:'{ "hash" :" %H", "author_name" : "%aN", "author_email" : "%aE", "date" : "%aI"},'>> $outputMergesFile
echo '{}]' >> $outputMergesFile

echo 'Processing'

python $pwd/process_to_json.py $outputDataFile $outputJsonFile
if [ $? -ne 0 ]; then
   echo "Export failed"
   exit 1
fi

python $pwd/upload_data.py $3 $outputJsonFile $outputMergesFile
if [ $? -ne 0 ]; then
   echo "Upload failed"
   exit 1
fi