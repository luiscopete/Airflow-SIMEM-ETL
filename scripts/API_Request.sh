#!/bin/bash

#################################
# Author: Luis Copete			#
# Role: Data Engineer			#
# Linkedin: in/luiscopete		#
#################################


# API URL
url="https://www.simem.co/backend-files/api/PublicData?startdate=$2&enddate=$3&datasetId=$1"

# Request API and host the response in a variable
response=$(curl -s "$url")

# check response
if [ $? -ne 0 ]; then
    echo "Error during the API request."
    exit 1
fi

# check if the response if empty
if [ -z "$response" ]; then
    echo "The response is empty."
    exit 1
fi

data=$(echo "$response" | jq '.')

# save the results in a file
echo "The extraction for dataset $1 in the dates range $2 / $3 is succeesfully at $(date)"
echo "$data" > ./RAW/$1.json