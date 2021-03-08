#!/usr/bin/env bash

# PROJECTPATH = path of the repo
# ELASTICSEARCH_ROOT = path of where elasticsearch directory is after downloading and unzipping tar file 

PROJECTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
ELASTICSEARCH_ROOT="../../elasticsearch" #to be modified...

echo "Running setup for COVID-QA project..."

echo "Installing elasticsearch"

echo "Set up data path for Elastic Search"
cat > elasticsearch.yml <<EOF
cluster.name: covid-qa
node.name: corpus-1
path:
    data:
        - $PROJECTPATH/data/elasticsearch
    repo:
        - $PROJECTPATH/saved/elasticsearch
EOF
cp elasticsearch.yml $ELASTICSEARCH_ROOT/config/

echo "Running Elasticsearch as a daemon..."
$ELASTICSEARCH_ROOT/bin/elasticsearch -d -p pid

sleep 15
echo "Create new index on Elastic Search..."
analyzer=$(cat <<EOF
{
  "mappings": {
    "properties": {
      "content.concepts": {
        "type":  "keyword"
      }
    }
  },
  "settings": {
    "analysis": {
      "analyzer": {
        "my_english_analyzer": {
          "type": "standard",
          "stopwords": "_english_"
        }
      }
    }
  }
}
EOF
)
curl -X PUT "localhost:9200/covid-qa?pretty" -H 'Content-Type: application/json' -d "$analyzer"


echo "Set up finished."