# Script to backup a repository in GraphDB to a Trig file
# 1st argument: IP address including the port number to the GraphDB server
# 2nd argument: repository ID in GraphDB
# 3rd argument: backup file in Trig format
# Usage: ./backup_graphdb_repository.sh localhost:7200 src_fake_news src_fake_news_backup.trig

# Ref: https://graphdb.ontotext.com/documentation/enterprise/backing-up-and-recovering-repo.html
curl -X GET -H "Accept:application/x-trig" "http://$1/repositories/$2/statements?infer=false" > $3
