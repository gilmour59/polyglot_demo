docker pull cassandra
docker run --name cassandra-dev -p 9042:9042 -d cassandra
docker exec -it cassandra-dev cqlsh
docker logs cassandra-dev


docker pull neo4j
docker run \
    --name neo4j-dev \
    -p 7474:7474 -p 7687:7687 \
    -d \
    -e NEO4J_AUTH=neo4j/YOUR_STRONG_PASSWORD \
    neo4j