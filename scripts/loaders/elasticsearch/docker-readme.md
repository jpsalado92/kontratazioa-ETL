
[Kibana home](http://localhost:5601/app/home#/)

## Increas `evm.max_map_count` to at least [262144]
```
wsl -d docker-desktop
sysctl -w vm.max_map_count=262144
```



## get certs from docker
`docker container cp <CID>:/usr/share/elasticsearch/config/certs .`


## docker-compose commands

Create and start the three-node Elasticsearch cluster and Kibana instance in **detached mode** (`-d` won't see any logs).
`docker-compose up -d`

Attach yourself to the logs of all running services, whereas -f means you follow the log output and the -t option gives you timestamps.
`docker-compose logs -f -t`

To stop the cluster, run docker-compose down. The data in the Docker volumes is preserved and loaded when you restart the cluster with docker-compose up.
`docker-compose down`

To delete the network, containers, and volumes when you stop the cluster, specify the -v option:
`docker-compose down -v`

List containers
`docker ps`