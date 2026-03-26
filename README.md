# DataFlow


### Usage

* Create 2 docker containers as connectors (source and sink) using postgres image

```bash
docker run --name postgres-db -dp 5432:5432 -v "C:/Users/vmsko/OneDrive/Desktop/DB_STUFF/docker-volumes/postgres:/var/lib/postgresql/data" -e "POSTGRES_PASSWORD=root" postgres:17-alpine

docker run --name postgres-db-2 -dp 5433:5432 -v "C:/Users/vmsko/OneDrive/Desktop/DB_STUFF/docker-volumes/postgres-2:/var/lib/postgresql/data" -e "POSTGRES_PASSWORD=root" postgres:17-alpine
```