# Development setup

### Postgres
```shell
docker run --name postgres -e POSTGRES_USER=root -e POSTGRES_PASSWORD=password -d -p 5432:5432 --restart unless-stopped postgres:latest
```

#### psql
```shell
docker run -it --rm --link postgres:postgres postgres psql -h postgres -U root
```

#### to create a new database, enter psql and run:
```sql
create role filchain with encrypted password 'password';
alter role filchain with login;
create database filchain;
grant all on database filchain to filchain;
\c filchain
alter schema public owner to filchain;
```

#### update env variables
```shell
cp .env.sample .env
```

```shell
LOTUS_API_INFURA='infura_lotus_api'
LOTUS_API='backup_lotus_api_full_node'
LOTUS_TOKEN='backup_lotus_api_token'
```

#### install modules
```shell
npm i
```

#### start scraper
```shell
npm run start
```
