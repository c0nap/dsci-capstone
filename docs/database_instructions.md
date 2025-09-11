
---
title: Data Science Capstone - Patrick Conan
---

## Database Setup

If you need help installing your databases, follow these guides (click to expand).

<details>
  <summary><h2>PostgreSQL</h2></summary>
  

  1. Install database engine.
```bash
sudo apt install postgresql
```
```bash
sudo -i -u postgres
psql
CREATE USER yourusername WITH PASSWORD 'yourpassword';
ALTER USER yourusername CREATEDB;
```
```bash

```

2. Start the database service.
```bash
sudo service mysql start
```
</details>

---

## Other Issues


Running MySQL on local machine: Must allow external connections.
```sql
-- Allow root from all IPs (not secure):
CREATE USER 'root'@'%' IDENTIFIED BY 'your_root_password';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

DB_NAME in .env must be all lowercase for Postgres.


## Neo4j Installation for WSL (Ubuntu)

1. Install prerequisites `wget`, `gnupg`, and `apt-transport-https`

Without these, you can’t fetch and trust Neo4j’s signing key or add an HTTPS repo.

```bash
sudo apt install wget gnupg apt-transport-https
```

2. Add the GPG key

Apt refuses to install packages from unknown sources. The GPG key tells apt to trust packages signed by Neo4j.

```bash
wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
```

3. Add the Neo4j repository

By default, Ubuntu only knows about Canonical’s repos. Adding the Neo4j repo is like specifying which warehouse has the Neo4j software. Without it, apt won’t find the `neo4j` package.

```bash
echo "deb https://debian.neo4j.com stable 5" | sudo tee /etc/apt/sources.list.d/neo4j.list
```

4. Run `apt update` after adding the repo

This refreshes apt’s index. Until you do, apt has no idea that Neo4j packages exist.

```bash
sudo apt update
```

5. Install Neo4j

Now apt knows where to find the package and can verify its signature.

```bash
sudo apt install neo4j
```

#### First-time setup

1. Start the database service. Ignore the message that Neo4j is running on port 7474 instead of 7687 - it runs on both.

```bash
sudo service neo4j start
```

2. Log in with default username and password. `cypher-shell` is installed with `neo4j`.

```bash
cypher-shell -u neo4j -p neo4j
```

3. Prompted to change password on first login (8+ chars long).
