
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
