.PHONY: up down logs python logs-python db shell-blazor

up:
	docker compose up -d

down:
	docker compose down

db:
	docker compose up -d db

python:
	docker compose up python

logs:
	docker compose logs -f

logs-python:
	docker compose logs -f python

shell-blazor:
	docker compose exec blazor sh

prune:
	docker system prune -f
