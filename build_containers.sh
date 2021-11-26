#! /bin/bash
docker build -t git_importer:latest -f git_importer.dockerfile .
docker-slim build --target git_importer:latest --tag git_importer_slim:latest --http-probe-off
