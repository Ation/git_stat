FROM debian:bullseye-20211011-slim

RUN   apt update \
   && apt install --no-install-recommends --no-install-suggests -y build-essential python3-dev python3-pip git wait-for-it libpq-dev \
   && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip install sqlalchemy==1.4.27 \
    pip install psycopg2-binary==2.9.2

WORKDIR /usr/app/repo
VOLUME ["/usr/app/repo"]

WORKDIR /usr/app
ADD *.py ./
ADD git_importer.sh ./

RUN git config --global pull.rebase false

CMD [ "wait-for-it", "postgres:5432", "--strict", "--timeout=120", "--", "./git_importer.sh" ]
