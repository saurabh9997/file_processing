FROM postgres:latest
ENV POSTGRES_PASSWORD=password
ENV POSTGRES_USER=admin
ENV POSTGRES_DB=postman
COPY create_fixtures.sql /docker-entrypoint-initdb.d/create_fixtures.sql

FROM python:latest
COPY . /file_processing
WORKDIR /file_processing
RUN pip3 --no-cache-dir install -r requirements.txt
EXPOSE 8091
ENTRYPOINT ["python3"]
CMD ["code/api.py", "--host=0.0.0.0"]