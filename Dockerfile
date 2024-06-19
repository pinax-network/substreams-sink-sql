FROM gcr.io/distroless/base-debian12:latest

ADD /substreams-sink-sql /app/substreams-sink-sql

ENV PATH "$PATH:/app"

ENTRYPOINT ["/app/substreams-sink-sql"]
