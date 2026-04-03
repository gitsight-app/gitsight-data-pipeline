FROM gradle:8.5-jdk17 AS jar-builder

WORKDIR /builder
COPY build.gradle .

RUN gradle copyDependencies --no-daemon

FROM bitnamilegacy/spark:3.5.1

USER root

COPY --from=jar-builder /builder/build/deps/*.jar /opt/bitnami/spark/jars/

WORKDIR /app
ENV PATH="/home/airflow/.local/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r pyproject.toml

RUN useradd -u 1001 -g 0 -M -d /opt/bitnami/spark spark || true \
    && if ! getent passwd 1001 >/dev/null 2>&1; \
    then echo "spark:x:1001:0:Spark User:/opt/bitnami/spark:/sbin/nologin" >> /etc/passwd; fi


USER 1001

WORKDIR /opt/bitnami/spark

COPY --chown=1001:0 include /opt/bitnami/spark/include
