import pendulum


def to_timestamp(timestamp: str) -> pendulum.DateTime:
    return pendulum.parse(timestamp)
