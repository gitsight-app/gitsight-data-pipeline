import base64


def to_user_node_id(user_id: int) -> str:
    """
    return node_id for fetch GraphQL Endpoint
    example: https://api.github.com/users/seon99il
    :param user_id: 63443366
    :return: MDQ6VXNlcjYzNDQzMzY2
    """
    return base64.b64encode(f"04:User{user_id}".encode()).decode()
