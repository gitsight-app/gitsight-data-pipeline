from datetime import datetime

from pyspark import Broadcast

USER_QUERY = """
query getUsers($node_ids: [ID!]!) {
  nodes(ids: $node_ids) {
    ... on User {
      databaseId
      login
      name
      company
      location
      followers { totalCount }
      following { totalCount }
      createdAt
      updatedAt
    }
  }
}
"""
GITHUB_GRAPHQL_END_POINT = "https://api.github.com/graphql"


def fetch_users_partition(
    iterator, *, token_bc: Broadcast[str], batch_size_bc: Broadcast[int]
):
    """
    fetch users details from GitHub GraphQL API in batch for each partition
    split partition into segments of 1000 rows,
    then fetch details in batch for each segment to avoid timeout and memory issues

    :example
    rows per partition: 5000
    segments: 1000
    batch size: 100

    for segment in partition:
        -> process_segment(segment[batch_size])
        -> process_segment(segment[batch_size])
        ...

        yield segment results

    :param iterator: partition
    :param token_bc: api token from driver
    :param batch_size_bc: user_count_api_per_request
    :return:
    """
    import asyncio
    import itertools

    import aiohttp

    from include.spark.utils.github_utils import to_user_node_id

    if not iterator:
        return iter([])

    token = token_bc.value
    batch_size = batch_size_bc.value or 50
    headers = {"Authorization": f"Bearer {token}"}

    async def process_segment(segment_ids):
        semaphore = asyncio.Semaphore(5)
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i in range(0, len(segment_ids), batch_size):
                batch = segment_ids[i : i + batch_size]
                tasks.append(_fetch_batch(session, batch, semaphore, headers))

            responses = await asyncio.gather(*tasks)
            return [item for sublist in responses for item in sublist]

    while True:
        segment_chunk = list(itertools.islice(iterator, 1000))
        if not segment_chunk:
            break

        node_ids = [to_user_node_id(row.user_id) for row in segment_chunk]

        segment_results = asyncio.run(process_segment(node_ids))

        for data in segment_results:
            yield data


async def _fetch_batch(session, ids, semaphore, headers):

    variables = {"node_ids": ids}

    async with semaphore:
        try:
            async with session.post(
                GITHUB_GRAPHQL_END_POINT,
                json={"query": USER_QUERY, "variables": variables},
                headers=headers,
                timeout=30,
            ) as resp:
                if resp.status != 200:
                    return []

                res_json = await resp.json()
                nodes = res_json.get("data", {}).get("nodes", [])
                results = []

                for node in nodes:
                    if not node:
                        continue

                    created_at = node.get("createdAt")
                    updated_at = node.get("updatedAt")

                    results.append(
                        {
                            "user_id": node.get("databaseId"),
                            "login": node.get("login"),
                            "name": node.get("name"),
                            "company": node.get("company"),
                            "location": node.get("location"),
                            "followers_count": node.get("followers", {}).get(
                                "totalCount", 0
                            ),
                            "following_count": node.get("following", {}).get(
                                "totalCount", 0
                            ),
                            "user_created_at": datetime.fromisoformat(
                                created_at.replace("Z", "+00:00")
                            )
                            if created_at
                            else None,
                            "user_updated_at": datetime.fromisoformat(
                                updated_at.replace("Z", "+00:00")
                            )
                            if updated_at
                            else None,
                        }
                    )
                return results
        except Exception as e:
            print(f"Exception: {e}")
            return []
