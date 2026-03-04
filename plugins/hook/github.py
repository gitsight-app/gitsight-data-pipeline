from airflow.providers.http.hooks.http import HttpHook
from requests import Response


class GithubHook(HttpHook):
    def __init__(
        self, *, http_conn_id: str = "github_api", method: str = "GET", **kwargs
    ):
        super().__init__(http_conn_id=http_conn_id, method=method, **kwargs)

    def get_repo(self, owner, repo):
        endpoint = f"/repos/{owner}/{repo}"

        response: Response = self.run(endpoint)

        response.raise_for_status()

        return response.json()
