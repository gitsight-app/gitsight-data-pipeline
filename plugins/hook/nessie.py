from typing import Any

import confuse
from airflow.sdk import BaseHook, Connection
from pynessie import NessieClient


class NessieHook(BaseHook):
    conn = None
    client = None

    def __init__(self, nessie_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.nessie_conn_id = nessie_conn_id

    def get_conn(self) -> Any:
        if self.conn is None:
            self.conn = self.get_connection(conn_id=self.nessie_conn_id)

        return self.conn

    def _get_nessie_client(self):
        if self.client:
            return self.client

        conn: Connection = self.get_conn()
        extra_json = conn.extra_dejson

        config = confuse.Configuration("nessie")
        config.set(
            {
                "endpoint": extra_json.get("uri"),
                "verify": True,
                "auth": {"type": "none"},
                "default_branch": extra_json.get("ref", "main"),
            }
        )

        return NessieClient(config)

    def create_branch(self, branch_name: str, ref: str | None = None):
        client = self._get_nessie_client()
        target_ref = ref or client.get_default_branch()
        base_reference = client.get_reference(target_ref)
        client.create_branch(
            branch=branch_name, ref=target_ref, hash_on_ref=base_reference.hash_
        )

    def delete_branch(self, branch_name: str):
        client = self._get_nessie_client()
        ref = client.get_reference(branch_name)
        client.delete_branch(branch=branch_name, hash_=ref.hash_)

    def merge_branch(self, from_ref: str, onto_branch: str | None = None):
        client = self._get_nessie_client()
        target_branch = onto_branch or client.get_default_branch()
        client.merge(from_ref=from_ref, onto_branch=target_branch)
