"""
Utility classes/functions for interacting with Atlassian products
"""
import requests
from typing import Any

import prefect


class BitbucketClient:
    """
    Thin wrapper around the Bitbucket cloud API, solely used for fetching file content.

    Args:
        - credentials (dict, optional): A dictionary of bitbucket credentials used to initialize the
            client; if not provided, will attempt to load the client using ambient environment settings.
        - host (str, optional): Specify the host if using bitbucket server. If not specified, defaults to
            bitbucket cloud.
    """
    CLOUD_API = "https://api.bitbucket.org/2.0"

    def __init__(self, credentials: dict = None, host: str = None):
        if credentials:
            user = credentials.get('BITBUCKET_USER')
            password = credentials.get('BITBUCKET_PASSWORD')
        else:
            user = prefect.context.get("secrets", {}).get("BITBUCKET_USER", None)
            password = prefect.context.get("secrets", {}).get("BITBUCKET_PASSWORD", None)

        self.host = host
        self.session = requests.Session()
        self.session.auth = (user, password)

    def get_contents(self, repo: str, path: str, branch: str = 'master') -> str:
        """
        Get the contents of a given file in a repo.

        Args:
            - repo (str, required): The repo to download the file from. In the case of a cloud connection,
                this has the format <workspace>/<repo_slug>. For server repositories, the string should
                be formatted like <project_key>/repos/<repo_slug>.
            - path (str, required): The path to the file.
            - branch (str, optional): The repository branch, defaults to 'master'

        Returns:
            The file content, as bytes.
        """
        if self.host:
            url = f"{self.host}/projects/{repo}/browse/{path}?raw&at={branch}"
        else:
            url = f"{self.CLOUD_API}/repositories/{repo}/src/{branch}/{path}"

        response = self._request("GET", url, body=None)

        if response.status_code == 404:
            raise ValueError(f"No file found at `{path}` in repo `{repo}@{branch}`")
        elif not (200 <= response.status_code < 300):
            raise ValueError(f"Request to Bitbucket failed with {response.status_code}")

        return str(response.content, "utf-8")

    def _request(self, method, url, body: Any = None, content_type: str = None, params: dict = None):
        """
        Helper method for building a request to the Bitbucket API.
        """
        headers = {'User-agent': f'Prefect/{prefect.__version__}'}

        if content_type:
            headers['Content-type'] = content_type

        response = self.session.request(
            method, url, data=body, params=params, headers=headers
        )

        if response.status_code == 401:
            raise ValueError("Bitbucket authentication failed, please check your credentials.")

        return response

