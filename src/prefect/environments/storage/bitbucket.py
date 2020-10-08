from typing import TYPE_CHECKING, Any, Dict, List

from prefect.utilities.atlassian import BitbucketClient
from prefect.environments.storage import Storage
from prefect.utilities.storage import extract_flow_from_file

if TYPE_CHECKING:
    from prefect.core.flow import Flow


class Bitbucket(Storage):
    """
    Bitbucket storage class. This class represents the Storage interface for Flows
    stored in `.py` files in a Bitbucket repository.
    """
    def __init__(self, repo: str, path: str = None, host: str = None, **kwargs: Any) -> None:
        self.flows = dict()  # type: Dict[str, str]
        self._flows = dict()  # type: Dict[str, "Flow"]
        self.repo = repo
        self.path = path

        self._client = BitbucketClient(host=host)

        super().__init__(**kwargs)

    @property
    def default_labels(self) -> List[str]:
        return ["bitbucket-flow-storage"]

    def get_flow(self, flow_location: str) -> "Flow":
        """
        Given a flow_location within this Storage object, returns the underlying Flow (if possible).
        If the Flow is not found an error will be logged and `None` will be returned.

        Args:
            - flow_location (str): the location of a flow within this Storage; in this case,
                a file path on a repository where a Flow file has been committed. Will use `path` if not
                provided.

        Returns:
            - Flow: the requested Flow

        Raises:
            - ValueError: if the flow is not contained in this storage
        """
        if flow_location:
            if flow_location not in self.flows.values():
                raise ValueError("Flow is not contained in this storage")
        elif self.path:
            flow_location = self.path
        else:
            raise ValueError("No flow location provided")

        content = self._client.get_contents(self.repo, flow_location)
        return extract_flow_from_file(file_contents=content)

    def add_flow(self, flow: "Flow") -> str:
        """
        Method for storing a new flow as bytes in the local filesytem.

        Args:
            - flow (Flow): a Prefect Flow to add

        Returns:
            - str: the location of the added flow in the repo

        Raises:
            - ValueError: if a flow with the same name is already contained in this storage
        """
        if flow.name in self:
            raise ValueError(
                'Name conflict: Flow with the name "{}" is already present in this storage.'.format(
                    flow.name
                )
            )

        self.flows[flow.name] = self.path
        self._flows[flow.name] = flow
        return self.path

    def build(self) -> "Storage":
        """
        Build the Bitbucket storage object and run basic healthchecks. Due to this object
        supporting file based storage no files are committed to the repository during
        this step. Instead, all files should be committed independently.

        Returns:
            - Storage: a Bitbucket object that contains information about how and where
                each flow is stored
        """
        self.run_basic_healthchecks()
        return self

