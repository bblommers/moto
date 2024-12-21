import abc

from requests.models import PreparedRequest

from ..models import Integration


class IntegrationParser:
    @abc.abstractmethod
    def invoke(
        self, request: PreparedRequest, integration: Integration
    ) -> tuple[int, str | bytes]:
        pass
