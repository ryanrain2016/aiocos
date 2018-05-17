from aiohttp import ClientRequest
from aiohttp import ClientResponse
from .errors import CosServiceError

__all__=['COSRequest', 'COSResponse']

class COSRequest(ClientRequest):

    def update_auth(self, auth):
        if auth is None:
            auth = self.auth
        if auth is None:
            return
        auth(self)


class COSResponse(ClientResponse):

    async def result(self):
        text = await self.text()
        status = self.status
        if status >=400:
            raise CosServiceError(text, status)
        else:
            return text
