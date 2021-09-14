from dataclasses import dataclass
from typing import Any, Optional, Union, Mapping, Iterable
from types import MethodType
import asyncio

import aiohttp
from aiohttp import BasicAuth, ClientTimeout, HttpVersion, http
from aiohttp.typedefs import StrOrURL, LooseHeaders, LooseCookies
from aiohttp.connector import BaseConnector


@dataclass
class Request:
    url: StrOrURL
    method: str = 'GET'
    params: Optional[Mapping[str, str]] = None
    data: Any = None
    json: Any = None
    headers: LooseHeaders = None
    skip_auto_headers: Optional[Iterable[str]] = None
    auth: Optional[BasicAuth] = None
    allow_redirects: bool = True
    max_redirects: int = 10
    compress: Optional[str] = None
    chunked: Optional[bool] = None
    expect100: bool = False
    raise_for_status: Optional[bool] = None
    read_until_eof: bool = True
    proxy: Optional[StrOrURL] = None
    proxy_auth: Optional[BasicAuth] = None
    timeout: Union[ClientTimeout, object] = ClientTimeout(total=5*60)
    cookies: Optional[LooseCookies] = None
    version: HttpVersion = http.HttpVersion11
    connector: Optional[BaseConnector] = None
    loop: Optional[asyncio.AbstractEventLoop] = None

    metadata: Any = None
    retry_times: int = 0

    def request(self):
        dic = self.__dict__
        sub_key = [
            'url', 'method', 'params', 'data', 'json', 'headers', 'skip_auto_headers', 'auth',
            'allow_redirects', 'max_redirects', 'compress', 'chunked', 'expect100', 'raise_for_status',
            'read_until_eof', 'proxy', 'proxy_auth', 'timeout', 'cookies', 'version', 'connector', 'loop'
        ]
        sub_dic = {key: value for key, value in dic.items() if key in sub_key}
        return aiohttp.request(**sub_dic)

    def increase_retry_times(self):
        self.retry_times = self.retry_times + 1


if __name__ == '__main__':
    async def meta(self):
        return self.metadata

    async def main():
        async with Request('https://xuehome.tk').request() as resp:
            data = await resp.text()
        print(data)
            # data = resp
            # data.metadata = 'xueyiwei'
            # data.meta = MethodType(meta, data)
            # print(await data.meta())


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
