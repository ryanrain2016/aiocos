from hashlib import sha1
import hmac
from time import time
from aiohttp import hdrs
from urllib.parse import quote, urlencode
from .utils import ensure_bytes, ensure_str

__all__=['Auth']


class Auth:
    def __init__(self, secretid, secretkey, expire=100):
        self.secretid = secretid
        self.secretkey = secretkey
        self.expire = expire

    @classmethod
    def _sha1(self, s):
        s = ensure_bytes(s)
        return sha1(s).hexdigest()

    @classmethod
    def _quote(self, s, *args, **kw):
        return quote(s, '-_.~', *args[1:], **kw)

    def _hmac_sha1(self, s, key=None):
        key = key or self.secretkey
        s = ensure_bytes(s)
        key = ensure_bytes(key)
        return hmac.new(key, s, sha1).hexdigest()

    @classmethod
    def _filter_headers(self, headers):
        _filter = lambda x: x == 'Host' or x.startswith('x')
        return {k.lower():v for k,v in headers.items() if _filter(k)}

    def __call__(self, request):
        method = request.method.lower()
        url = request.url.path
        params = dict(request.url.query)
        headers = self._filter_headers(request.headers)
        auth = self.get_auth(method, url, self.expire, headers, params)
        request.headers.add(hdrs.AUTHORIZATION, auth)

    def get_auth(self, method, url, expired, headers, params):
        options = {
            'q-sign-algorithm': 'sha1',
            'q-ak': self.secretid,
            'q-sign-time': None,
            'q-key-time': None,
            'q-header-list': None,
            'q-url-param-list': None,
            'q-signature': None
        }
        t = time()
        options['q-sign-time'] = options['q-key-time']='%d;%d'%( t - 60, t + self.expire)
        options['q-header-list'] = ';'.join(sorted(headers.keys()))
        options['q-url-param-list'] = ';'.join(sorted(params.keys()))
        skey = self._hmac_sha1(options['q-sign-time'])
        http_str = '\n'.join([method, 
            url, 
            urlencode(sorted(params.items()), 
                quote_via=self._quote).replace('+', '%2B'), 
            urlencode(sorted(headers.items()), 
                quote_via=self._quote)]) + '\n'
        s_str = '\n'.join([options['q-sign-algorithm'], 
            options['q-sign-time'], 
            self._sha1(http_str)]) + '\n'
        sig = self._hmac_sha1(s_str, skey)
        options['q-signature']=sig
        auth = urlencode(options, quote_via=lambda x, *args, **kw:x)
        return auth
