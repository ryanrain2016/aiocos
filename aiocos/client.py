import logging
import asyncio
import aiohttp
from .reqrep import COSResponse, COSRequest
from .errors import CosClientError, CosServiceError
from .utils import *
from urllib.parse import quote
# from yarl import URL

logger = logging.getLogger(__name__)

__all__ = ['CosS3Client']

class CosS3Client:
    def __init__(self, config):
        self._config = config
        self.auth = config.get_auth()
        self._session = aiohttp.ClientSession(request_class=COSRequest, 
                response_class=COSResponse,
                auth=self.auth)
        self._close_handler = None

    def close(self):
        self._close_handler = asyncio.ensure_future(self._session.close())

    async def wait_close(self):
        if self._close_handler is None:
            self.close()
        await self._close_handler
        self._close_handler = None
        
    def _get_url(self, bucket, key=''):
        Key = Key.lstrip('/')
        url = 'https://%s.cos.%s.myqcloud.com/%s'%(Bucket, self._config.region, Key)
        return url

    async def _parser(self, resp):
        return await resp.text()

    async def _parser_headers(self, resp):
        return dict(resp.headers)

    async def _request(self, method, *args, parser=None, **kwargs):
        parser = parser or self._parser
        try:
            async with self._session.request(method, *args, **kwargs) as resp:
                status = resp.status
                if status >= 400:
                    text = await resp.text()
                    raise CosServiceError(text, status)
                result = parser(resp)
                while isawaitable(result):
                    result = await result
                return result
        except CosServiceError:
            raise
        except Exception as e:
            raise CosClientError from e

    async def get_service(self):
        url = 'https://service.cos.myqcloud.com'
        async def parser(resp):
            return xml2dict(await resp.text(), ['Bucket'])
        return await self._request('GET', url=url, parser=parser)

    async def create_bucket(self, Bucket, **kwargs):
        """创建一个bucket
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 创建bucket
            response = await client.create_bucket(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        url = self._get_url(bucket=Bucket)
        logger.info("create bucket, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request(
                method='PUT',
                url=url,
                headers=headers)

    async def delete_bucket(self, Bucket):
        """删除一个bucket，bucket必须为空
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 删除bucket
            response = client.delete_bucket(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        url = self._get_url(bucket=Bucket)
        logger.info("delete bucket, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request(
                method='DELETE',
                url=url,
                headers=headers)

    async def head_bucket(self, Bucket):
        """确认bucket是否存在
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 确认bucket是否存在
            response = client.head_bucket(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        url = self._get_url(bucket=Bucket)
        logger.info("head bucket, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request(
            method='HEAD',
            url=url,
            headers=headers)

    async def get_bucket_location(self, Bucket):
        """查询bucket所属地域
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 存储桶的地域信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 获取bucket所在地域信息
            response = await client.get_bucket_location(
                Bucket='bucket'
            )
            print (response['LocationConstraint'])
        """
        headers = mapped(kwargs)
        params = {'location': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("get bucket location, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            return xml2dict(await resp.text())
        return await self._request('GET',
            url=url,
            headers=headers,
            params=params,
            parser=parser)


    async def list_objects(self, Bucket, Delimiter="", Marker="", MaxKeys=1000, Prefix="", EncodingType="", **kwargs):
        """获取文件列表
        :param Bucket(string): 存储桶名称.
        :param Prefix(string): 设置匹配文件的前缀.
        :param Delimiter(string): 分隔符.
        :param Marker(string): 从marker开始列出条目.
        :param MaxKeys(int): 设置单次返回最大的数量,最大为1000.
        :param EncodingType(string): 设置返回结果编码方式,只能设置为url.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 文件的相关信息，包括Etag等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 列出bucket
            response = await client.list_objects(
                Bucket='bucket',
                MaxKeys=100,
                Prefix='中文',
                Delimiter='/'
            )
        """
        decodeflag = True
        headers = mapped(kwargs)
        url = self._get_url(Bucket)
        logger.info("list objects, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        params = {
            'prefix': Prefix,
            'delimiter': Delimiter,
            'marker': Marker,
            'max-keys': MaxKeys
        }
        if EncodingType:
            if EncodingType != 'url':
                raise CosClientError('EncodingType must be url')
            decodeflag = False  # 用户自己设置了EncodingType不需要去decode
            params['encoding-type'] = EncodingType
        else:
            params['encoding-type'] = 'url'
        params = format_values(params)

        async def parser(resp):
            xml = await resp.text()
            data = xml2dict(xml, ['Contents', 'CommonPrefixes'])
            if decodeflag:
                decode_result(
                    data,
                    [
                        'Prefix',
                        'Marker',
                        'NextMarker'
                    ],
                    [
                        ['Contents', 'Key'],
                        ['CommonPrefixes', 'Prefix']
                    ]
                )
            return data
        return await self._request('GET', 
            url=url,
            params=params, 
            headers=headers,
            parser=parser)

    async def list_objects_versions(self, Bucket, Prefix="", Delimiter="", KeyMarker="", VersionIdMarker="", MaxKeys=1000, EncodingType="", **kwargs):
        """获取文件列表
        :param Bucket(string): 存储桶名称.
        :param Prefix(string): 设置匹配文件的前缀.
        :param Delimiter(string): 分隔符.
        :param KeyMarker(string): 从KeyMarker指定的Key开始列出条目.
        :param VersionIdMarker(string): 从VersionIdMarker指定的版本开始列出条目.
        :param MaxKeys(int): 设置单次返回最大的数量,最大为1000.
        :param EncodingType(string): 设置返回结果编码方式,只能设置为url.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 文件的相关信息，包括Etag等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 列出bucket带版本
            response = await client.list_objects_versions(
                Bucket='bucket',
                MaxKeys=100,
                Prefix='中文',
                Delimiter='/'
            )
        """
        headers = mapped(kwargs)
        decodeflag = True
        url = self._get_url(Bucket)
        logger.info("list objects versions, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        params = {
            'versions': '',
            'prefix': Prefix,
            'delimiter': Delimiter,
            'key-marker': KeyMarker,
            'version-id-marker': VersionIdMarker,
            'max-keys': MaxKeys
            }
        if EncodingType:
            if EncodingType != 'url':
                raise CosClientError('EncodingType must be url')
            decodeflag = False
            params['encoding-type'] = EncodingType
        else:
            params['encoding-type'] = 'url'
        params = format_values(params)

        async def parser(resp):
            xml = await resp.text()
            data = xml2dict(xml, ['Version', 'DeleteMarker', 'CommonPrefixes'])
            if decodeflag:
                decode_result(
                    data,
                    [
                        'Prefix',
                        'KeyMarker',
                        'NextKeyMarker',
                        'VersionIdMarker',
                        'NextVersionIdMarker'
                    ],
                    [
                        ['Version', 'Key'],
                        ['CommonPrefixes', 'Prefix'],
                        ['DeleteMarker', 'Key']
                    ]
                )
            return data
        return await self._request('GET', url=url, params=params, headers=headers, parser=parser)

    async def list_multipart_uploads(self, Bucket, Prefix="", Delimiter="", KeyMarker="", UploadIdMarker="", MaxUploads=1000, EncodingType="", **kwargs):
        """获取Bucket中正在进行的分块上传
        :param Bucket(string): 存储桶名称.
        :param Prefix(string): 设置匹配文件的前缀.
        :param Delimiter(string): 分隔符.
        :param KeyMarker(string): 从KeyMarker指定的Key开始列出条目.
        :param UploadIdMarker(string): 从UploadIdMarker指定的UploadID开始列出条目.
        :param MaxUploads(int): 设置单次返回最大的数量,最大为1000.
        :param EncodingType(string): 设置返回结果编码方式,只能设置为url.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 文件的相关信息，包括Etag等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 列出所有分块上传
            response = await client.list_multipart_uploads(
                Bucket='bucket',
                MaxUploads=100,
                Prefix='中文',
                Delimiter='/'
            )
        """
        headers = mapped(kwargs)
        decodeflag = True
        url = self._get_url(Bucket)
        logger.info("get multipart uploads, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        params = {
            'uploads': '',
            'prefix': Prefix,
            'delimiter': Delimiter,
            'key-marker': KeyMarker,
            'upload-id-marker': UploadIdMarker,
            'max-uploads': MaxUploads
            }
        if EncodingType:
            if EncodingType != 'url':
                raise CosClientError('EncodingType must be url')
            decodeflag = False
            params['encoding-type'] = EncodingType
        else:
            params['encoding-type'] = 'url'
        params = format_values(params)

        async def parser(resp):
            xml = await resp.text()
            data = xml2dict(xml, ['Upload', 'CommonPrefixes'])
            if decodeflag:
                decode_result(
                    data,
                    [
                        'Prefix',
                        'KeyMarker',
                        'NextKeyMarker',
                        'UploadIdMarker',
                        'NextUploadIdMarker'
                    ],
                    [
                        ['Upload', 'Key'],
                        ['CommonPrefixes', 'Prefix']
                    ]
                )
            return data
        return await self._request('GET', url=url, params=params, headers=headers, parser=parser)

    async def put_bucket_acl(self, Bucket, AccessControlPolicy={}, **kwargs):
        """设置bucket ACL
        :param Bucket(string): 存储桶名称.
        :param AccessControlPolicy(dict): 设置bucket ACL规则.
        :param kwargs(dict): 通过headers来设置ACL.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 设置 object ACL
            response = await client.put_bucket_acl(
                Bucket='bucket',
                ACL='private',
                GrantRead='id="qcs::cam::uin/123:uin/456",id="qcs::cam::uin/123:uin/123"'
            )
        """
        await self.put_object_acl(Bucket=Bucket, Key='', AccessControlPolicy=AccessControlPolicy, **kwargs)

    async def get_bucket_acl(self, Bucket, **kwargs):
        return await self.get_object_acl(Bucket=Bucket, Key='', AccessControlPolicy=AccessControlPolicy, **kwargs)

    async def put_bucket_cors(self, Bucket, CORSConfiguration={}, **kwargs):
        """设置bucket CORS
        :param Bucket(string): 存储桶名称.
        :param CORSConfiguration(dict): 设置Bucket跨域规则.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 设置bucket跨域配置
            cors_config = {
                'CORSRule': [
                    {
                        'ID': '1234',
                        'AllowedOrigin': ['http://www.qq.com'],
                        'AllowedMethod': ['GET', 'PUT'],
                        'AllowedHeader': ['x-cos-meta-test'],
                        'ExposeHeader': ['x-cos-meta-test1'],
                        'MaxAgeSeconds': 500
                    }
                ]
            }
            response = await client.put_bucket_cors(
                Bucket='bucket',
                CORSConfiguration=cors_config
            )
        """
        xml = dict2xml(CORSConfiguration, root="CORSConfiguration")
        headers = mapped(kwargs)
        headers['Content-MD5'] = get_md5(xml_config)
        headers['Content-Type'] = 'application/xml'
        params = {'cors': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("put bucket cors, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request('PUT',
            url=url,
            data=xml,
            headers=headers,
            params=params)

    async def get_bucket_cors(self, Bucket, **kwargs):
        """获取bucket CORS
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 获取Bucket对应的跨域配置.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 获取bucket跨域配置
            response = await client.get_bucket_cors(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        params = {'cors': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("get bucket cors, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))

        async def parser(resp):
            xml = await resp.text()
            data = xml2dict(xml)
            if 'CORSRule' in data and not isinstance(data['CORSRule'], list):
                lst = []
                lst.append(data['CORSRule'])
                data['CORSRule'] = lst
            if 'CORSRule' in data:
                allow_lst = ['AllowedOrigin', 'AllowedMethod', 'AllowedHeader', 'ExposeHeader']
                for rule in data['CORSRule']:
                    for text in allow_lst:
                        if text in rule and not isinstance(rule[text], list):
                            lst = []
                            lst.append(rule[text])
                            rule[text] = lst
            return data

        return await self._request('GET',
            url=url,
            headers=headers,
            params=params,
            parser=parser)


    async def delete_bucket_cors(self, Bucket, **kwargs):
        """删除bucket CORS
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 删除bucket跨域配置
            response = await client.delete_bucket_cors(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        params = {'cors': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("delete bucket cors, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request('DELETE',
            url=url,
            headers=headers,
            params=params)

    async def put_bucket_lifecycle(self, Bucket, LifecycleConfiguration={}, **kwargs):
        """设置bucket LifeCycle
        :param Bucket(string): 存储桶名称.
        :param LifecycleConfiguration(dict): 设置Bucket的生命周期规则.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 设置bucket生命周期配置
            lifecycle_config = {
                'Rule': [
                    {
                        'Expiration': {'Date': get_date(2018, 4, 24)},
                        'ID': '123',
                        'Filter': {'Prefix': ''},
                        'Status': 'Enabled',
                    }
                ]
            }
            response = await client.put_bucket_lifecycle(
                Bucket='bucket',
                LifecycleConfiguration=lifecycle_config
            )
        """
        xml = dict2xml(LifecycleConfiguration, root='LifecycleConfiguration')
        headers = mapped(kwargs)
        headers['Content-MD5'] = get_md5(xml_config)
        headers['Content-Type'] = 'application/xml'
        params = {'lifecycle': ''}
        url = self._conf.uri(bucket=Bucket)
        logger.info("put bucket lifecycle, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request('PUT',
            url=url,
            data=xml,
            headers=headers,
            params=params)

    async def get_bucket_lifecycle(self, Bucket, **kwargs):
        """获取bucket LifeCycle
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return(dict): Bucket对应的生命周期配置.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 获取bucket生命周期配置
            response = await client.get_bucket_lifecycle(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        params = {'lifecycle': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("get bucket lifecycle, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))

        async def parser(resp):
            xml = await resp.text()
            data = xml2dict(xml, ['Rule'])
            if 'Rule' in data:
                for rule in data['Rule']:
                    format_dict(rule, ['Transition', 'NoncurrentVersionTransition'])
                    if 'Filter' in rule:
                        format_dict(rule['Filter'], ['Tag'])
            return data

        return await self._request()

    async def delete_bucket_lifecycle(self, Bucket, **kwargs):
        """删除bucket LifeCycle
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 删除bucket生命周期配置
            response = await client.delete_bucket_lifecycle(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        params = {'lifecycle': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("delete bucket lifecycle, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request(
            method='DELETE',
            url=url,
            headers=headers,
            params=params)

    async def put_bucket_versioning(self, Bucket, Status, **kwargs):
        """设置bucket版本控制
        :param Bucket(string): 存储桶名称.
        :param Status(string): 设置Bucket版本控制的状态，可选值为'Enabled'|'Suspended'.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 打开多版本配置
            response = await client.put_bucket_versioning(
                Bucket='bucket',
                Status='Enabled'
            )
        """
        headers = mapped(kwargs)
        params = {'versioning': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("put bucket versioning, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        if Status != 'Enabled' and Status != 'Suspended':
            raise CosClientError('versioning status must be set to Enabled or Suspended!')
        config = dict()
        config['Status'] = Status
        xml = dict2xml(data=config, root='VersioningConfiguration')
        await self._request(
            method='PUT',
            url=url,
            data=xml,
            headers=headers,
            params=params)

    async def get_bucket_versioning(self, Bucket, **kwargs):
        """查询bucket版本控制
        :param Bucket(string): 存储桶名称.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 获取Bucket版本控制的配置.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 获取多版本配置
            response = await client.get_bucket_versioning(
                Bucket='bucket'
            )
        """
        headers = mapped(kwargs)
        params = {'versioning': ''}
        url = self._get_url(bucket=Bucket)
        logger.info("get bucket versioning, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            return xml2dict(await resp.text())

        return await self._request(
            method='GET',
            url=url,
            headers=headers,
            params=params,
            parser=parser)

    async def put_object(self, Bucket, Body, Key, EnableMD5=False, **kwargs):
        """单文件上传接口，适用于小文件，最大不得超过5GB
        :param Bucket(string): 存储桶名称.
        :param Body(file|string): 上传的文件内容，类型为文件流或字节流.
        :param Key(string): COS路径.
        :param EnableMD5(bool): 是否需要SDK计算Content-MD5，打开此开关会增加上传耗时.
        :kwargs(dict): 设置上传的headers.
        :return(dict): 上传成功返回的结果，包含ETag等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 上传本地文件到cos
            with open('test.txt', 'rb') as fp:
                response = await client.put_object(
                    Bucket='bucket',
                    Body=fp,
                    Key='test.txt'
                )
                print (response['ETag'])
        """
        await check_object_content_length(Body)
        headers = mapped(kwargs)
        url = self._get_url(Bucket, Key)
        if EnableMD5:
            md5_str = await get_content_md5(Body)
            if md5_str is not None:
                headers['Content-MD5'] = md5_str
        return await self._request('PUT', url=url, data=Body, headers=headers, parser=self._parser_headers)


    async def get_object(self, Bucket, Key, **kwargs):
        """单文件下载接口
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param kwargs(dict): 设置下载的headers.
        :return(dict): 下载成功返回的结果,包含Body对应的StreamBody,可以获取文件流或下载文件到本地.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 下载cos上的文件到本地
            response = await client.get_object(
                Bucket='bucket'
                Key='test.txt'
            )
            await response['Body'].get_stream_to_file('local_file.txt')
        """
        headers = mapped(kwargs)
        final_headers = {}
        params = {}
        for key in headers:
            if key.startswith("response"):
                params[key] = headers[key]
            else:
                final_headers[key] = headers[key]
        headers = final_headers
        if 'versionId' in headers:
            params['versionId'] = headers['versionId']
            del headers['versionId']
        format_values(params)
        url = self._get_url(Bucket, Key)
        logger.info("get object, url=:{url} ,headers=:{headers}, params=:{params}".format(
            url=url,
            headers=headers,
            params=params))
        try:
            resp = await self._session.request('GET', url=url, headers=headers, params=params)
            status = resp.status
            if status >= 400:
                text = await resp.text()
                raise CosServiceError(text, status)
            response = dict(resp.headers)
            response['Body'] = StreamBody(resp)
            return response
        except CosServiceError:
            raise
        except Exception as e:
            raise CosClientError from e

    async def get_presigned_download_url(self, Bucket, Key, Expired=300):
        """生成预签名的下载url
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param Expired(int): 签名过期时间.
        :return(string): 预先签名的下载URL.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 获取预签名文件下载链接
            response = client.get_presigned_download_url(
                Bucket='bucket'
                Key='test.txt'
            )
        """
        url = self._get_url(Bucket, Key)
        sign = self.get_auth('GET', Bucket, Key, Expired)
        url = url + '?sign=' + quote(sign)
        return url

    async def delete_object(self, Bucket, Key, **kwargs):
        """单文件删除接口
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param kwargs(dict): 设置请求headers.
        :return: dict.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 删除一个文件
            response = client.delete_object(
                Bucket='bucket'
                Key='test.txt'
            )
        """
        headers = mapped(kwargs)
        params = {}
        if 'versionId' in headers:
            params['versionId'] = headers['versionId']
            del headers['versionId']
        url = self._get_url(Bucket, Key)
        logger.info("delete object, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        return await self._request('DELETE', url=url, parser=self._parser_headers ,headers=headers, params=params)

    async def delete_objects(self, Bucket, Delete={}, **kwargs):
        """文件批量删除接口,单次最多支持1000个object
        :param Bucket(string): 存储桶名称.
        :param Delete(dict): 批量删除的object信息.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 批量删除的结果.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 批量删除文件
            objects = {
                "Quiet": "true",
                "Object": [
                    {
                        "Key": "file_name1"
                    },
                    {
                        "Key": "file_name2"
                    }
                ]
            }
            response = await client.delete_objects(
                Bucket='bucket'
                Delete=objects
            )
        """
        xml = dict2xml(Delete, 'Delete')
        headers = mapped(kwargs)
        headers['Content-MD5'] = get_md5(xml)
        headers['Content-Type'] = 'application/xml'
        params = {'delete': ''}
        params = format_values(params)
        url = self._get_url(Bucket)
        logger.info("delete objects, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            xml = await resp.text()
            return xml2dict(xml, lst=['Deleted', 'Error'])
        return await self._request('POST', url=url, data=xml,headers=headers, params=params)

    async def head_object(self, Bucket, Key, **kwargs):
        """获取文件信息
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 文件的metadata信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 查询文件属性
            response = await client.head_object(
                Bucket='bucket'
                Key='test.txt'
            )
        """
        headers = mapped(kwargs)
        params = {}
        if 'versionId' in headers:
            params['versionId'] = headers['versionId']
        del headers['versionId']
        url = self._get_url(Bucket, Key)
        logger.info("head object, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        return await self._request('HEAD', url=url, headers=headers, params=params)

    async def create_multipart_upload(self, Bucket, Key, **kwargs):
        """创建分块上传，适用于大文件上传
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 初始化分块上传返回的结果，包含UploadId等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 创建分块上传
            response = client.create_multipart_upload(
                Bucket='bucket',
                Key='test.txt'
            )
        """
        headers = mapped(kwargs)
        params = {'uploads': b''}
        url = self._get_url(Bucket, key)
        logger.info("create multipart upload, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            xml = await resp.text()
            return xml2dict(xml)
        return await self._request('POST', url=url, headers=headers, params=params, parser=parser)

    async def abort_multipart_upload(self, Bucket, Key, UploadId, **kwargs):
        """放弃一个已经存在的分片上传任务，删除所有已经存在的分片.
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param UploadId(string): 分块上传创建的UploadId.
        :param kwargs(dict): 设置请求headers.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 分块上传
            response = await client.abort_multipart_upload(
                Bucket='bucket',
                Key='multipartfile.txt',
                UploadId='uploadid'
            )
        """
        headers = mapped(kwargs)
        params = {'uploadId': UploadId}
        params = format_values(params)
        url = self._get_url(Bucket, Key)
        logger.info("abort multipart upload, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request('DELETE',
            url=url,
            headers=headers,
            params=params)

    async def upload_part(self, Bucket, Key, Body, PartNumber, UploadId, **kwargs):
        """上传分块，单个大小不得超过5GB
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param Body(file|string): 上传分块的内容,可以为文件流或者字节流.
        :param PartNumber(int): 上传分块的编号.
        :param UploadId(string): 分块上传创建的UploadId.
        :param kwargs(dict): 设置请求headers.
        :param EnableMD5(bool): 是否需要SDK计算Content-MD5，打开此开关会增加上传耗时.
        :return(dict): 上传成功返回的结果，包含单个分块ETag等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 分块上传
            async with aiofiles.open('test.txt', 'rb') as fp:
                data = await fp.read(1024*1024)
                response = await client.upload_part(
                    Bucket='bucket',
                    Body=data,
                    Key='test.txt'
                )
        """
        check_object_content_length(Body)
        headers = mapped(kwargs)
        params = {'partNumber': PartNumber, 'uploadId': UploadId}
        params = format_values(params)
        url = self._get_url(Bucket, Key)
        logger.info("upload part, url=:{url} ,headers=:{headers}, params=:{params}".format(
            url=url,
            headers=headers,
            params=params))
        if EnableMD5:
            md5_str = await get_content_md5(Body)
            if md5_str is not None:
                headers['Content-MD5'] = md5_str
        async def parser(resp):
            etag = resp.headers['ETag']
            return {'ETag': etag}
        return await self._request('PUT', url=url, headers=headers, params=params, parser=parser)

    async def list_parts(self, Bucket, Key, UploadId, MaxParts=1000, PartNumberMarker=0, EncodingType='', **kwargs):
        """列出已上传的分片.
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param UploadId(string): 分块上传创建的UploadId.
        :param EncodingType(string): 设置返回结果编码方式,只能设置为url.
        :param MaxParts(int): 设置单次返回最大的分块数量,最大为1000.
        :param PartNumberMarker(int): 设置返回的开始处,从PartNumberMarker下一个分块开始列出.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 分块的相关信息，包括Etag和PartNumber等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 列出分块
            response = await client.list_parts(
                Bucket='bucket',
                Key='multipartfile.txt',
                UploadId='uploadid'
            )
        """
        headers = mapped(kwargs)
        decodeflag = True
        params = {
            'uploadId': UploadId,
            'part-number-marker': PartNumberMarker,
            'max-parts': MaxParts}
        if EncodingType:
            if EncodingType != 'url':
                raise CosClientError('EncodingType must be url')
            params['encoding-type'] = EncodingType
            decodeflag = False
        else:
            params['encoding-type'] = 'url'
        params = format_values(params)
        url = self._get_url(Bucket, Key)
        logger.info("list multipart upload parts, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            xml = await resp.text()
            d = xml2dict(xml, ['Part'])
            if decodeflag:
                decode_result(d, ['Key'], [])
            return d
        return await self._request('GET', url=url, headers=headers, params=params, parser=parser)

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload={}, **kwargs):
        """完成分片上传,除最后一块分块块大小必须大于等于1MB,否则会返回错误.
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param UploadId(string): 分块上传创建的UploadId.
        :param MultipartUpload(dict): 所有分块的信息,包含Etag和PartNumber.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 上传成功返回的结果，包含整个文件的ETag等信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 分块上传
            response = await client.complete_multipart_upload(
                Bucket='bucket',
                Key='multipartfile.txt',
                UploadId='uploadid',
                MultipartUpload={'Part': lst}
            )
        """
        headers = mapped(kwargs)
        params = {'uploadId': UploadId}
        params = format_values(params)
        url = self._get_url(Bucket, Key)
        logger.info("create multipart upload, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            xml = await resp.text()
            rt = dict(resp.headers)
            rt.update(xml2dict(xml))
            return rt
        return await self._request('POST', 
            url=url, 
            data=dict2xml(MultipartUpload), 
            timeout=1200,
            headers=headers,
            params=params,parser=parser)

    async def put_object_acl(self, Bucket, Key, AccessControlPolicy={}, **kwargs):
        """设置object ACL
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param AccessControlPolicy(dict): 设置object ACL规则.
        :param kwargs(dict): 通过headers来设置ACL.
        :return: None.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 设置 object ACL
            response = await client.put_object_acl(
                Bucket='bucket',
                Key='multipartfile.txt',
                ACL='public-read',
                GrantRead='id="qcs::cam::uin/123:uin/456",id="qcs::cam::uin/123:uin/123"'
            )
        """
        def grant_to_xml(grant):
            return """<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="RootAccount">
            <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="RootAccount">
            <ID>qcs::cam::uin/{OwnerUin}:uin/{SubUin}</ID>
            </Grantee>
            <Permission>{Permission}</Permission></Grant>""".format(grant)
        def owner_to_xml(owner):
            return """<Owner><ID>qcs::cam::uin/{OwnerUin}:uin/{SubUin}</ID></Owner>""".format(owner)
        
        xml = ""
        if AccessControlPolicy:
            xml = dict2xml(AccessControlPolicy, root='AccessControlPolicy', rules={
                'Grant': grant_to_xml,
                'Owner': owner_to_xml
                })
        headers = mapped(kwargs)
        params = {'acl': ''}
        url = self._get_url(Bucket, Key)
        logger.info("put object acl, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        await self._request('PUT', url=url,
            data=xml,
            headers=headers,
            params=params)

    async def get_object_acl(self, Bucket, Key, **kwargs):
        """获取object ACL
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param kwargs(dict): 设置请求headers.
        :return(dict): Object对应的ACL信息.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 获取object ACL
            response = await client.get_object_acl(
                Bucket='bucket',
                Key='multipartfile.txt'
            )
        """
        headers = mapped(kwargs)
        params = {'acl': ''}
        url = self._get_url(Bucket, Key)
        logger.info("get object acl, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            xml = await resp.text()
            xml = xml.replace("{http://www.qcloud.com/document/product/436/7751}", "")
            xml = xml.replace("{https://cloud.tencent.com/document/product/436}", "")
            xml = xml.replace("{http://doc.s3.amazonaws.com/2006-03-01}", "")
            xml = xml.replace("{http://www.w3.org/2001/XMLSchema-instance}", "")
            xml = xml.replace('type', 'Type')
            data = xml2dict(xml)
            if data['AccessControlList'] is not None and isinstance(data['AccessControlList']['Grant'], dict):
                lst = [data['AccessControlList']['Grant']]
                data['AccessControlList']['Grant'] = lst
            return data

        return await self._request('GET', url=url,
            headers=headers,
            params=params,parser=parser)

    async def copy_object(self, Bucket, Key, CopySource, CopyStatus='Copy', **kwargs):
        """文件拷贝，文件信息修改
        :param Bucket(string): 存储桶名称.
        :param Key(string): 上传COS路径.
        :param CopySource(dict): 拷贝源,包含Appid,Bucket,Region,Key.
        :param CopyStatus(string): 拷贝状态,可选值'Copy'|'Replaced'.
        :param kwargs(dict): 设置请求headers.
        :return(dict): 拷贝成功的结果.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 文件拷贝
            copy_source = {'Bucket': 'test04-1252448703', 'Key': '/test.txt', 'Region': 'ap-beijing-1'}
            response = await client.copy_object(
                Bucket='bucket',
                Key='test.txt',
                CopySource=copy_source
            )
        """
        headers = mapped(kwargs)
        headers['x-cos-copy-source'] = gen_copy_source_url(CopySource)
        if CopyStatus != 'Copy' and CopyStatus != 'Replaced':
            raise CosClientError('CopyStatus must be Copy or Replaced')
        headers['x-cos-metadata-directive'] = CopyStatus
        url = self._get_url(Bucket, Key)
        logger.info("copy object, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            xml = await resp.text()
            rt = dict(resp.headers)
            rt.update(xml2dict(xml))
            return rt
        return await self._request('PUT', url=url, headers=headers, parser=parser)

    async def upload_part_copy(self, Bucket, Key, PartNumber, UploadId, CopySource, CopySourceRange='', **kwargs):
        """拷贝指定文件至分块上传
        :param Bucket(string): 存储桶名称.
        :param Key(string): 上传COS路径.
        :param PartNumber(int): 上传分块的编号.
        :param UploadId(string): 分块上传创建的UploadId.
        :param CopySource(dict): 拷贝源,包含Appid,Bucket,Region,Key.
        :param CopySourceRange(string): 拷贝源的字节范围,bytes=first-last。
        :param kwargs(dict): 设置请求headers.
        :return(dict): 拷贝成功的结果.
        .. code-block:: python
            config = CosConfig(Region=region, Secret_id=secret_id, Secret_key=secret_key, Token=token)  # 获取配置对象
            client = CosS3Client(config)
            # 分块上传拷贝分块
            copy_source = {'Bucket': 'test04-1252448703', 'Key': '/test.txt', 'Region': 'ap-beijing-1'}
            response = await client.upload_part_copy(
                Bucket='bucket',
                Key='test.txt',
                PartNumber=1,
                UploadId='your uploadid',
                CopySource=copy_source
            )
        """
        headers = mapped(kwargs)
        headers['x-cos-copy-source'] = gen_copy_source_url(CopySource)
        headers['x-cos-copy-source-range'] = CopySourceRange
        params = {'partNumber': PartNumber, 'uploadId': UploadId}
        params = format_values(params)
        url = self._get_url(Bucket, Key)
        logger.info("upload part copy, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        async def parser(resp):
            xml = await resp.text()
            rt = dict(resp.headers)
            rt.update(xml2dict(xml))
            return rt
        return await self._request('PUT', url=url, headers=headers, parser=parser, params=params)

    async def restore_object(self, Bucket, Key, RestoreRequest={}, **kwargs):
        """取回沉降到CAS中的object到COS
        :param Bucket(string): 存储桶名称.
        :param Key(string): COS路径.
        :param RestoreRequest: 取回object的属性设置
        :param kwargs(dict): 设置请求headers.
        :return: None.
        """
        params = {'restore':''}
        headers = mapped(kwargs)
        if 'versionId' in headers:
            params['versionId'] = headers['versionId']
            headers.pop('versionId')
        url = self._get_url(Bucket, Key)
        logger.info("restore_object, url=:{url} ,headers=:{headers}".format(
            url=url,
            headers=headers))
        xml = dict2xml(RestoreRequest, root=b'RestoreRequest')
        await self._request('POST', url=url,
            data=xml,
            headers=headers,
            params=params)

    async def upload_file(Bucket, Key, LocalFilePath, PartSize=1, MAXThread=5, **kwargs):
        pass

    def get_auth(Method, Bucket, Key, Expired=300, Headers=None, Params=None):
        region = self._config.region
        Headers = Headers or {}
        if 'Host' not in Headers:
            Headers.add('Host', '%s.cos.%s.myqcloud.com'%(Bucket, region))
        return self.auth.get_auth(Method, Key, Expired, Headers, Params or {})



