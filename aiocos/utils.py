import aiofiles
from inspect import isawaitable
from hashlib import md5
import os
import re
from collections import defaultdict
from urllib.parse import unquote

SINGLE_UPLOAD_LENGTH = 5*1024*1024*1024
LOGGING_UIN = 'id="qcs::cam::uin/100001001014:uin/100001001014"'

XML_RE = re.compile(r'<(\w+)>([\s\S]+?)</\1>', re.M)

maplist = {
            'ContentLength': 'Content-Length',
            'ContentMD5': 'Content-MD5',
            'ContentType': 'Content-Type',
            'CacheControl': 'Cache-Control',
            'ContentDisposition': 'Content-Disposition',
            'ContentEncoding': 'Content-Encoding',
            'ContentLanguage': 'Content-Language',
            'Expires': 'Expires',
            'ResponseContentType': 'response-content-type',
            'ResponseContentLanguage': 'response-content-language',
            'ResponseExpires': 'response-expires',
            'ResponseCacheControl': 'response-cache-control',
            'ResponseContentDisposition': 'response-content-disposition',
            'ResponseContentEncoding': 'response-content-encoding',
            'Metadata': 'Metadata',
            'ACL': 'x-cos-acl',
            'GrantFullControl': 'x-cos-grant-full-control',
            'GrantWrite': 'x-cos-grant-write',
            'GrantRead': 'x-cos-grant-read',
            'StorageClass': 'x-cos-storage-class',
            'Range': 'Range',
            'IfMatch': 'If-Match',
            'IfNoneMatch': 'If-None-Match',
            'IfModifiedSince': 'If-Modified-Since',
            'IfUnmodifiedSince': 'If-Unmodified-Since',
            'CopySourceIfMatch': 'x-cos-copy-source-If-Match',
            'CopySourceIfNoneMatch': 'x-cos-copy-source-If-None-Match',
            'CopySourceIfModifiedSince': 'x-cos-copy-source-If-Modified-Since',
            'CopySourceIfUnmodifiedSince': 'x-cos-copy-source-If-Unmodified-Since',
            'VersionId': 'versionId',
            'ServerSideEncryption': 'x-cos-server-side-encryption',
            'SSECustomerAlgorithm': 'x-cos-server-side-encryption-customer-algorithm',
            'SSECustomerKey': 'x-cos-server-side-encryption-customer-key',
            'SSECustomerKeyMD5': 'x-cos-server-side-encryption-customer-key-MD5',
            'SSEKMSKeyId': 'x-cos-server-side-encryption-cos-kms-key-id'
}

class CosConfig:
    def __init__(self, Secret_id, Secret_key, Region, Token='', auth_expire=100):
        self.secret_id = Secret_id
        self.secret_key = Secret_key
        self.region = Region
        self.token = Token
        self.auth_expire = 100

    def get_auth(self):
        from .auth import Auth
        return Auth(self.secret_id, self.secret_key, self.auth_expire)

class StreamBody():
    def __init__(self, rt):
        self._rt = rt

    def get_raw_stream(self):
        return self._rt.content

    def get_stream(self, chunk_size=1024):
        return self._rt.content

    async def get_stream_to_file(self, file_name):
        if 'Content-Length' in self._rt.headers:
            content_len = int(self._rt.headers['Content-Length'])
        else:
            raise IOError("download failed without Content-Length header")
        file_len = 0
        async with aiofiles.open(file_name, 'wb') as fp:
            reader = self.get_stream()
            while True:
                chunk = await reader.read(1024)
                if chunk:
                    file_len += len(chunk)
                    await fp.write(chunk)
                else:
                    break
            await fp.flush()
            await fp.close()
        if file_len != content_len:
            raise IOError("download failed with incomplete file")
        self._rt.close()


def ensure_bytes(s):
    if type(s) is str:
        s = s.encode()
    return s

def ensure_str(s):
    if type(s) is bytes:
        s = s.decode()
    return s

def mapped(headers):
    """S3到COS参数的一个映射"""
    _headers = dict()
    for i in headers:
        if i in maplist:
            if i == 'Metadata':
                for meta in headers[i]:
                    _headers[meta] = headers[i][meta]
            else:
                _headers[maplist[i]] = headers[i]
        else:
            raise CosClientError('No Parameter Named ' + i + ' Please Check It')
    return _headers

async def check_object_content_length(data):
    """put_object接口和upload_part接口的文件大小不允许超过5G"""

    content_length = 0
    if isinstance(data, (str, bytes)):
        content_length = len(ensure_bytes(data))
    elif hasattr(data, 'fileno') and hasattr(data, 'tell'):
        fileno = data.fileno()
        total_length = os.fstat(fileno).st_size
        current_position = data.tell()
        if isawaitable(current_position):
            current_position = await current_position
        content_length = total_length - current_position
    if content_length > SINGLE_UPLOAD_LENGTH:
        raise CosClientError('The object size you upload can not be larger than 5GB in put_object or upload_part')

def get_md5(body):
    data = ensure_bytes(data)
    return md5(data).hexdigest()

async def get_content_md5(body):
    if isinstance(body, (str, bytes)):
        return get_md5(body)
    elif hasattr(body, 'tell') and hasattr(body, 'seek') and hasattr(body, 'read'):
        file_position = body.tell()  # 记录文件当前位置
        if isawaitable(file_position):
            file_position = await file_position
        data = body.read()
        if isawaitable(data):
            data = await data
        md5_str = get_md5(data)
        seek = body.seek(file_position)  # 恢复初始的文件位置
        if isawaitable(seek):
            await seek
        return md5_str


def format_values(data):
    """格式化headers和params中的values为bytes"""
    for i in data:
        data[i] = ensure_bytes(data[i])
    return data

def dict2xml(d, root=b'Root', rules={}):
    root = ensure_bytes(root)
    xml = [b'<%s>'%root]
    for el in d:
        if el in rules:
            xml.append(rules[el](d[el]))
        elif type(d[el]) is dict:
            xml.append(dict2xml(d[el], root=el))
        elif isinstance(d[el], (tuple, list)):
            xml += [dict2xml(x, el) for x in d[el]]
        elif isinstance(d[el], (str, bytes)):
            xml.append(b'<%s>%s</%s>'%(ensure_bytes(el), ensure_bytes(d[el]), ensure_bytes(el)))

    xml.append(b'</%s>'%root)
    return b''.join(xml)

def xml2dict(xml, lst=[]):
    xml = ensure_str(xml).strip()
    root = XML_RE.findall(xml)
    if not root:
        return xml
    ret = {}
    lst_dict = defaultdict(list)
    for k,v in root:
        if k in lst:
            lst_dict[k].append(xml2dict(v, lst))
        else:
            ret[k] = xml2dict(v, lst)
    ret.update(lst_dict)
    return ret

def gen_copy_source_url(copy_source):
    """拼接拷贝源url"""
    try:
        bucket = copy_source['Bucket']
        path = copy_source['Key']
        path = format_path(path)
        region = copy_source['Region']
    except KeyError as e:
        raise CosClientError('Copy source Parameter[%s] error'%e.args[0])
    versionId = copy_source.get('VersionId', "")
    if versionid != '':
        path = path + '?versionId=' + versionid
    url = "{bucket}.{region}.myqcloud.com/{path}".format(
            bucket=bucket,
            region=region,
            path=path
        )
    return url

def decode_result(data, key_lst, multi_key_list):
    """decode结果中的字段"""
    for key in key_lst:
        if key in data and data[key]:
            data[key] = unquote(data[key])
    for multi_key in multi_key_list:
        if multi_key[0] in data:
            for item in data[multi_key[0]]:
                if multi_key[1] in item and item[multi_key[1]]:
                    item[multi_key[1]] = unquote(item[multi_key[1]])
    return data

def get_date(yy, mm, dd):
    """获取lifecycle中Date字段"""
    date_str = datetime(yy, mm, dd).isoformat()
    final_date_str = date_str+'+08:00'
    return final_date_str


def format_path(path):
    """检查path是否合法,格式化path"""
    if not isinstance(path, (str, bytes)):
        raise CosClientError("key is not string")
    if not path:
        raise CosClientError("Key is required not empty")
    path = ensure_str(path)
    if path[0] == u'/':
        path = path[1:]
    # 提前对path进行encode
    path = quote(ensure_bytes(path), b'/-_.~')
    return path

def format_dict(data, key_lst):
    """转换返回dict中的可重复字段为list"""
    for key in key_lst:
        # 将dict转为list，保持一致
        if key in data and isinstance(data[key], dict):
            lst = []
            lst.append(data[key])
            data[key] = lst
    return data