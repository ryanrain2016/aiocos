# aiocos
腾讯的cos python sdk 基于asyncio/aiohttp的版本

# 介绍
基于aiohttp和asyncio下的腾讯云存储的sdk， api的参数列表和返回值和官方的一致，只是使用的异步处理。

# 用法
```python
from aiocos import CosS3Client, CosConfig
import asyncio

async def main():
    config = CosConfig(Secret_id="xxxx", 
        Secret_key="xxxx", 
        Region="xxxx", 
        Token="", 
        auth_expire=100)
    client = CosS3Client(config)
    try:
        ret = await client.put_object('Bucket-appid', f._file, '/file_name')
        print(ret)
    finally:
        await client.wait_close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
```