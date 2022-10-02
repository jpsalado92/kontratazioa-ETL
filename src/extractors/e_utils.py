import asyncio
import logging
import sys
from asyncio import Semaphore
from typing import IO

import aiofile
import aiohttp
from aiohttp import ClientSession, ClientTimeout


def async_download_urls(urls_fpaths):
    logging.info(f"Number of objects to be downloaded: {len(urls_fpaths)}")
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(get_tenders_xmls(urls_fpaths))


async def get_html(session: ClientSession, **kwargs) -> str:
    resp = await session.request(**kwargs)
    resp.raise_for_status()
    html = resp.text()
    return await html


async def fetch_html(session: ClientSession, **kwargs) -> str:
    attempt = 1
    times = 5
    while attempt < times + 1:
        try:
            html = await get_html(session, **kwargs)
        except (
                aiohttp.ClientError,
                aiohttp.http_exceptions.HttpProcessingError,
        ) as e:
            message = getattr(e, 'message', None)
            status = getattr(e, 'status', None)
            if status != 404:
                attempt += 1
            else:
                logging.warning(f"aiohttp exception for {kwargs} [{status}]: {message}")
                return
        except asyncio.exceptions.TimeoutError as e:
            attempt += 1
            logging.warning(f"Non-aiohttp exception occured:  {getattr(e, '__dict__', {})}")
        except Exception as e:
            logging.warning(f"Non-aiohttp exception occured:  {getattr(e, '__dict__', {})}")
            return
        else:
            return html
    logging.warning(f'Unable to succesfully get {kwargs} after {times}. [{status}]: {message}')
    return


async def write_one(file: IO, sem: Semaphore, **kwargs) -> None:
    async with sem:
        html = await fetch_html(**kwargs)
        if not html:
            return None
        async with aiofile.AIOFile(file, 'w') as fl:
            await fl.write(html)


async def get_tenders_xmls(urls_fpaths):
    timeout = ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=100, force_close=True)
    sem = Semaphore(100)
    async with ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for request_kwargs, fpath in urls_fpaths:
            tasks.append(write_one(file=fpath, session=session, sem=sem, **request_kwargs))
        await asyncio.gather(*tasks)
    return
