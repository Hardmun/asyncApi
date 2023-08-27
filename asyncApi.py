import asyncio
import logging
import os
import sys
from json import dumps as json_dumps

import aiohttp

def getGlobalVariables():
    """global path"""
    global projectDir
    projectDir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
        os.path.abspath(__file__))

    global loggerglobal
    loggerglobal = logging.getLogger("errors")
    loggerglobal.setLevel(logging.ERROR)

    formatter = logging.Formatter("%(asctime)s:%(message)s")

    globalHandler = logging.FileHandler(os.path.join(projectDir, "errors.log"))
    globalHandler.setLevel(logging.raiseExceptions)
    globalHandler.setFormatter(formatter)

    loggerglobal.addHandler(globalHandler)

def logDecorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseException as errMsg:
            loggerglobal.exception(f"An error has been occurred in function {func.__name__}", exc_info=errMsg)
            sys.exit(0)

    return wrapper

@logDecorator
async def post_query(session, url, json):
    async with session.post(url=url, json=json) as resp:
        if resp.status == 200:
            return await resp.json()
        else:
            return [{"error": {"status": resp.status,
                               "reason": resp.reason,
                               "json": json,
                               "url": url}}]

@logDecorator
async def post(settings):
    base_url = settings.get("base_url")
    url = settings.get("url")
    ssl = settings.get("ssl")
    login = settings.get("login")
    password = settings.get("password")
    headers = settings.get("headers")
    data = settings.get("data")
    uuid = settings.get("uuid")

    if not ("http://" in base_url or "https://" in base_url):
        if ssl and ssl=="true":
            base_url = f"https://{base_url}"
        elif ssl and ssl=="false":
            base_url = f"http://{base_url}"

    if login:
        basicAuth = aiohttp.BasicAuth(login, password)
    else:
        basicAuth = ""

    async with aiohttp.ClientSession(base_url=base_url, auth=basicAuth, headers=headers) as session:
        tasks = []
        for json in data:
            tasks.append(asyncio.ensure_future(post_query(session, url, json)))

        result = await asyncio.gather(*tasks)
        result_json = {"data": []}
        json_value = result_json.get("data")
        if isinstance(result, list):
            for lst_result in result:
                if isinstance(lst_result, list):
                    lstCount = len(lst_result)
                    if lstCount == 1:
                        json_value.append(lst_result[0])
                    if lstCount > 1:
                        json_value.append({"error": {"status": 200,
                                                     "reason": lst_result.__str__()}})

        # dirPath = os.path.join(projectDir, uuid)
        # if not os.path.exists(dirPath):
        #     os.mkdir(dirPath)
        with open(os.path.join(projectDir, f"{uuid}.json"), "w", encoding="utf-8") as jsonFile:
            jsonFile.write(json_dumps(result_json).__str__())

@logDecorator
def readParameters(fileSettings_string):
    settings = eval(fileSettings_string)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if settings.get("method") == "post":
        asyncio.run(post(settings))

if __name__ == '__main__':
    getGlobalVariables()
    if len(sys.argv) == 2:
        arg = sys.argv[1]
        readParameters(arg)
