import asyncio
import logging
import os
import sys
from json import dumps as json_dumps, load as json_load, loads as json_loads
from shutil import rmtree

import aiohttp

global projectDir
global loggerglobal

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
async def repeatQueueForErrors(error_raws, session, url, data, json_value):
    if len(error_raws):
        """Awaiting server response for 1 seconds"""
        await asyncio.sleep(1)
        tasks = []
        for error_raw in error_raws:
            tasks.append(asyncio.ensure_future(post_query(session, url, data[error_raw])))
        result = await asyncio.gather(*tasks)

        if isinstance(result, list):
            for idx, lst_result in enumerate(result):
                if isinstance(lst_result, list):
                    error_idx = error_raws[idx]
                    lstCount = len(lst_result)
                    if lstCount > 1:
                        json_value[error_raws[idx]] = {"error": {"status": 200, "reason": lst_result.__str__()},
                                                       "index": error_idx}
                    elif lstCount == 1:
                        itm = lst_result[0]
                        error_number = str(itm.get("error").get("status") if itm.get("error") else None)
                        loggerglobal.exception(
                            f"JSON value: {json_value[error_idx]} has been replaced. HTTP error: {error_number}",
                            exc_info=f"Bad request{1}")
                        itm.update({"index": error_idx})
                        json_value[error_idx] = itm

                    else:
                        json_value[error_idx] = {"error": {"status": 200, "reason": "Result is empty"},
                                                 "index": error_idx}

@logDecorator
async def post_query(session, url, json):
    async with session.post(url=url, json=json) as resp:
        if resp.status in [200, 201, 400]:
            return await resp.json() if len(resp.content._buffer) != 0 else {
                "error": {"status": 200, "reason": "Result is empty"}}
        else:
            return [{"error": {"status": resp.status, "reason": resp.reason, "json": json, "url": url}}]

@logDecorator
async def post(data, uuid):
    base_url = data.get("base_url")
    url = data.get("url")
    ssl = data.get("ssl")
    login = data.get("login")
    password = data.get("password")
    headers = data.get("headers")
    data = data.get("data")

    if not ("http://" in base_url or "https://" in base_url):
        if ssl and ssl == "true":
            base_url = f"https://{base_url}"
        else:
            base_url = f"http://{base_url}"

    if login and not (headers and headers.get("Authorization")):
        basicAuth = aiohttp.BasicAuth(login, password)
    else:
        basicAuth = None

    async with aiohttp.ClientSession(base_url=base_url, auth=basicAuth, headers=headers) as session:
        tasks = []
        for json in data:
            tasks.append(asyncio.ensure_future(post_query(session, url, json)))

        result = await asyncio.gather(*tasks)
        result_json = {"data": []}
        json_value = result_json.get("data")
        error_raws = []
        if isinstance(result, list):
            for idx, lst_result in enumerate(result):
                if isinstance(lst_result, list):
                    lstCount = len(lst_result)
                    if lstCount > 1:
                        json_value.append({"error": {"status": 200, "reason": lst_result.__str__()}, "index": idx})
                    elif lstCount == 1:
                        itm = lst_result[0]
                        itm.update({"index": idx})
                        json_value.append(itm)
                        error_ = itm.get("error")
                        if error_:
                            error_status = error_.get("status")
                            if error_status in [502, 401]:
                                error_raws.append(idx)
                    else:
                        json_value.append({"error": {"status": 200, "reason": "Result is empty"}, "index": idx})
                elif isinstance(lst_result, dict):
                    lst_result.update({"index": idx})
                    if lst_result.get("ErrorType") == 'ValidationException':
                        lst_result.update(
                            {"error": {"status": 400, "reason": lst_result['ErrorItems'][0]['ErrorMessage']}})
                    json_value.append(lst_result)

        await repeatQueueForErrors(error_raws, session, url, data, json_value)

        with open(os.path.join(projectDir, uuid, "result.json"), "w", encoding="UTF-8") as jsonFile:
            jsonFile.write(json_dumps(result_json, ensure_ascii=False).__str__())

@logDecorator
def callAsyncApi(uuid):
    with open(os.path.join(projectDir, uuid, f"data.json"), "r", encoding="UTF-8") as jsonFile:
        data = json_load(jsonFile)

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    if data.get("method") == "post":
        asyncio.run(post(data, uuid))

def clearLogs():
    globalHandler = logging.FileHandler(os.path.join(projectDir, "errors.log"), "w")
    loggerglobal.addHandler(globalHandler)

@logDecorator
def clearTempFiles(Tempdir):
    rmtree(os.path.join(projectDir, Tempdir), ignore_errors=True)

if __name__ == '__main__':
    getGlobalVariables()
    paramLen = len(sys.argv)
    if paramLen == 2:
        arg = sys.argv[1]
        if arg == "-clearLogs":
            clearLogs()
        else:
            callAsyncApi(arg)
    elif paramLen == 3:
        arg = sys.argv[1]
        if arg == "-clear":
            clearTempFiles(sys.argv[2])
    else:
        loggerglobal.exception(f"Wrong parameters.")
