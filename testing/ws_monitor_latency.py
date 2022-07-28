import asyncio
import logging
import time


async def log_latency(websocket, logger):
    t0 = time.perf_counter()
    pong_waiter = await websocket.ping()
    await pong_waiter
    t1 = time.perf_counter()
    logger.info("Connection latency: %.3f seconds", t1 - t0)


asyncio.create_task(log_latency(websocket, logging.getLogger()))
