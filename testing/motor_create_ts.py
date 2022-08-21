import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import CollectionInvalid


async def create_coll():
    try:
        await db.create_collection("testColl1", timeseries={"timeField": "timestamp"})
    except CollectionInvalid:
        print("Caught exception!")


if __name__ == "__main__":
    client = AsyncIOMotorClient(
        host="192.168.1.20", port=27017, directConnection=True, retryWrites=False
    )
    db = client["tstest"]
    asyncio.run(create_coll())
