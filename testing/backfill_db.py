import configparser
from this import d
from pymongo import MongoClient


if __name__ == '__main__':
    config = configparser.RawConfigParser()
    config.read('settings.cfg')

    client = MongoClient(
        host=config['mongodb']['host'],
        port=int(config['mongodb']['port']),
        directConnection=True
    )
    db = client[config['mongodb']['db']]
    coll = db[config['mongodb']['collection']]

    iex_docs = coll.update_many(
        {'ticker': {'$ne': None}, 'exchange': {'$eq': None}},
        {'$set': {'exchange': 'iex'}}
    )
    
    print(f"Updated {iex_docs.modified_count} docs.")
    
    