import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


if __name__ == '__main__':
    logger.debug('Debug test.')
    logger.info('Info test.')
    logger.error('Error test.')
    logger.exception('Exception test.')