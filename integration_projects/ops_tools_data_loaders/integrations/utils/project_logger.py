import logging
import sys
from asyncio.log import logger

logger = logging
logger.basicConfig(stream=sys.stdout, level=logging.INFO,
                   format='%(levelname)s: %(filename)s: %(funcName)s: %(message)s: %(asctime)s')
