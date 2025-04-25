import json
import logging
from logging import Formatter

class JsonFormatter(Formatter):
    def __init__(self):
        super(JsonFormatter, self).__init__()

    def format(self, record):
        json_record = {}

        limit = 5000
        message = record.getMessage()
        message_bytes = message #.encode('utf-8')
        if len(message_bytes) > limit:
            message = message_bytes[:limit]
            #message = trimmed_message_bytes.decode('utf-8', errors='ignore')
            
        json_record["message"] = message
        json_record["level"] = str.replace(str.replace(record.levelname, "WARNING", "WARN"), "CRITICAL", "FATAL")
        return json.dumps(json_record)


logHandler = logging.StreamHandler()
logHandler.setFormatter(JsonFormatter())

logger = logging.getLogger('MyLogger')
logger.propagate = False
logger.addHandler(logHandler)
logger.setLevel(logging.DEBUG)
