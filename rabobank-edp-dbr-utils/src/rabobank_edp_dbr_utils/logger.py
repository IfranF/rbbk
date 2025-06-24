"""Logger Module

This module configures the logger for the package. 
The logger is configured to log to the console and to a file. The log level is set to DEBUG.
"""

import logging
import logging.config

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': logging.INFO,
        }
        # for future use
        # 'file': { 
        #     'class': 'logging.FileHandler',
        #     'formatter': 'standard',
        #     'filename': LOG_FILE,
        #     'level': logging.DEBUG,
        # }
    },
    'loggers': {
        '': {
            'handlers': ['console'], # 'file' can be added later
            'level': logging.DEBUG,
            'propagate': True
        }
    }
}

logging.config.dictConfig(LOGGING_CONFIG)
