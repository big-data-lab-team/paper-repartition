import datetime


def log(message, level=0):
    LOG_LEVEL = 1
    if level >= LOG_LEVEL:
        print(f'[ {datetime.datetime.now().time()} ] {message}')
