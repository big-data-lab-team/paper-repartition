import datetime
import os


def log(message, level=0):
    LOG_LEVEL = 2
    if level >= LOG_LEVEL:
        print(f'[ {datetime.datetime.now().time()} ] {message}')

    if level == 2 and (log_file := os.getenv('KEEP_LOG')) is not None:
        with open(log_file, 'a+') as f:
            f.write(message.split(os.linesep)[1] + os.linesep)
