import json
import sys


class ParseJsonMessage(object):

    def __init__(self, filename):
        self.filename = filename

    def parse_message(self, message, keys):
        new_message = dict()
        for key in keys:
            if message.get(key):
                new_message[key] = message.get(key)
        return new_message

    def parse(self, keys):
        with open(self.filename) as log_file:
            for line in log_file:
                if line.strip():
                    message = json.loads(line)
                    print("======================")
                    parsed_message = self.parse_message(message, keys)
                    for key, val in parsed_message.items():
                        print("{}: {}".format(key, val))
                    print("======================")

ParseJsonMessage(sys.argv[1]).parse(sys.argv[2].split(','))

