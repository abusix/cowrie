from __future__ import absolute_import, division

import json
from configparser import NoOptionError

from gnsq import Producer

import cowrie.core.output
from cowrie.core.config import CowrieConfig


class Output(cowrie.core.output.Output):
    """
    nsq output
    """

    def start(self):
        host = CowrieConfig().get('output_nsq', 'host')

        try:
            port = CowrieConfig().getint('output_nsq', 'port')
        except NoOptionError:
            port = 4150

        try:
            auth_secret = CowrieConfig().get('output_nsq', 'auth_secret')
        except NoOptionError:
            auth_secret = None

        try:
            tls = CowrieConfig().getboolean('output_nsq', 'use_tls')
        except NoOptionError:
            tls = False

        self.producer = Producer(host + ":" + str(port), auth_secret=auth_secret, tls_v1=tls, tls_options={})
        self.topic = CowrieConfig().get('output_nsq', 'topic')
        self.producer.start()

    def stop(self):
        self.producer.close()

    def write(self, logentry):
        for i in list(logentry.keys()):
            # Remove twisted 15 legacy keys
            if i.startswith('log_'):
                del logentry[i]

        self.producer.publish(self.topic, json.dumps(logentry).encode("utf-8"))
