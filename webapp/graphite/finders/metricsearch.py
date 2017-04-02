from django.conf import settings
from graphite.node import BranchNode, LeafNode
from graphite.readers import ClickhouseReader
import urllib3


DEFAULT_METRICSEARCH_HOST = "localhost"
DEFAULT_METRICSEARCH_PORT = 7000

http = urllib3.PoolManager(num_pools=10, maxsize=5)

class MetricSearchException(Exception):
    pass

class MetricSearchFinder:
    def __init__(self):
        self.search_host = getattr(settings, "METRICSEARCH_HOST", DEFAULT_METRICSEARCH_HOST)
        self.search_port = getattr(settings, "METRICSEARCH_PORT", DEFAULT_METRICSEARCH_PORT)

    def find_nodes(self, query):
        url = "http://%s:%d/search?query=%s" % (self.search_host, self.search_port, query.pattern)
        r = http.request("GET", url)
        if r.status != 200:
            raise MetricSearchException("Invalid status code from metricsearch: %d" % r.status)

        data = r.data.split("\n")
        print data
        for line in data:
            if line == "":
                continue
            if line.endswith("."):
                yield BranchNode(line[:-1])
            else:
                reader = ClickhouseReader()
                yield LeafNode(line, reader)