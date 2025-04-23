import unittest
import time
from server.server import RedisServer
from client.client import RedisClient
import threading

class TestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = RedisServer({
            'host': 'localhost',
            'port': 6381,
            'aof_enabled': False,
            'rdb_enabled': False
        })
        cls.server_thread = threading.Thread(target=cls.server.start)
        cls.server_thread.start()
        time.sleep(0.1)
        cls.client = RedisClient(port=6381)
    
    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.server_thread.join()

class TestStrings(TestBase):
    def test_basic_operations(self):
        self.client.set("str", "hello")
        self.assertEqual(self.client.get("str"), "hello")
        self.assertEqual(self.client.execute("STRLEN", "str"), "5")
    
    def test_incr_decr(self):
        self.client.set("counter", "10")
        self.assertEqual(self.client.execute("INCR", "counter"), "11")
        self.assertEqual(self.client.execute("DECR", "counter"), "10")
    
    def test_append(self):
        self.client.set("greet", "hello")
        self.assertEqual(self.client.execute("APPEND", "greet", " world"), "11")
        self.assertEqual(self.client.get("greet"), "hello world")

class TestLists(TestBase):
    def test_push_pop(self):
        self.assertEqual(self.client.execute("LPUSH", "mylist", "world"), "1")
        self.assertEqual(self.client.execute("LPUSH", "mylist", "hello"), "2")
        self.assertEqual(self.client.execute("RPOP", "mylist"), "world")
        self.assertEqual(self.client.execute("LPOP", "mylist"), "hello")
    
    def test_range(self):
        self.client.execute("RPUSH", "nums", *map(str, range(5)))
        self.assertEqual(
            self.client.execute("LRANGE", "nums", "0", "-1"),
            ["0", "1", "2", "3", "4"]
        )

class TestHashes(TestBase):
    def test_hash_ops(self):
        self.assertEqual(self.client.execute("HSET", "user:1", "name", "Alice"), "1")
        self.assertEqual(self.client.execute("HGET", "user:1", "name"), "Alice")
        self.assertEqual(self.client.execute("HGETALL", "user:1"), ["name", "Alice"])

class TestSets(TestBase):
    def test_set_ops(self):
        self.assertEqual(self.client.execute("SADD", "myset", "a", "b", "c"), "3")
        self.assertEqual(self.client.execute("SCARD", "myset"), "3")
        self.assertEqual(self.client.execute("SMEMBERS", "myset"), ["a", "b", "c"])

class TestSortedSets(TestBase):
    def test_zset_ops(self):
        self.assertEqual(self.client.execute("ZADD", "leaderboard", "100", "player1"), "1")
        self.assertEqual(self.client.execute("ZADD", "leaderboard", "200", "player2"), "1")
        self.assertEqual(
            self.client.execute("ZRANGE", "leaderboard", "0", "-1", "WITHSCORES"),
            ["player1", "100", "player2", "200"]
        )

class TestStreams(TestBase):
    def test_stream_ops(self):
        msg_id = self.client.execute("XADD", "mystream", "*", "field1", "value1")
        self.assertIsNotNone(msg_id)
        result = self.client.execute("XRANGE", "mystream", "-", "+")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][1]["field1"], "value1")

class TestBitmaps(TestBase):
    def test_bit_ops(self):
        self.assertEqual(self.client.execute("SETBIT", "mybits", "7", "1"), "0")
        self.assertEqual(self.client.execute("GETBIT", "mybits", "7"), "1")
        self.assertEqual(self.client.execute("BITCOUNT", "mybits"), "1")

class TestGeo(TestBase):
    def test_geo_ops(self):
        self.assertEqual(
            self.client.execute("GEOADD", "places", "13.361389", "38.115556", "Palermo"), 
            "1"
        )
        dist = self.client.execute("GEODIST", "places", "Palermo", "Palermo")
        self.assertEqual(dist, "0.0")

class TestTimeSeries(TestBase):
    def test_ts_ops(self):
        ts = str(time.time())
        self.client.execute("TSADD", "temperatures", "25.5", ts)
        result = self.client.execute("TSGET", "temperatures")
        self.assertEqual(float(result[1]), 25.5)

class TestVectors(TestBase):
    def test_vector_ops(self):
        self.client.execute("VECADD", "vec1", *map(str, [0.1, 0.2, 0.3]))
        result = self.client.execute("VECGET", "vec1")
        self.assertEqual(len(result), 3)
        self.assertAlmostEqual(float(result[0]), 0.1)

if __name__ == '__main__':
    unittest.main()