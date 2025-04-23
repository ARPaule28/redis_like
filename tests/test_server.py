import unittest
import threading
import time
from server.server import RedisServer
from client.client import RedisClient

class TestRedisServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = RedisServer({
            'host': 'localhost',
            'port': 6380,  # Different port for tests
            'aof_enabled': False,
            'rdb_enabled': False
        })
        cls.server_thread = threading.Thread(target=cls.server.start)
        cls.server_thread.start()
        time.sleep(0.1)  # Wait for server to start
        cls.client = RedisClient(port=6380)
    
    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.server_thread.join()
    
    def test_ping(self):
        self.assertEqual(self.client.execute("PING"), "PONG")
    
    def test_set_get(self):
        self.client.set("test", "value")
        self.assertEqual(self.client.get("test"), "value")
    
    def test_expire(self):
        self.client.set("temp", "value")
        self.client.execute("EXPIRE", "temp", "1")
        self.assertEqual(self.client.execute("TTL", "temp"), "1")
        time.sleep(1.1)
        self.assertIsNone(self.client.get("temp"))
    
    def test_concurrent_clients(self):
        def client_work():
            client = RedisClient(port=6380)
            client.set("conc", "val")
            return client.get("conc")
        
        results = []
        threads = []
        for _ in range(10):
            t = threading.Thread(target=lambda: results.append(client_work()))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        self.assertEqual(len(results), 10)
        self.assertTrue(all(r == "val" for r in results))

if __name__ == '__main__':
    unittest.main()