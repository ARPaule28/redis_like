import unittest
import time
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError

class TestDataStore(unittest.TestCase):
    def setUp(self):
        self.store = DataStore()
    
    def test_set_get(self):
        self.store.set("test", "value", "string")
        self.assertEqual(self.store.get("test"), "value")
        self.assertEqual(self.store.type_of("test"), "string")
    
    def test_expiration(self):
        self.store.set("temp", "value", "string")
        self.store.expire("temp", 1)
        self.assertTrue(self.store.exists("temp"))
        time.sleep(1.1)
        self.assertFalse(self.store.exists("temp"))
    
    def test_delete(self):
        self.store.set("test", "value", "string")
        self.assertTrue(self.store.delete("test"))
        self.assertFalse(self.store.exists("test"))
    
    def test_wrong_type(self):
        self.store.set("test", "value", "string")
        with self.assertRaises(WrongTypeError):
            self.store._lists["test"] = []
            self.store.get("test")

class TestExceptions(unittest.TestCase):
    def test_key_not_found(self):
        with self.assertRaises(KeyNotFoundError):
            raise KeyNotFoundError("test")
    
    def test_wrong_type(self):
        with self.assertRaises(WrongTypeError):
            raise WrongTypeError("string", "hash")

if __name__ == '__main__':
    unittest.main()