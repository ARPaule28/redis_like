import math
import numpy as np
from typing import Dict, List, Optional, Tuple
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError, VectorError

class VectorOps:
    def __init__(self, data_store: DataStore, dimension: int = 128):
        self._data = data_store
        self._dimension = dimension
        self._vectors: Dict[str, np.ndarray] = {}
        self._index = None  # For similarity search
        self._needs_reindex = True
    
    def vec_add(self, key: str, vector: List[float]) -> bool:
        """Add a vector to the database"""
        if len(vector) != self._dimension:
            raise VectorError(f"Vector dimension must be {self._dimension}")
        
        self._vectors[key] = np.array(vector, dtype=np.float32)
        self._data._type_map[key] = 'vector'
        self._needs_reindex = True
        return True
    
    def vec_get(self, key: str) -> Optional[List[float]]:
        """Get a vector by key"""
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'vector':
            raise WrongTypeError('vector', self._data.type(key))
        return self._vectors[key].tolist()
    
    def vec_similarity(self, key1: str, key2: str, 
                      metric: str = 'cosine') -> float:
        """Calculate similarity between two vectors"""
        vec1 = self.vec_get(key1)
        vec2 = self.vec_get(key2)
        
        if vec1 is None or vec2 is None:
            raise KeyNotFoundError(key1 if vec1 is None else key2)
        
        return self._calculate_similarity(vec1, vec2, metric)
    
    def vec_search(self, query: List[float], k: int = 5,
                  metric: str = 'cosine') -> List[Tuple[str, float]]:
        """Search for similar vectors"""
        if len(query) != self._dimension:
            raise VectorError(f"Query dimension must be {self._dimension}")
        
        if self._needs_reindex:
            self._build_index()
        
        query_vec = np.array(query, dtype=np.float32).reshape(1, -1)
        
        if metric == 'cosine':
            # Calculate cosine similarity manually
            similarities = []
            for key, vec in self._vectors.items():
                sim = self._calculate_similarity(query, vec.tolist(), 'cosine')
                similarities.append((key, sim))
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:k]
        else:
            # For other metrics use the index
            distances, indices = self._index.kneighbors(query_vec, n_neighbors=k)
            return [(list(self._vectors.keys())[i], float(1 - d)) 
                    for i, d in zip(indices[0], distances[0])]
    
    def vec_operation(self, operation: str, keys: List[str]) -> List[float]:
        """Perform vector operations (add, subtract, average)"""
        vectors = []
        for key in keys:
            vec = self.vec_get(key)
            if vec is None:
                raise KeyNotFoundError(key)
            vectors.append(np.array(vec))
        
        if operation == 'add':
            result = np.sum(vectors, axis=0)
        elif operation == 'subtract':
            result = vectors[0] - vectors[1]
        elif operation == 'average':
            result = np.mean(vectors, axis=0)
        else:
            raise VectorError(f"Unknown operation: {operation}")
        
        return result.tolist()
    
    def _build_index(self):
        """Build index for similarity search"""
        if not self._vectors:
            return
        
        vectors = list(self._vectors.values())
        self._index = NearestNeighbors(
            n_neighbors=5, 
            metric='cosine', 
            algorithm='brute'
        )
        self._index.fit(vectors)
        self._needs_reindex = False
    
    def _calculate_similarity(self, vec1: List[float], vec2: List[float], 
                            metric: str) -> float:
        """Calculate similarity between two vectors"""
        v1 = np.array(vec1)
        v2 = np.array(vec2)
        
        if metric == 'cosine':
            return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))
        elif metric == 'euclidean':
            return float(1 / (1 + np.linalg.norm(v1 - v2)))
        elif metric == 'dot':
            return float(np.dot(v1, v2))
        else:
            raise VectorError(f"Unknown metric: {metric}")