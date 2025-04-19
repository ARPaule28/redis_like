import time
from typing import Dict, List, Optional, Tuple
from core.data_store import DataStore
from core.exceptions import KeyNotFoundError, WrongTypeError

class TimeSeriesOps:
    def __init__(self, data_store: DataStore):
        self._data = data_store
        self._series: Dict[str, List[Tuple[float, float]]] = {}
    
    def ts_add(self, key: str, value: float, 
              timestamp: Optional[float] = None) -> float:
        """Add a sample to the time series"""
        if key not in self._series:
            self._series[key] = []
            self._data._type_map[key] = 'timeseries'
        
        ts = timestamp if timestamp is not None else time.time()
        self._series[key].append((ts, value))
        return ts
    
    def ts_range(self, key: str, start: float, end: float,
                count: Optional[int] = None) -> List[Tuple[float, float]]:
        """Get samples from time series within range"""
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'timeseries':
            raise WrongTypeError('timeseries', self._data.type(key))
        
        samples = [sample for sample in self._series[key] 
                 if start <= sample[0] <= end]
        return samples[:count] if count else samples
    
    def ts_get(self, key: str) -> Optional[Tuple[float, float]]:
        """Get the latest sample from time series"""
        if not self._data.exists(key):
            return None
        if self._data.type(key) != 'timeseries':
            raise WrongTypeError('timeseries', self._data.type(key))
        
        return self._series[key][-1] if self._series[key] else None
    
    def ts_info(self, key: str) -> Dict[str, Union[int, float]]:
        """Get information about the time series"""
        if not self._data.exists(key):
            raise KeyNotFoundError(key)
        if self._data.type(key) != 'timeseries':
            raise WrongTypeError('timeseries', self._data.type(key))
        
        if not self._series[key]:
            return {
                'samples': 0,
                'first_timestamp': 0,
                'last_timestamp': 0,
                'min_value': 0,
                'max_value': 0
            }
        
        values = [v for _, v in self._series[key]]
        return {
            'samples': len(self._series[key]),
            'first_timestamp': self._series[key][0][0],
            'last_timestamp': self._series[key][-1][0],
            'min_value': min(values),
            'max_value': max(values)
        }
    
    def ts_aggregate(self, key: str, aggregation: str,
                    start: float, end: float,
                    bucket_size: float) -> List[Tuple[float, float]]:
        """Aggregate samples within time buckets"""
        if not self._data.exists(key):
            return []
        if self._data.type(key) != 'timeseries':
            raise WrongTypeError('timeseries', self._data.type(key))
        
        samples = self.ts_range(key, start, end)
        if not samples:
            return []
        
        aggregated = []
        current_bucket = start
        bucket_samples = []
        
        for ts, value in samples:
            if ts < current_bucket + bucket_size:
                bucket_samples.append(value)
            else:
                if bucket_samples:
                    aggregated.append((
                        current_bucket,
                        self._apply_aggregation(bucket_samples, aggregation)
                    ))
                current_bucket += bucket_size
                bucket_samples = [value]
        
        if bucket_samples:
            aggregated.append((
                current_bucket,
                self._apply_aggregation(bucket_samples, aggregation)
            ))
        
        return aggregated
    
    def _apply_aggregation(self, values: List[float], method: str) -> float:
        """Apply aggregation method to values"""
        if method == 'avg':
            return sum(values) / len(values)
        elif method == 'sum':
            return sum(values)
        elif method == 'min':
            return min(values)
        elif method == 'max':
            return max(values)
        elif method == 'count':
            return len(values)
        elif method == 'first':
            return values[0]
        elif method == 'last':
            return values[-1]
        else:
            raise ValueError(f"Unknown aggregation method: {method}")