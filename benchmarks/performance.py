import time
import statistics
import argparse
from typing import List, Dict, Tuple, Callable
from client.client import RedisClient

class RedisBenchmark:
    """Comprehensive benchmarking tool for Redis-like server"""
    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.client = RedisClient(host, port)
        self.results: Dict[str, Dict[str, float]] = {}
    
    def run_tests(self, tests: List[str], iterations: int = 1000, 
                 payload_size: int = 100) -> Dict[str, Dict[str, float]]:
        """Run specified benchmarks"""
        benchmark_methods = {
            'ping': self._benchmark_ping,
            'set': self._benchmark_set,
            'get': self._benchmark_get,
            'incr': self._benchmark_incr,
            'lpush': self._benchmark_lpush,
            'lrange': self._benchmark_lrange,
            'hset': self._benchmark_hset,
            'hgetall': self._benchmark_hgetall,
            'sadd': self._benchmark_sadd,
            'zadd': self._benchmark_zadd,
            'xadd': self._benchmark_xadd,
            'geoadd': self._benchmark_geoadd,
            'bitops': self._benchmark_bitops,
            'pipeline': self._benchmark_pipeline,
            'concurrent': self._benchmark_concurrent
        }
        
        for test in tests:
            if test in benchmark_methods:
                print(f"Running {test} benchmark...")
                self.results[test] = benchmark_methods[test](iterations, payload_size)
        
        return self.results
    
    def _measure(self, func: Callable, *args) -> Tuple[float, float]:
        """Measure execution time of a function"""
        start = time.perf_counter()
        func(*args)
        duration = time.perf_counter() - start
        return duration * 1000  # Convert to milliseconds
    
    def _benchmark_ping(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark PING command"""
        latencies = []
        for _ in range(iterations):
            latencies.append(self._measure(self.client.execute, 'PING'))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies),
            'p99_latency': statistics.quantiles(latencies, n=100)[-1]
        }
    
    def _benchmark_set(self, iterations: int, size: int) -> Dict[str, float]:
        """Benchmark SET command with payload"""
        value = 'x' * size
        latencies = []
        for i in range(iterations):
            key = f"bench:{i}"
            latencies.append(self._measure(self.client.set, key, value))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies),
            'throughput_mb': (iterations * size) / (sum(latencies) / 1000) / (1024 * 1024)
        }
    
    def _benchmark_get(self, iterations: int, size: int) -> Dict[str, float]:
        """Benchmark GET command"""
        # First populate keys
        value = 'x' * size
        for i in range(iterations):
            self.client.set(f"bench:{i}", value)
        
        # Then measure GET
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(self.client.get, f"bench:{i}"))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_incr(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark INCR command"""
        self.client.set("counter", "0")
        latencies = []
        for _ in range(iterations):
            latencies.append(self._measure(self.client.execute, 'INCR', 'counter'))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_lpush(self, iterations: int, size: int) -> Dict[str, float]:
        """Benchmark LPUSH command"""
        values = ['x' * size] * 10  # Push 10 items per operation
        latencies = []
        for i in range(iterations):
            key = f"list:{i}"
            latencies.append(self._measure(self.client.execute, 'LPUSH', key, *values))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_lrange(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark LRANGE command"""
        # Setup lists first
        for i in range(iterations):
            self.client.execute('LPUSH', f"list:{i}", *[str(x) for x in range(100)])
        
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'LRANGE', f"list:{i}", '0', '99'
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    # Additional benchmark methods for other data structures...
    def _benchmark_hset(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark HSET command"""
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'HSET', f"hash:{i}", "field", "value"
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_hgetall(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark HGETALL command"""
        # Setup hashes first
        for i in range(iterations):
            self.client.execute('HSET', f"hash:{i}", *[f"f{x}" for x in range(10)], *["v"]*10)
        
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'HGETALL', f"hash:{i}"
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_sadd(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark SADD command"""
        members = [str(x) for x in range(10)]
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'SADD', f"set:{i}", *members
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_zadd(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark ZADD command"""
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'ZADD', f"zset:{i}", "1.0", "member"
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_xadd(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark XADD command"""
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'XADD', f"stream:{i}", '*', "field", "value"
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_geoadd(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark GEOADD command"""
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'GEOADD', "locations", "13.361389", "38.115556", f"place:{i}"
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_bitops(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark bitmap operations"""
        latencies = []
        for i in range(iterations):
            latencies.append(self._measure(
                self.client.execute, 'SETBIT', "bits", str(i), "1"
            ))
        
        return {
            'ops_per_sec': iterations / (sum(latencies) / 1000),
            'avg_latency': statistics.mean(latencies)
        }
    
    def _benchmark_pipeline(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark pipelined commands"""
        # Measure 100 commands in a pipeline
        def pipeline_ops():
            pipe = []
            for i in range(100):
                pipe.append(('SET', f"pipe:{i}", "value"))
            self.client.execute_pipeline(pipe)
        
        durations = []
        for _ in range(iterations):
            durations.append(self._measure(pipeline_ops))
        
        return {
            'ops_per_sec': (iterations * 100) / (sum(durations) / 1000),
            'avg_latency': statistics.mean(durations) / 100  # per command
        }
    
    def _benchmark_concurrent(self, iterations: int, _) -> Dict[str, float]:
        """Benchmark concurrent client performance"""
        import concurrent.futures
        def worker():
            self.client.set("conc:test", "value")
            return self.client.get("conc:test")
        
        latencies = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            start = time.perf_counter()
            futures = [executor.submit(worker) for _ in range(iterations)]
            concurrent.futures.wait(futures)
            duration = time.perf_counter() - start
        
        return {
            'ops_per_sec': iterations / duration,
            'total_time': duration * 1000
        }
    
    def print_results(self):
        """Print formatted benchmark results"""
        print("\nBenchmark Results:")
        print("=" * 60)
        for test, metrics in self.results.items():
            print(f"\n{test.upper()} Benchmark:")
            for metric, value in metrics.items():
                if 'latency' in metric:
                    print(f"{metric.replace('_', ' ').title()}: {value:.2f} ms")
                elif 'mb' in metric:
                    print(f"{metric.replace('_', ' ').title()}: {value:.2f} MB/s")
                else:
                    print(f"{metric.replace('_', ' ').title()}: {value:,.2f}")

def benchmark_cli():
    """Command line interface for benchmarks"""
    parser = argparse.ArgumentParser(description='Redis-like server benchmark tool')
    parser.add_argument('--host', default='localhost', help='Server hostname')
    parser.add_argument('--port', type=int, default=6379, help='Server port')
    parser.add_argument('--iterations', type=int, default=1000, help='Operations per test')
    parser.add_argument('--size', type=int, default=100, help='Payload size in bytes')
    parser.add_argument('--tests', nargs='+', default=['ping', 'set', 'get'], 
                       help='Tests to run (ping, set, get, incr, lpush, lrange, etc.)')
    
    args = parser.parse_args()
    
    benchmark = RedisBenchmark(args.host, args.port)
    benchmark.run_tests(args.tests, args.iterations, args.size)
    benchmark.print_results()

if __name__ == '__main__':
    benchmark_cli()