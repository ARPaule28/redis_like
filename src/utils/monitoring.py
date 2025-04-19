import time
import threading
import psutil  # Requires psutil package
from typing import Dict, Any, List, Optional
from collections import deque, defaultdict
from dataclasses import dataclass

@dataclass
class ServerStats:
    uptime: float
    connected_clients: int
    commands_processed: int
    memory_used: float
    cpu_usage: float
    keyspace_hits: int
    keyspace_misses: int
    ops_per_sec: float
    latency_stats: Dict[str, float]
    command_stats: Dict[str, Dict[str, int]]

class Monitoring:
    """Server monitoring and statistics collection"""
    def __init__(self):
        self._start_time = time.time()
        self._lock = threading.Lock()
        self._command_count = 0
        self._keyspace_hits = 0
        self._keyspace_misses = 0
        self._latency_history = deque(maxlen=1000)
        self._command_stats = defaultdict(lambda: {'calls': 0, 'micros': 0})
        self._connected_clients = 0
        self._sample_interval = 5  # seconds
        self._cpu_samples = deque(maxlen=10)
        self._memory_samples = deque(maxlen=10)
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_system,
            daemon=True
        )
        self._monitor_thread.start()

    def _monitor_system(self) -> None:
        """Background thread to monitor system resources"""
        while self._running:
            cpu = psutil.cpu_percent(interval=1)
            mem = psutil.virtual_memory().used / (1024 * 1024)  # MB
            
            with self._lock:
                self._cpu_samples.append(cpu)
                self._memory_samples.append(mem)
            
            time.sleep(self._sample_interval)

    def record_command(self, command: str, latency_micros: int) -> None:
        """Record command execution statistics"""
        with self._lock:
            self._command_count += 1
            self._latency_history.append(latency_micros)
            self._command_stats[command]['calls'] += 1
            self._command_stats[command]['micros'] += latency_micros

    def record_keyspace_hit(self) -> None:
        """Record a keyspace hit"""
        with self._lock:
            self._keyspace_hits += 1

    def record_keyspace_miss(self) -> None:
        """Record a keyspace miss"""
        with self._lock:
            self._keyspace_misses += 1

    def client_connected(self) -> None:
        """Record new client connection"""
        with self._lock:
            self._connected_clients += 1

    def client_disconnected(self) -> None:
        """Record client disconnection"""
        with self._lock:
            self._connected_clients -= 1

    def get_stats(self) -> ServerStats:
        """Get current server statistics"""
        with self._lock:
            now = time.time()
            uptime = now - self._start_time
            ops_per_sec = self._command_count / uptime if uptime > 0 else 0
            
            # Calculate latency percentiles
            latencies = sorted(self._latency_history)
            latency_stats = {
                'p50': self._percentile(latencies, 50),
                'p95': self._percentile(latencies, 95),
                'p99': self._percentile(latencies, 99),
                'max': latencies[-1] if latencies else 0
            }
            
            # Calculate average CPU and memory
            avg_cpu = sum(self._cpu_samples) / len(self._cpu_samples) if self._cpu_samples else 0
            avg_mem = sum(self._memory_samples) / len(self._memory_samples) if self._memory_samples else 0
            
            # Prepare command stats
            formatted_cmd_stats = {}
            for cmd, stats in self._command_stats.items():
                formatted_cmd_stats[cmd] = {
                    'calls': stats['calls'],
                    'avg_micros': stats['micros'] / stats['calls'] if stats['calls'] else 0
                }
            
            return ServerStats(
                uptime=uptime,
                connected_clients=self._connected_clients,
                commands_processed=self._command_count,
                memory_used=avg_mem,
                cpu_usage=avg_cpu,
                keyspace_hits=self._keyspace_hits,
                keyspace_misses=self._keyspace_misses,
                ops_per_sec=ops_per_sec,
                latency_stats=latency_stats,
                command_stats=formatted_cmd_stats
            )

    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile from sorted data"""
        if not data:
            return 0.0
        k = (len(data) - 1) * (percentile / 100)
        f = int(k)
        c = k - f
        if f + 1 < len(data):
            return data[f] + (data[f + 1] - data[f]) * c
        return data[f]

    def close(self) -> None:
        """Clean up monitoring resources"""
        self._running = False
        self._monitor_thread.join()

    def get_info_sections(self) -> Dict[str, str]:
        """Get server info in Redis-like sections"""
        stats = self.get_stats()
        return {
            'server': self._format_server_info(stats),
            'clients': self._format_clients_info(stats),
            'memory': self._format_memory_info(stats),
            'stats': self._format_stats_info(stats),
            'commandstats': self._format_command_stats(stats),
            'latencystats': self._format_latency_stats(stats)
        }

    def _format_server_info(self, stats: ServerStats) -> str:
        return (
            f"redis_version:0.1\n"
            f"os:{psutil.OS_RELEASE}\n"
            f"arch_bits:{64 if psutil.ARCH.endswith('64') else 32}\n"
            f"uptime_in_seconds:{int(stats.uptime)}\n"
            f"uptime_in_days:{int(stats.uptime // 86400)}\n"
        )

    def _format_clients_info(self, stats: ServerStats) -> str:
        return (
            f"connected_clients:{stats.connected_clients}\n"
            f"blocked_clients:0\n"
        )

    def _format_memory_info(self, stats: ServerStats) -> str:
        return (
            f"used_memory:{stats.memory_used:.2f}\n"
            f"used_memory_human:{stats.memory_used:.2f}M\n"
            f"mem_fragmentation_ratio:1.00\n"
        )

    def _format_stats_info(self, stats: ServerStats) -> str:
        return (
            f"total_connections_received:{stats.commands_processed}\n"
            f"total_commands_processed:{stats.commands_processed}\n"
            f"instantaneous_ops_per_sec:{stats.ops_per_sec:.2f}\n"
            f"keyspace_hits:{stats.keyspace_hits}\n"
            f"keyspace_misses:{stats.keyspace_misses}\n"
        )

    def _format_command_stats(self, stats: ServerStats) -> str:
        lines = []
        for cmd, data in stats.command_stats.items():
            lines.append(
                f"cmdstat_{cmd}:"
                f"calls={data['calls']},"
                f"usec={data['avg_micros'] * data['calls']:.0f},"
                f"usec_per_call={data['avg_micros']:.2f}"
            )
        return "\n".join(lines)

    def _format_latency_stats(self, stats: ServerStats) -> str:
        return (
            f"latency_percentiles_usec_50:{stats.latency_stats['p50']:.2f}\n"
            f"latency_percentiles_usec_95:{stats.latency_stats['p95']:.2f}\n"
            f"latency_percentiles_usec_99:{stats.latency_stats['p99']:.2f}\n"
            f"latency_percentiles_usec_max:{stats.latency_stats['max']:.2f}\n"
        )