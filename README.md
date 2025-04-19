# Python Redis-like

## Overview

This project implements a core key-value store with comprehensive data types and functionalities, inspired by Redis. It supports operations for strings, lists, sets, hashes, sorted sets, streams, geospatial data, bitmaps, and more.

## Features

- **Core Operations**: SET, GET, DEL, EXISTS, and key expiration.
- **Data Types**: Strings, JSON, lists, sets, hashes, sorted sets, streams, geospatial data, bitmaps, bitfields, probabilistic data structures, and time series.
- **Server Functionality**: Client-server communication, transactions, and publish/subscribe mechanisms.
- **Persistence**: Append-Only File (AOF) and point-in-time snapshots.
- **Replication**: Master-slave data replication.
- **Client Development**: A client class for connecting to the server.
- **Performance Optimization**: Enhanced client-side caching, pipelining optimization, and benchmarking tools.
- **Security**: Authentication mechanisms and TLS support.
- **Monitoring and Management**: Tools for monitoring performance and managing the data store.

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   ```
2. Navigate to the project directory:
   ```
   cd python-kv-store
   ```
3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

To start the server, run:
```
python -m src.server.server
```

To use the client, run:
```
python -m src.client.client
```

## Testing

To run the tests, use:
```
pytest tests/
```
## License

This project is licensed under the MIT License.