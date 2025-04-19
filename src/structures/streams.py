python-kv-store
├── src
│   ├── core
│   │   ├── __init__.py
│   │   ├── data_store.py
│   │   ├── data_types.py
│   │   └── exceptions.py
│   ├── storage
│   │   ├── __init__.py
│   │   ├── persistence.py
│   │   └── replication.py
│   ├── server
│   │   ├── __init__.py
│   │   ├── server.py
│   │   └── handlers.py
│   ├── client
│   │   ├── __init__.py
│   │   ├── client.py
│   │   └── connection.py
│   ├── structures
│   │   ├── __init__.py
│   │   ├── strings.py
│   │   ├── lists.py
│   │   ├── sets.py
│   │   ├── hashes.py
│   │   ├── sorted_sets.py
│   │   ├── streams.py
│   │   ├── geo.py
│   │   ├── bitmaps.py
│   │   ├── time_series.py
│   │   └── vectors.py
│   └── utils
│       ├── __init__.py
│       ├── monitoring.py
│       └── security.py
├── tests
│   ├── __init__.py
│   ├── test_core.py
│   ├── test_structures.py
│   └── test_server.py
├── benchmarks
│   ├── __init__.py
│   └── performance.py
├── requirements.txt
├── setup.py
└── README.md