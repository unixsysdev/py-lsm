# VM Clone Py

VM Clone Py is a toy project demonstrating a simple implementation of a time-series data storage and query system using Python and a basic LSM (Log-Structured Merge-tree) approach.

## Project Components

1. `vm-storage.py`: The main storage engine that saves incoming data and processes queries.
2. `vm-insert.py`: A script to insert data into the storage engine.
3. `vm-select.py`: A script to query data from the storage engine.
4. `control.sh`: A Bash script to control (start, stop, restart, and check status of) the Python scripts.

## Prerequisites

- Python 3.x
- `lsm` library (`pip install lsm`)
- `requests` library (`pip install requests`)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/vm-clone-py.git
   cd vm-clone-py
   ```

2. Install the required Python libraries:
   ```
   pip install lsm requests
   ```

## Usage

### Starting the Services

Use the control script to start all services:

```
./control.sh start
```

This will start `vm-storage.py`, `vm-insert.py`, and `vm-select.py` in the background.

### Inserting Data

Data insertion is handled by `vm-insert.py`. It listens for POST requests with Influx Line Protocol formatted data.

Example using curl:

```
curl -X POST -d 'measurement,tag1=value1,tag2=value2 field1=10,field2=20 1465839830100400200' http://localhost:8086
```

### Querying Data

Queries are handled by `vm-select.py`, which forwards them to `vm-storage.py` for processing. Use GET requests with a `query` parameter.

Example using curl:

```
curl "http://localhost:8088/?query=select%20field1%20from%20measurement%20where%20tag1=value1"
```

### Controlling Services

The `control.sh` script provides the following commands:

- Start all services: `./control.sh start`
- Stop all services: `./control.sh stop`
- Restart all services: `./control.sh restart`
- Check status of all services: `./control.sh status`

You can also control individual services by specifying the script name:

```
./control.sh start vm-storage.py
./control.sh stop vm-insert.py
./control.sh restart vm-select.py
./control.sh status vm-storage.py
```

## Project Structure

- `vm-storage.py`: Main storage engine
- `vm-insert.py`: Data insertion script
- `vm-select.py`: Query processing script
- `control.sh`: Service control script
- `influx_data.ldb`: Main database file
- `influx_data.ldb-log`: Write-ahead log file
- `influx_data.ldb-shm`: Shared memory file for database operations

## Limitations

This is a toy project and has several limitations:

1. It uses a basic LSM implementation that may not include all optimizations of a full LSM-tree.
2. The query language is very simplistic and doesn't support complex operations.
3. There's no authentication or security measures in place.
4. It may not be suitable for handling large volumes of data or high concurrent access.

## Contributing

This is a demonstration project, but feel free to fork it and expand upon it. If you have suggestions or improvements, please open an issue or submit a pull request.

## License

This project is open source and available under the [MIT License](LICENSE).
