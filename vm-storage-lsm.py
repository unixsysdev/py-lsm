import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from collections import OrderedDict
import time
from urllib.parse import parse_qs, urlparse
import re
import os
import pickle
from datetime import datetime, timezone

class MemTable:
    def __init__(self, max_size=1000):
        self.data = OrderedDict()
        self.max_size = max_size

    def put(self, key, value):
        self.data[key] = value
        if len(self.data) > self.max_size:
            return self.flush()
        return None

    def get(self, key):
        return self.data.get(key)

    def flush(self):
        flushed_data = list(self.data.items())
        self.data.clear()
        return flushed_data

class SSTable:
    def __init__(self, level, index):
        self.filename = f"level_{level}_sstable_{index}.db"
        self.data = OrderedDict()

    def put(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def save(self):
        with open(self.filename, 'wb') as f:
            pickle.dump(self.data, f)

    def load(self):
        if os.path.exists(self.filename):
            with open(self.filename, 'rb') as f:
                self.data = pickle.load(f)

class LSMTree:
    def __init__(self, max_levels=4):
        self.memtable = MemTable()
        self.levels = [[] for _ in range(max_levels)]
        self.max_levels = max_levels
        self.load_existing_sstables()

    def load_existing_sstables(self):
        for level in range(self.max_levels):
            i = 0
            while True:
                sstable = SSTable(level, i)
                if os.path.exists(sstable.filename):
                    sstable.load()
                    self.levels[level].append(sstable)
                    i += 1
                else:
                    break

    def put(self, key, value):
        flushed_data = self.memtable.put(key, value)
        if flushed_data:
            self._compact(flushed_data, 0)

    def get(self, key):
        value = self.memtable.get(key)
        if value:
            return value

        for level in range(self.max_levels):
            for sstable in self.levels[level]:
                value = sstable.get(key)
                if value:
                    return value

        return None

    def _compact(self, data, level):
        if level >= self.max_levels:
            return

        new_sstable = SSTable(level, len(self.levels[level]))
        for key, value in data:
            new_sstable.put(key, value)

        new_sstable.save()
        self.levels[level].append(new_sstable)

        if len(self.levels[level]) > 2:  # Simple compaction strategy
            self._merge_and_compact(level)

    def _merge_and_compact(self, level):
        merged_data = OrderedDict()
        for sstable in self.levels[level]:
            merged_data.update(sstable.data)

        self.levels[level] = []  # Clear current level
        self._compact(merged_data.items(), level + 1)  # Compact to next level

def print_db_contents(lsm_tree):
    print("Current memtable contents:")
    for key, value in lsm_tree.memtable.data.items():
        print(f"Key: {key}, Value: {value}")
    print("Current SSTable contents:")
    for level in range(lsm_tree.max_levels):
        for sstable in lsm_tree.levels[level]:
            for key, value in sstable.data.items():
                print(f"Level {level}, Key: {key}, Value: {value}")

class QueryParser:
    def __init__(self, query_string):
        self.query_string = query_string
        self.parsed_query = {
            'select': [],
            'from': '',
            'where': [],
            'group_by': [],
            'time_range': {},
            'limit': None,
            'offset': None
        }
        self.parse()

    def parse(self):
        parts = self.query_string.split()
        i = 0
        while i < len(parts):
            if parts[i].upper() == 'SELECT':
                i += 1
                while i < len(parts) and parts[i].upper() != 'FROM':
                    self.parsed_query['select'].append(parts[i].strip(','))
                    i += 1
            elif parts[i].upper() == 'FROM':
                i += 1
                self.parsed_query['from'] = parts[i]
            elif parts[i].upper() == 'WHERE':
                i += 1
                while i < len(parts) and parts[i].upper() not in ['GROUP', 'TIME', 'LIMIT', 'OFFSET']:
                    self.parsed_query['where'].append(parts[i])
                    i += 1
            elif parts[i].upper() == 'GROUP' and parts[i+1].upper() == 'BY':
                i += 2
                while i < len(parts) and parts[i].upper() not in ['TIME', 'LIMIT', 'OFFSET']:
                    self.parsed_query['group_by'].append(parts[i].strip(','))
                    i += 1
            elif parts[i].upper() == 'TIME' and parts[i+1].upper() == 'RANGE':
                i += 2
                self.parsed_query['time_range']['start'] = parts[i]
                i += 2  # Skip 'TO'
                self.parsed_query['time_range']['end'] = parts[i]
            elif parts[i].upper() == 'LIMIT':
                i += 1
                self.parsed_query['limit'] = int(parts[i])
            elif parts[i].upper() == 'OFFSET':
                i += 1
                self.parsed_query['offset'] = int(parts[i])
            i += 1

    def get_parsed_query(self):
        return self.parsed_query

class LSMDataHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.lsm_tree = LSMTree()
        super().__init__(*args, **kwargs)

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        self.save_data(post_data)
        
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Data received and saved")
        
        print_db_contents(self.lsm_tree)  # Print database contents after each insert
  
    def do_GET(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        print(f"Parsed URL: {parsed_url}")
        print(f"Query params: {query_params}")

        if 'query' not in query_params:
            self.send_error(400, "Missing 'query' parameter")
            return

        query = query_params['query'][0]
        print(f"Raw received query: {query}")

        # Extract the actual query by finding the last occurrence of 'SELECT'
        actual_query = query[query.upper().rindex('SELECT'):]
        print(f"Processed query: {actual_query}")

        results = self.process_query(actual_query)
        print(f"Query results: {results}")

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode())
    
    def save_data(self, data):
        parts = data.split()
        if len(parts) < 2:
            print(f"Invalid data format: {data}")
            return

        measurement_tags = parts[0].split(',')
        measurement = measurement_tags[0]
        tags = dict(tag.split('=') for tag in measurement_tags[1:] if '=' in tag)
        fields = dict(field.split('=') for field in parts[1].split(',') if '=' in field)
        timestamp = parts[2] if len(parts) > 2 else str(int(time.time() * 1e9))

        parsed_data = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "timestamp": timestamp
        }

        key = f"{measurement}:{timestamp}"
        self.lsm_tree.put(key, json.dumps(parsed_data))
        print(f"Saved data: {parsed_data}")
    
    def process_query(self, query):
        parser = QueryParser(query)
        parsed_query = parser.get_parsed_query()
        
        results = []
        for level in range(self.lsm_tree.max_levels):
            for sstable in self.lsm_tree.levels[level]:
                for key, value in sstable.data.items():
                    parsed_data = json.loads(value)
                    if self.matches_query(parsed_data, parsed_query):
                        results.append(parsed_data)
        
        # Check memtable
        for key, value in self.lsm_tree.memtable.data.items():
            parsed_data = json.loads(value)
            if self.matches_query(parsed_data, parsed_query):
                results.append(parsed_data)

        results = self.apply_aggregations(results, parsed_query)
        results = self.apply_grouping(results, parsed_query)
        results = self.apply_pagination(results, parsed_query)
        
        return results

    def matches_query(self, data, parsed_query):
        if parsed_query['from'] != data['measurement']:
            return False
        
        for condition in parsed_query['where']:
            if '=' in condition:
                key, value = condition.split('=')
                key = key.strip()
                value = value.strip().replace("'", "").replace('"', '')
                if key in data['tags']:
                    if data['tags'][key] != value:
                        return False
                elif key in data['fields']:
                    if str(data['fields'][key]) != value:
                        return False
                else:
                    return False
        
        if parsed_query['time_range']:
            timestamp = int(data['timestamp'])
            start_time = self.parse_time(parsed_query['time_range']['start'])
            end_time = self.parse_time(parsed_query['time_range']['end'])
            if timestamp < start_time or timestamp > end_time:
                return False
        
        return True

    def parse_time(self, time_str):
        # Parse ISO 8601 format to timestamp
        dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1e9)  # Convert to nanoseconds

    def apply_aggregations(self, results, parsed_query):
        if not parsed_query['select'] or parsed_query['select'][0] == '*':
            return results

        aggregated_results = []
        for agg_function in parsed_query['select']:
            if agg_function.upper().startswith(('COUNT', 'SUM', 'AVG', 'MIN', 'MAX')):
                field = agg_function[agg_function.index('(')+1:agg_function.index(')')]
                agg_value = self.calculate_aggregation(results, agg_function.split('(')[0].upper(), field)
                aggregated_results.append({agg_function: agg_value})

        return aggregated_results if aggregated_results else results

    def calculate_aggregation(self, results, function, field):
        values = [float(result['fields'].get(field, 0)) for result in results]
        if function == 'COUNT':
            return len(values)
        elif function == 'SUM':
            return sum(values)
        elif function == 'AVG':
            return sum(values) / len(values) if values else 0
        elif function == 'MIN':
            return min(values) if values else None
        elif function == 'MAX':
            return max(values) if values else None

    def apply_grouping(self, results, parsed_query):
        if not parsed_query['group_by']:
            return results

        grouped_results = {}
        for result in results:
            group_key = tuple(result['tags'].get(tag, '') for tag in parsed_query['group_by'])
            if group_key not in grouped_results:
                grouped_results[group_key] = []
            grouped_results[group_key].append(result)

        return [
            {
                'group': dict(zip(parsed_query['group_by'], group)),
                'results': self.apply_aggregations(group_results, parsed_query)
            }
            for group, group_results in grouped_results.items()
        ]

    def apply_pagination(self, results, parsed_query):
        offset = parsed_query['offset'] or 0
        limit = parsed_query['limit']
        if limit:
            return results[offset:offset+limit]
        return results[offset:]

def run_server(host, port):
    server_address = (host, port)
    httpd = HTTPServer(server_address, LSMDataHandler)
    print(f"LSM Data Saver and Query Handler running on http://{host}:{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    HOST = "localhost"
    PORT = 8087
    run_server(HOST, PORT)
