import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from lsm import LSM
import time
from urllib.parse import parse_qs, urlparse
import re

# Initialize LSM database
db = LSM('influx_data.ldb')

def print_db_contents():
    print("Current database contents:")
    with db.cursor() as cursor:
        for key, value in cursor:
            print(f"Key: {key.decode()}, Value: {value.decode()}")

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
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        self.save_data(post_data)
        
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Data received and forwarded")
        
        print_db_contents()  # Print database contents after each insert
    
    def do_GET(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        if 'query' not in query_params:
            self.send_error(400, "Missing 'query' parameter")
            return
        
        query = query_params['query'][0]
        print(f"Received query: {query}")  # Debug print
        print_db_contents()  # Print database contents before processing query
        
        results = self.process_query(query)
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode())
    
    def save_data(self, data):
        key = str(int(time.time() * 1000)).encode()
        db[key] = data.encode()
        print(f"Saved data: {data}")
    
    def process_query(self, query):
        parser = QueryParser(query)
        parsed_query = parser.get_parsed_query()
        
        results = []
        with db.cursor() as cursor:
            for key, value in cursor:
                data = value.decode()
                if self.matches_query(data, parsed_query):
                    parsed_data = self.parse_influx_data(data)
                    if parsed_data:
                        results.append(parsed_data)
        
        results = self.apply_aggregations(results, parsed_query)
        results = self.apply_grouping(results, parsed_query)
        results = self.apply_pagination(results, parsed_query)
        
        return results

    def matches_query(self, data, parsed_query):
        if parsed_query['from'] not in data:
            return False
        
        for condition in parsed_query['where']:
            if '=' in condition:
                key, value = condition.split('=')
                if key.strip() not in data or value.strip().replace("'", "") not in data:
                    return False
        
        if parsed_query['time_range']:
            # Implement time range check here
            pass
        
        return True

    def apply_aggregations(self, results, parsed_query):
        # Implement aggregations here
        return results

    def apply_grouping(self, results, parsed_query):
        # Implement grouping here
        return results

    def apply_pagination(self, results, parsed_query):
        offset = parsed_query['offset'] or 0
        limit = parsed_query['limit']
        if limit:
            return results[offset:offset+limit]
        return results[offset:]

    def parse_influx_data(self, data):
        parts = data.split()
        if len(parts) < 2:
            return None
        
        measurement_tags = parts[0].split(',')
        measurement = measurement_tags[0]
        tags = dict(tag.split('=') for tag in measurement_tags[1:] if '=' in tag)
        
        fields = dict(field.split('=') for field in parts[1].split(',') if '=' in field)
        
        timestamp = parts[2] if len(parts) > 2 else str(int(time.time() * 1e9))
        
        return {
            "measurement": measurement,
            "tags": tags,
            "fields": fields,
            "timestamp": timestamp
        }

def run_server(host, port):
    server_address = (host, port)
    httpd = HTTPServer(server_address, LSMDataHandler)
    print(f"LSM Data Saver and Query Handler running on http://{host}:{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    HOST = "localhost"
    PORT = 8087
    run_server(HOST, PORT)
