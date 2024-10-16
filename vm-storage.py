import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from lsm import LSM
import time
from urllib.parse import parse_qs, urlparse
import re

# Initialize LSM database
db = LSM('influx_data.ldb')

class LSMDataHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        # Process and save the data
        self.save_data(post_data)
        
        # Send acknowledgement back to the client
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Data saved successfully")
    
    def do_GET(self):
        # Handle GET requests for querying data
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        if 'query' not in query_params:
            self.send_error(400, "Missing 'query' parameter")
            return
        
        query = query_params['query'][0]
        results = self.process_query(query)
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode())
    
    def save_data(self, data):
        # Use current timestamp as key
        key = str(int(time.time() * 1000)).encode()
        
        # Save data to LSM database
        db[key] = data.encode()
        
        print(f"Saved data: {data}")
    
    def process_query(self, query):
        # This is a simplified query processor
        match = re.match(r'select\s+(.+)\s+from\s+(.+)\s+where\s+(.+)', query, re.IGNORECASE)
        if not match:
            return {"error": "Invalid query format"}
        
        fields, measurement, conditions = match.groups()
        
        results = []
        # Iterate over the LSM database using a cursor
        with db.cursor() as cursor:
            for key, value in cursor:
                data = value.decode()
                if measurement in data and self.check_conditions(data, conditions):
                    parsed_data = self.parse_influx_data(data)
                    if parsed_data:
                        results.append(parsed_data)
        
        return results
    
    def check_conditions(self, data, conditions):
        condition_parts = conditions.split('and')
        for part in condition_parts:
            if '=' in part:
                key, value = part.split('=')
                if key.strip() not in data or value.strip() not in data:
                    return False
        return True
    
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
