from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
from urllib.parse import urlencode, parse_qs, unquote
import json

class QueryHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = parse_qs(self.path[2:])  # Remove leading '/?'
        if 'query' not in parsed_path:
            self.send_error(400, "Missing 'query' parameter")
            return

        query = unquote(parsed_path['query'][0])  # Decode the URL-encoded query
        print(f"Received query: {query}")  # Debug print

        results = self.forward_query(query)

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(results, indent=2).encode())

    def forward_query(self, query):
        storage_url = "http://localhost:8087"
        params = {'query': query}
        try:
            response = requests.get(storage_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": f"Error communicating with storage service: {str(e)}"}

def run_server(host, port):
    server_address = (host, port)
    httpd = HTTPServer(server_address, QueryHandler)
    print(f"Query Server running on http://{host}:{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    HOST = "localhost"
    PORT = 8088
    run_server(HOST, PORT)
