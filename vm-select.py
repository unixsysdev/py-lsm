from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
from urllib.parse import urlencode

class QueryHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Forward the entire query to the LSM Data Saver
        lsm_saver_url = f"http://localhost:8087{self.path}"
        
        try:
            response = requests.get(lsm_saver_url)
            
            # Forward the response back to the client
            self.send_response(response.status_code)
            for header, value in response.headers.items():
                self.send_header(header, value)
            self.end_headers()
            self.wfile.write(response.content)
        
        except requests.exceptions.RequestException as e:
            self.send_error(500, f"Error communicating with LSM Data Saver: {str(e)}")

def run_server(host, port):
    server_address = (host, port)
    httpd = HTTPServer(server_address, QueryHandler)
    print(f"Query Server running on http://{host}:{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    HOST = "localhost"
    PORT = 8088  # Different port from the LSM Data Saver
    run_server(HOST, PORT)
