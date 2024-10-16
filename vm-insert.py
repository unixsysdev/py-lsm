import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests

class InfluxDataHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')
        
        # Echo the data to the console
        print(f"Received data: {post_data}")
        
        # Forward the data to the LSM Data Saver
        self.send_to_lsm_saver(post_data)
        
        # Send a simple acknowledgement back to the client
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Data received and forwarded")
    
    def send_to_lsm_saver(self, data):
        try:
            response = requests.post('http://localhost:8087', data=data)
            print(f"LSM Data Saver response: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error sending data to LSM Data Saver: {e}")

def run_server(host, port):
    server_address = (host, port)
    httpd = HTTPServer(server_address, InfluxDataHandler)
    print(f"Influx Data Listener running on http://{host}:{port}")
    httpd.serve_forever()

if __name__ == "__main__":
    HOST = "0.0.0.0"  # Listen on all available interfaces
    PORT = 8086  # Default InfluxDB port
    run_server(HOST, PORT)
