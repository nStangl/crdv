from http.server import HTTPServer, BaseHTTPRequestHandler
import time
from urllib.parse import urlparse, parse_qs
import subprocess
import re
import socket


# load interface
interface = ''
for line in subprocess.check_output('route').decode('utf-8').splitlines():
    if line.startswith('default'):
        interface = line.split()[7]
assert interface != ''
print(f'Using interface {interface}')


def hostnameToIP(hostname):
    # already an ip address
    if re.match(r'\d+\.\d+\.\d+\.\d+', hostname):
        return hostname
    return socket.gethostbyname(hostname)


def block(ips, blockTime):
    for ip in ips:
        if ip == "all":
            subprocess.check_call(["ip", "link", "set", interface, "down"])
        else:
            subprocess.check_call(["ip", "route", "add", "blackhole", hostnameToIP(ip)])

    time.sleep(blockTime)

    for ip in ips:
        if ip == "all":
            subprocess.check_call(["ip", "link", "set", interface, "up"])
        else:
            subprocess.check_call(["ip", "route", "del", "blackhole", hostnameToIP(ip)])


class CustomHandler(BaseHTTPRequestHandler):
    def ok(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write("ok".encode())


    def badRequest(self, msg):
        self.send_response(400)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(msg.encode())


    def do_POST(self):
        path = urlparse(self.path)
        query = parse_qs(path.query)

        if path.path == '/block':
            if 'ip' not in query:
                return self.badRequest("missing ip")
            if 'time' not in query:
                return self.badRequest("missing time")
            block(query['ip'], int(query['time'][0]))
            self.ok()


server = HTTPServer(('0.0.0.0', 8083), CustomHandler)
server.serve_forever()
