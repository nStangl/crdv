from collections import defaultdict
import re
import subprocess
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

started = False
done = False
csv = ""
thread = None


# load interface
interface = ''
for line in subprocess.check_output('route').decode('utf-8').splitlines():
    if line.startswith('default'):
        interface = line.split()[7]
assert interface != ''
print(f'Using interface {interface}')


def measure():
    global started, done, csv
    done = False
    started = True
    out = open('out.txt', 'w')
    tcpdump = subprocess.Popen(['tcpdump', '-tt', '-i', interface, '-n', '-q', '--micro'], stdout=out)
    ip_addr = subprocess.check_output(['ip', 'addr', 'show', interface]).decode('utf-8')
    selfIp = re.findall(r'inet (\d+\.\d+\.\d+\.\d+)', ip_addr)[0]

    while not done:
        time.sleep(1)
    tcpdump.terminate()
    out.close()

    data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    begin = None
    
    with open('out.txt') as f:
        for line in f:
            try:
                time_, src, dst, length = re.findall(
                    r'(\d+\.\d+) IP (\d+\.\d+\.\d+\.\d+)\.\d+ > (\d+\.\d+\.\d+\.\d+)\.\d+: \w+ (\d+)', 
                    line
                )[0]
                
                if not begin:
                    begin = float(time_)
                
                if src == selfIp:
                    data[int(float(time_) - begin)][dst]['bytes_sent'] += int(length)
                    data[int(float(time_) - begin)][dst]['packets_sent'] += 1
                else:
                    data[int(float(time_) - begin)][src]['bytes_recv'] += int(length)
                    data[int(float(time_) - begin)][src]['packets_recv'] += 1
            except:
                pass

    csv = "time,ip,bytes_sent,bytes_recv,packets_sent,packets_recv\n"
    last_time = 0
    for time_, ipStats in sorted(data.items()):
        # fill empty seconds
        for t in range(last_time + 1, time_):
            csv += f"{t},-,0,0,0,0\n"
        last_time = time_
        for ip, stats in ipStats.items():
            csv += f"{time_},{ip},{stats['bytes_sent']},{stats['bytes_recv']},{stats['packets_sent']},{stats['packets_recv']}\n"


def stop():
    global thread, started, done
    done = True

    if thread is not None:
        thread.join()

    thread = None
    started = False


class CustomHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global started, done, csv, thread

        if self.path == '/start':
            stop()
            thread = threading.Thread(target=measure)
            thread.start()
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write("ok".encode())

        elif self.path == '/stop':
            if not started:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write("not started".encode())
                return

            stop()
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(str(csv).encode())
            started = False


server = HTTPServer(('0.0.0.0', 8080), CustomHandler)
server.serve_forever()
