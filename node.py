import argparse, json , socket, threading, time, sys, signal
from http.server import BaseHTTPRequestHandler, HTTPServer

from urllib.parse import urlparse
from sudoku_solver import SudokuSolver
from sudoku import Sudoku  
from time import sleep
import logging

lock = threading.Lock()
shutdown_event = threading.Event()
solved_sudoku = None

partial_solutions = {}
expected_solutions = 0
handicap = 0
task_assignments = {}
last_assigned_peer = -1
last_broadcast_time = 0
broadcast_interval = 2  



#/stats
stats = {
    "all": {
        "solved": 0,
        "validations": 0
    },

    "nodes" : []
}

#/network
network = {} 



#helper functions
def print_grid(grid):
    grid_str = "\n".join(" ".join(str(num) if num != 0 else '.' for num in row) for row in grid)
    return grid_str


def should_broadcast():
    global last_broadcast_time
    current_time = time.time()
    if (current_time - last_broadcast_time) > broadcast_interval:
        last_broadcast_time = current_time
        return True
    return False



def stats_update_node_vals(node_address, validations):
    global stats
    updated = False

    with lock:
        for node in stats["nodes"]:
            if node["address"] == node_address:
                node["validations"] += validations
                updated = True
                break
        else:
            stats["nodes"].append({"address": node_address, "validations": validations})
            updated = True

        stats["all"]["validations"] += validations

    if updated and should_broadcast():
        broadcast_stats_update()



def broadcast_stats_update():
    print(f"[BROADCAST] Broadcasting updated stats list to all peers.")
    node = f'{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}'
    message = {'command': 'stats_update', 'stats': stats}

    with lock:
        peers = list(network.keys())
    
    for peer in peers:
        if peer != node:
            send_to_peer((peer.split(':')[0], int(peer.split(':')[1])), message)
            sleep(0.1)  #small delay to prevent overwhelming the network




def signal_handler(signal, frame):
    print('Ctrl+C pressed, shutting down...')
    shutdown_event.set()
    



def broadcast_network_update():
    print(f"[NETWORK UPDATE] Broadcasting updated network list to all peers.")
    message = {'command': 'network_update', 'network': network}
    node = f'{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}'
    
    peers = list(network.keys())
     

    for peer in peers:
        if peer != node:
            send_to_peer((peer.split(':')[0], int(peer.split(':')[1])), message)
            sleep(0.1)  #small delay to prevent overwhelming the network




def heartbeat(shutdown_event):
    #maintain persistent connections and periodically check if peers are alive.
    connections = {}
    node = f'{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}'

    while not shutdown_event.is_set():
        with lock:
            peers = list(network.keys())

        for peer in peers:
            if peer != node:
                try:
                    if peer not in connections:
                        connections[peer] = socket.create_connection((peer.split(':')[0], int(peer.split(':')[1])), timeout=5)
                    logging.debug(f"[INFO] Sending message to peer {peer}: ping")
                    connections[peer].sendall(json.dumps({'command': 'ping'}).encode('utf-8'))
                
                    #wait for pong response
                    connections[peer].settimeout(10)

                    try:
                        data = connections[peer].recv(1024)
                        if data:
                            response = json.loads(data.decode('utf-8'))
                            if response.get('command') == 'pong':
                                time.sleep(5)  #interval between heartbeats
                                continue

                    except socket.timeout:
                        print(f"Peer {peer} failed to respond.")

                except socket.error:
                    #handling the failure
                    print(f"Peer {peer} failed to respond.")

                with lock:
                    if peer in network:
                        del network[peer]

                    if peer in network[node]:
                        network[node].remove(peer)

                    broadcast_network_update()
                    stats["nodes"] = [node for node in stats["nodes"] if node["address"] != peer]


                if peer in connections:
                    conn = connections.pop(peer, None)
                    if conn:
                        conn.close()
    
                #reassign tasks from the failed peer
                reassign_tasks(peer)

    #clean up all connections on shutdown
    for conn in connections.values():
        conn.close()





def solve_sudoku_partially(board, empty_cells, start_index, end_index, node):
    solver = SudokuSolver()
    partial_board = [row[:] for row in board] #copy
    indices = empty_cells[start_index:end_index]

    for row, col in indices:
        if partial_board[row][col] == 0:
            for num in range(1, 10):
                if solver.is_valid(partial_board, row, col, num):
                    partial_board[row][col] = num
                    break

    logging.debug(f"[SOLVE PARTIAL] Partial solution found with {solver.validations} validations.")
    logging.debug(print_grid(partial_board))

    response = {'command': 'solve_answer', 'peer': node, 'sudoku': partial_board, 'start_index': start_index, 'validations': solver.validations}
    return response



def combine_solutions(partial_solutions):
    combined_grid = [[0] * 9 for _ in range(9)]

    for solution in partial_solutions.values():
        if solution is not None:
            for i in range(9):
                for j in range(9):
                    if combined_grid[i][j] == 0 and solution[i][j] != 0:
                        combined_grid[i][j] = solution[i][j]

    return combined_grid




def assign_task(task):
    global network
    global task_assignments
    global last_assigned_peer
    peers = list(network.keys())

    if peers:
        last_assigned_peer = (last_assigned_peer + 1)

        if last_assigned_peer >= len(peers): #wrap around
            last_assigned_peer = 0

        new_peer = peers[last_assigned_peer]
        task_assignments.setdefault(new_peer, []).append(task)
        message = {'command': 'solve', 'peer': task['peer'], 'sudoku': task['sudoku'], 'start_index': task['start_index'], 'depth': task['depth']}
        send_to_peer((new_peer.split(':')[0], int(new_peer.split(':')[1])), message)




def reassign_tasks(failed_peer):
    global task_assignments
    print(f"[REASSIGN] Reassigning tasks from failed peer {failed_peer}.")
    with lock:
        tasks = task_assignments.get(failed_peer, [])
        if tasks:
            del task_assignments[failed_peer]
            for task in tasks:
                assign_task(task)





####################### PEER 2 PEER ############################

#Start P2P server
def run_p2p_server(port, shutdown_event):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  #keep connections alive

        s.bind(('0.0.0.0', port))  #bind to all network interfaces
        s.listen()
        print(f'[STARTING] Starting P2P server on port {port}')
        s.settimeout(1) 

        with lock:
            node = f"{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}"
            print(f"[NEW] Peer {node} is now connected.")

            if node not in network:
                network[node] = []

            stats["nodes"].append({"address": node, "validations": 0})

        try:    
            while not shutdown_event.is_set():
                try:
                    conn, addr = s.accept()
                    threading.Thread(target=handle_peer_connection, args=(conn, addr, shutdown_event)).start()
                
                except socket.timeout:
                    continue

                except Exception as e:
                    print(f"Server socket error: {e}")

        finally:
            print("P2P server shutdown complete.")
            s.close()
            sys.exit(0)



#send messages to peers
def send_to_peer(peer, message):
    node = f'{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}'
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            logging.debug(f"[INFO] Connecting to peer {peer}...")
            s.connect(peer)
            logging.debug(f"[INFO] Sending message to peer {peer}: {message}")
            s.sendall(json.dumps(message).encode('utf-8'))

    except ConnectionRefusedError:
        print(f"[ERROR] Connection to peer {peer} refused.")
        peer_str = f"{peer[0]}:{peer[1]}"
        with lock:
            if peer_str in network:
                del network[peer_str]
            if peer_str in network[node]:
                network[node].remove(peer_str)

        reassign_tasks(peer)  #reassign tasks if the peer is unreachable
        print(f"[DISCONNECTED] Peer {peer} has disconnected.")
        broadcast_network_update()
  
    except Exception as e:
        print(f"[ERROR] Failed to send message to peer {peer}: {e}")


                      
#handle connections
def handle_peer_connection(conn, addr, shutdown_event):
    global network
    global solved_sudoku
    global stats
    global expected_solutions
    global partial_solutions
    
    node = f'{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}'

    try:
        while not shutdown_event.is_set():
            try:
                data = conn.recv(1024)
                if data:
                    message = json.loads(data.decode('utf-8'))
                    logging.debug(f"[INFO] Received message from peer {message}")

                    #join
                    if message.get('command') == 'join':
                        new_peer = message.get('peer')
                    
                        with lock:

                            if new_peer not in network:
                                network[new_peer] = []

                            #add this node to the peers network                  
                            if node not in network[new_peer]:
                                network[new_peer].append(node)

                            #add new peer to this nodes network
                            if new_peer not in network[node]:
                                network[node].append(new_peer)

                        broadcast_network_update()


                    elif message.get('command') == 'ping':
                        response = {'command': 'pong'}
                        conn.sendall(json.dumps(response).encode('utf-8'))
                    


                    #solve
                    elif message.get('command') == 'solve':
                        sudoku_msg = message.get('sudoku')
                        peer = message.get('peer')
                        start_index = message.get('start_index')
                        depth = message.get('depth')
                        

                        print(f"[SOLVE] Solving Sudoku puzzle from peer {peer}.")
                        empty_cells = [(i, j) for i in range(9) for j in range(9) if sudoku_msg[i][j] == 0]


                        response = solve_sudoku_partially(sudoku_msg, empty_cells, start_index, depth, node)

                        send_to_peer((peer.split(':')[0], int(peer.split(':')[1])), response)
 

                    #solve_answer
                    elif message.get('command') == 'solve_answer':
                        start_index = message.get('start_index')
                        sudoku_part = message.get('sudoku')
                        peer = message.get('peer')
                        validations = message.get('validations')
                        logging.debug(f"[PARTIAL SOLVE ANSWER] Received solved Sudoku puzzle from peer {peer}.")

                        with lock:
                            partial_solutions[start_index] = sudoku_part
                                
                        stats_update_node_vals(peer, validations)



                    #network_update
                    elif message.get('command') == 'network_update':
                        updated_network = message.get('network')
                        with lock:
                            #only update if the network state has changed
                            if network != updated_network:
                                network = updated_network


                    #stats_update
                    elif message.get('command') == 'stats_update':
                        updated_stats = message.get('stats')
                        with lock:
                            #only update if the stats have  changed
                            if stats != updated_stats:
                                stats = updated_stats

                if not data:
                    continue

            except socket.timeout:
                continue 
            
            except ConnectionResetError:
                break
            
            except Exception as e:
                print(f"[ERROR] Error: {e}")
                break

    finally:
        time.sleep(6)  #wait for heartbeat
  



####################### HTTP ############################

class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == "/stats":
            self.handle_stats()


        elif path == "/network":
            self.handle_network()

        else:
            self.send_response(404)
            self.end_headers()



    def do_POST(self):
        parsed_path = urlparse(self.path)
        path = parsed_path.path
        
        if path == "/solve":
            self.handle_solve()

        else:
            self.send_response(404)
            self.end_headers()



    def handle_stats(self):
    
        response = json.dumps(stats).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(response))
        self.end_headers()
        self.wfile.write(response)


    def handle_network(self):
    
        response = json.dumps(network).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(response))
        self.end_headers()
        self.wfile.write(response)



    def handle_solve(self):
        global solved_sudoku
        global partial_solutions
        global expected_solutions
        global task_assignments
        global handicap

        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length).decode('utf-8')        
        node = f'{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}'

        try:
            request_data = json.loads(post_data)
            sudoku = request_data.get("sudoku")
         

            if sudoku:
                with lock:
                    #split the puzzle into parts for multiple nodes
                    peers = list(network.keys())
                    empty_cells = [(i, j) for i in range(9) for j in range(9) if sudoku[i][j] == 0]
                    num_nodes = len(peers)  
                    depth_per_node = len(empty_cells) // num_nodes
                    remainder = len(empty_cells) % num_nodes
                    expected_solutions = num_nodes
                 


                current_index = 0
                for i in range(num_nodes):
                    start_index = current_index
                    additional_cell = 1 if i < remainder else 0
                    end_index = start_index + depth_per_node + additional_cell
                    task = {'command': 'solve', 'sudoku': sudoku, 'start_index': start_index, 'depth': end_index, 'peer': node}
                    assign_task(task) 

                    current_index = end_index


                event = threading.Event()
                wait_thread = threading.Thread(target=self.wait_for_solution, args=(event,))
                wait_thread.start()
                wait_thread.join(timeout=260)  #wait max 30 seconds
        

                if event.is_set():
                    combined_sudoku = combine_solutions(partial_solutions)
                    checker = Sudoku(combined_sudoku)

                    if handicap > 0:
                        sleep(handicap / 1000)


                    if checker.check() == True:
                        solved_sudoku = combined_sudoku
                        stats["all"]["solved"] += 1
                        print(f"[SOLVED] Solved Sudoku puzzle:")
                        print(print_grid(solved_sudoku))
                        

                    else:
                        print(f"[INVALID] Invalid Sudoku solution.")
                        self.send_response(400)  # Bad Request

         

                if solved_sudoku:
                    response = {'sudoku': solved_sudoku}
                    response_data = json.dumps(response).encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', len(response_data))
                    self.end_headers()
                    self.wfile.write(response_data)


                else:
                    self.send_response(408)  # Request Timeout
                    self.end_headers()


            else:
                self.send_response(400)  #bad Request
                self.end_headers()


        except json.JSONDecodeError as e:
            print(f"JSONDecodeError: {e}")
            self.send_response(400)  #bad Request
            self.end_headers()


    def wait_for_solution(self, event):
        global partial_solutions
        global expected_solutions

        while not event.is_set() and not shutdown_event.is_set():
            if len(partial_solutions) == expected_solutions:
                event.set()
            sleep(0.5)  # Check every half-second


# Start HTTP server
def run_http_server(port):
    server = HTTPServer(('0.0.0.0', port), RequestHandler)
    print(f'[STARTING] Starting HTTP server on port {port}')
    server.serve_forever()




if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description='Distributed Sudoku Solver Node')
    parser.add_argument('-p', '--http_port', type=int, required=True, help='HTTP port for the node')
    parser.add_argument('-s', '--p2p_port', type=int, required=True, help='P2P port for the node')
    parser.add_argument('-a', '--anchor', type=str, help='Anchor node address (host:port)')
    parser.add_argument('-H', '--handicap', type=int, default=0, help='Handicap in milliseconds for validation function')
    parser.add_argument('-l', '--log', action='store_true', help='Enable debug logging')

    args = parser.parse_args()
    if args.handicap:
        handicap = args.handicap

    if args.log:
        logging.basicConfig(level=logging.DEBUG)
  
#START SERVERS


    #threads for servers
    http_thread = threading.Thread(target=run_http_server, args=(args.http_port,))
    p2p_thread = threading.Thread(target=run_p2p_server, args=(args.p2p_port, shutdown_event))
    heartbeat_thread = threading.Thread(target=heartbeat, args=(shutdown_event,))


    http_thread.start()
    p2p_thread.start()
    heartbeat_thread.start()


    #if an anchor node is specified, join the P2P network through the anchor node
    if args.anchor:
        anchor_host, anchor_port = args.anchor.split(':')
        anchor_peer = (anchor_host, int(anchor_port))
        print(f"[INFO] Attempting to join anchor node at {anchor_peer}")
        send_to_peer(anchor_peer, {'command': 'join', 'peer': f'{socket.gethostbyname(socket.gethostname())}:{args.p2p_port}'})


    #wait for the threads to finish
    try:
        http_thread.join()
        p2p_thread.join()
        heartbeat_thread.join()
        

    except KeyboardInterrupt:
        shutdown_event.set()
        http_thread.join()
        p2p_thread.join()
        heartbeat_thread.join()


    print("Servers have been gracefully shutdown.")


       