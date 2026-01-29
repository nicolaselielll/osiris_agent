"""
BT.CPP Groot2 ZMQ Event Collector

Connects to the Groot2 ZMQ publisher from BT.CPP and collects:
- Tree structure (on connect and on change)
- Node status updates
- Breakpoint events

Events are forwarded to the WebBridge for transmission over WebSocket.
"""

import json
import random
import struct
import threading
import time
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import IntEnum

import zmq

# Hardcoded ports for now (Groot2 default)
GROOT_SERVER_PORT = 1666  # REQ/REP for tree structure
GROOT_PUBLISHER_PORT = 1667  # PUB/SUB for status updates
GROOT_HOST = "127.0.0.1"

# ZMQ timeouts
ZMQ_RECV_TIMEOUT_MS = 1000
ZMQ_RECONNECT_INTERVAL = 2.0


class NodeStatus(IntEnum):
    """BT.CPP NodeStatus enum values"""
    IDLE = 0
    RUNNING = 1
    SUCCESS = 2
    FAILURE = 3
    SKIPPED = 4

    @classmethod
    def to_string(cls, value: int) -> str:
        try:
            return cls(value).name
        except ValueError:
            return f"UNKNOWN({value})"


@dataclass
class BTNode:
    """Represents a node in the behavior tree"""
    uid: int
    name: str
    tag: str  # Node type (e.g., "Sequence", "Action", "Condition")
    status: str = "IDLE"


@dataclass
class BTTree:
    """Represents the full behavior tree structure"""
    tree_id: Optional[str] = None
    nodes: Dict[int, BTNode] = field(default_factory=dict)
    xml: str = ""
    structure: Dict[str, Any] = field(default_factory=dict)


class BTCollector:
    """
    Collects BT events from Groot2 ZMQ publisher and forwards them.
    
    Usage:
        collector = BTCollector(event_callback)
        collector.start()
        # ... later ...
        collector.stop()
    """

    def __init__(
        self,
        event_callback: Callable[[Dict[str, Any]], None],
        host: str = GROOT_HOST,
        server_port: int = GROOT_SERVER_PORT,
        publisher_port: int = GROOT_PUBLISHER_PORT,
        logger=None
    ):
        """
        Initialize the BT collector.
        
        Args:
            event_callback: Function to call with parsed events (dict)
            host: Groot2 host address
            server_port: Groot2 server port (for tree structure requests)
            publisher_port: Groot2 publisher port (for status updates)
            logger: Optional logger (uses print if None)
        """
        self._event_callback = event_callback
        self._host = host
        self._server_port = server_port
        self._publisher_port = publisher_port
        self._logger = logger
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._context: Optional[zmq.Context] = None
        
        self._current_tree: Optional[BTTree] = None
        self._last_statuses: Dict[int, str] = {}

    def _log_info(self, msg: str):
        if self._logger:
            self._logger.info(msg)
        else:
            print(f"[BTCollector INFO] {msg}")

    def _log_debug(self, msg: str):
        if self._logger:
            self._logger.debug(msg)

    def _log_error(self, msg: str):
        if self._logger:
            self._logger.error(msg)
        else:
            print(f"[BTCollector ERROR] {msg}")

    def _log_warn(self, msg: str):
        if self._logger:
            self._logger.warn(msg)
        else:
            print(f"[BTCollector WARN] {msg}")

    def start(self):
        """Start the collector in a background thread."""
        if self._running:
            self._log_warn("BTCollector already running")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._log_info(f"BTCollector started (server={self._host}:{self._server_port}, pub={self._publisher_port})")

    def stop(self):
        """Stop the collector."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        self._log_info("BTCollector stopped")

    def _run_loop(self):
        """Main collector loop - runs in background thread."""
        self._context = zmq.Context()
        
        while self._running:
            try:
                self._collect_loop()
            except Exception as e:
                self._log_error(f"Collection error: {e}")
            
            if self._running:
                self._log_info(f"Reconnecting in {ZMQ_RECONNECT_INTERVAL}s...")
                time.sleep(ZMQ_RECONNECT_INTERVAL)
        
        if self._context:
            self._context.term()
            self._context = None

    def _collect_loop(self):
        """Single collection session - connects, gets tree, subscribes to updates."""
        # Create sockets
        req_socket = self._context.socket(zmq.REQ)
        req_socket.setsockopt(zmq.RCVTIMEO, ZMQ_RECV_TIMEOUT_MS)
        req_socket.setsockopt(zmq.LINGER, 0)
        
        sub_socket = self._context.socket(zmq.SUB)
        sub_socket.setsockopt(zmq.RCVTIMEO, ZMQ_RECV_TIMEOUT_MS)
        sub_socket.setsockopt(zmq.LINGER, 0)
        sub_socket.setsockopt(zmq.SUBSCRIBE, b"")  # Subscribe to all messages
        
        try:
            # Connect to server for tree structure
            server_addr = f"tcp://{self._host}:{self._server_port}"
            self._log_info(f"Connecting to Groot2 server: {server_addr}")
            req_socket.connect(server_addr)
            
            # Connect to publisher for status updates
            pub_addr = f"tcp://{self._host}:{self._publisher_port}"
            self._log_info(f"Connecting to Groot2 publisher: {pub_addr}")
            sub_socket.connect(pub_addr)
            
            # Request tree structure
            self._request_tree_structure(req_socket)
            
            # Main status update loop
            self._log_info("Listening for BT status updates...")
            while self._running:
                try:
                    msg = sub_socket.recv(zmq.NOBLOCK)
                    self._handle_status_message(msg)
                except zmq.Again:
                    # No message available, continue
                    time.sleep(0.01)
                except Exception as e:
                    self._log_error(f"Error receiving status: {e}")
                    break
                    
        finally:
            req_socket.close()
            sub_socket.close()

    def _request_tree_structure(self, socket: zmq.Socket):
        """Request and parse tree structure from Groot2 server."""
        try:
            # Groot2 protocol requires proper binary header
            # Header: protocol(uint8)=2, type(uint8)='T', unique_id(uint32)
            protocol = 2
            req_type = ord('T')  # 'T' = FULLTREE request
            unique_id = random.getrandbits(32)
            header = struct.pack('<BBI', protocol, req_type, unique_id)
            
            self._log_debug("Requesting tree structure...")
            socket.send(header)
            
            response = socket.recv()
            self._parse_tree_response(response)
            
        except zmq.Again:
            self._log_warn("Timeout waiting for tree structure")
        except Exception as e:
            self._log_error(f"Error requesting tree: {e}")

    def _parse_tree_response(self, data: bytes):
        """
        Parse tree structure response from Groot2.
        
        The Groot2 protocol sends:
        - First 4 bytes: number of nodes (uint32)
        - Then for each node: uid (uint16), instance_name (null-terminated), registration_name (null-terminated)
        - Finally: XML string of the tree
        """
        if len(data) < 4:
            self._log_warn(f"Tree response too short: {len(data)} bytes")
            return
        
        try:
            offset = 0
            
            # Read number of nodes
            num_nodes = struct.unpack('<I', data[offset:offset+4])[0]
            offset += 4
            self._log_debug(f"Tree has {num_nodes} nodes")
            
            tree = BTTree()
            nodes_list = []
            
            # Parse each node
            for _ in range(num_nodes):
                if offset + 2 > len(data):
                    break
                    
                # Read UID (uint16)
                uid = struct.unpack('<H', data[offset:offset+2])[0]
                offset += 2
                
                # Read instance name (null-terminated string)
                name_end = data.find(b'\x00', offset)
                if name_end == -1:
                    break
                instance_name = data[offset:name_end].decode('utf-8', errors='replace')
                offset = name_end + 1
                
                # Read registration name / tag (null-terminated string)
                tag_end = data.find(b'\x00', offset)
                if tag_end == -1:
                    break
                registration_name = data[offset:tag_end].decode('utf-8', errors='replace')
                offset = tag_end + 1
                
                node = BTNode(uid=uid, name=instance_name, tag=registration_name)
                tree.nodes[uid] = node
                nodes_list.append({
                    'uid': uid,
                    'name': instance_name,
                    'tag': registration_name
                })
                
                self._log_debug(f"  Node {uid}: {instance_name} ({registration_name})")
            
            # Remaining data is XML
            if offset < len(data):
                tree.xml = data[offset:].decode('utf-8', errors='replace').rstrip('\x00')
            
            self._current_tree = tree
            self._last_statuses.clear()
            
            # Send tree event
            event = {
                'type': 'bt_tree',
                'timestamp': time.time(),
                'tree_id': tree.tree_id,
                'tree': {},  # Could add hierarchical structure here
                'nodes': nodes_list,
                'xml': tree.xml
            }
            
            self._log_info(f"Received tree structure: {len(nodes_list)} nodes")
            self._event_callback(event)
            
        except Exception as e:
            self._log_error(f"Error parsing tree response: {e}")

    def _handle_status_message(self, data: bytes):
        """
        Parse status update message from Groot2 publisher.
        
        The Groot2 protocol sends:
        - Sequence of (uid: uint16, status: uint8) pairs
        - Can also be prefixed with a message type byte
        """
        if len(data) < 1:
            return
        
        try:
            offset = 0
            changes = []
            
            # Check if first byte is a message type indicator
            # Groot2 may send different message types
            first_byte = data[0]
            
            # If it looks like a status message (small number indicating type)
            if first_byte <= 10 and len(data) > 1:
                msg_type = first_byte
                offset = 1
                
                if msg_type == 2:  # Breakpoint message type in some versions
                    self._handle_breakpoint_message(data[offset:])
                    return
            
            # Parse status updates: (uid: uint16, status: uint8) pairs
            while offset + 3 <= len(data):
                uid = struct.unpack('<H', data[offset:offset+2])[0]
                status_val = data[offset+2]
                offset += 3
                
                status_str = NodeStatus.to_string(status_val)
                
                # Only report changes
                if self._last_statuses.get(uid) != status_str:
                    self._last_statuses[uid] = status_str
                    
                    # Get node info if available
                    node_name = ""
                    node_tag = ""
                    if self._current_tree and uid in self._current_tree.nodes:
                        node = self._current_tree.nodes[uid]
                        node_name = node.name
                        node_tag = node.tag
                    
                    changes.append({
                        'uid': uid,
                        'name': node_name,
                        'tag': node_tag,
                        'status': status_str
                    })
            
            if changes:
                event = {
                    'type': 'bt_status',
                    'timestamp': time.time(),
                    'tree_id': self._current_tree.tree_id if self._current_tree else None,
                    'changes': changes
                }
                
                self._log_debug(f"Status update: {len(changes)} changes")
                self._event_callback(event)
                
        except Exception as e:
            self._log_error(f"Error parsing status message: {e}")

    def _handle_breakpoint_message(self, data: bytes):
        """Handle breakpoint messages from Groot2."""
        try:
            # Breakpoint data is typically raw bytes that can be hex-encoded
            parts = []
            
            # Split into chunks if needed (some protocols send multi-part)
            chunk_size = 64
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i+chunk_size]
                parts.append(chunk.hex())
            
            event = {
                'type': 'bt_breakpoint',
                'timestamp': time.time(),
                'parts': parts
            }
            
            self._log_debug("Breakpoint event received")
            self._event_callback(event)
            
        except Exception as e:
            self._log_error(f"Error parsing breakpoint message: {e}")


# For testing standalone
if __name__ == "__main__":
    def print_event(event):
        print(f"EVENT: {json.dumps(event, indent=2)}")
    
    collector = BTCollector(print_event)
    collector.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.stop()
