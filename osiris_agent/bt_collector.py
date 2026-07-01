"""
BT.CPP Groot2 ZMQ Event Collector

Connects to the Groot2 ZMQ publisher from BT.CPP and collects:
- Tree structure (on connect and on change)
- Node status updates
- Breakpoint events

Events are forwarded to the WebBridge for transmission over WebSocket.
"""

import json
import struct
import threading
import time
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass, field
from enum import IntEnum

import zmq
import msgpack
import random  # added to support unique_id generation in requests

# Default Groot2 configuration (industry standard)
DEFAULT_SERVER_PORT = 1667  # REQ/REP for tree structure and status polling
DEFAULT_PUBLISHER_PORT = 1668  # PUB/SUB for breakpoint notifications only
DEFAULT_HOST = "127.0.0.1"

# ZMQ timeouts
ZMQ_RECV_TIMEOUT_MS = 500
ZMQ_RECONNECT_INTERVAL = 0.5
TRANSITION_POLL_INTERVAL = 0.05  # Poll transitions every 50ms (fast enough to not miss any)
SNAPSHOT_POLL_INTERVAL = 0.5   # Poll full status snapshot for tree-change detection


class NodeStatus(IntEnum):
    """BT.CPP NodeStatus enum values (includes extended states)"""
    IDLE = 0
    RUNNING = 1
    SUCCESS = 2
    FAILURE = 3
    SKIPPED = 4
    # Extended states (BT.CPP internal)
    IDLE_WAS_RUNNING = 11
    IDLE_WAS_SUCCESS = 12
    IDLE_WAS_FAILURE = 13

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
        host: Optional[str] = None,
        server_port: Optional[int] = None,
        publisher_port: Optional[int] = None,
        logger=None
    ):
        """
        Initialize the BT collector.
        
        Args:
            event_callback: Function to call with parsed events (dict)
            host: Groot2 host address (default: 127.0.0.1)
            server_port: Groot2 server port (default: 1667)
            publisher_port: Groot2 publisher port (default: 1668)
            logger: Optional logger (uses print if None)
        """
        self._event_callback = event_callback
        self._host = host if host is not None else DEFAULT_HOST
        self._server_port = server_port if server_port is not None else DEFAULT_SERVER_PORT
        self._publisher_port = publisher_port if publisher_port is not None else DEFAULT_PUBLISHER_PORT
        self._logger = logger
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._context: Optional[zmq.Context] = None
        
        self._current_tree: Optional[BTTree] = None
        self._last_statuses: Dict[int, str] = {}
        self._blackboard: Dict[str, Any] = {}
        self._last_emitted_blackboard: Optional[str] = None  # hash of last emitted bb, for change detection
        self._subtree_names: str = ""
        self._recording: bool = False
        self._last_snapshot_poll: float = 0.0

    def _log_info(self, msg: str):
        if self._logger:
            self._logger.info(f'[bt] {msg}')
        else:
            print(f"[BTCollector] {msg}")

    def _log_debug(self, msg: str):
        if self._logger:
            self._logger.debug(f'[bt] {msg}')
        else:
            print(f"[BTCollector DEBUG] {msg}")

    def _log_error(self, msg: str):
        if self._logger:
            self._logger.error(f'[bt] {msg}')
        else:
            print(f"[BTCollector ERROR] {msg}")

    def _log_warn(self, msg: str):
        if self._logger:
            self._logger.warning(f'[bt] {msg}')
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
        """Single collection session - connects, gets tree, polls for status updates."""
        # Create REQ socket for tree structure AND status polling
        req_socket = self._context.socket(zmq.REQ)
        req_socket.setsockopt(zmq.RCVTIMEO, ZMQ_RECV_TIMEOUT_MS)
        req_socket.setsockopt(zmq.LINGER, 0)
        
        try:
            # Connect to server for tree structure and status polling
            server_addr = f"tcp://{self._host}:{self._server_port}"
            self._log_info(f"Connecting to Groot2 server: {server_addr}")
            req_socket.connect(server_addr)
            
            # Main loop - request tree first, then poll for transitions
            self._log_info("Starting BT collection loop...")
            tree_received = False
            _miss_count = 0  # consecutive timeouts without ever receiving a tree
            
            while self._running:
                try:
                    # Request tree structure if we don't have it yet
                    if not tree_received:
                        tree_received = self._request_tree_structure(req_socket)
                        if not tree_received:
                            time.sleep(ZMQ_RECONNECT_INTERVAL)
                            continue
                        # Fetch initial status snapshot + start recording transitions
                        try:
                            self._poll_status(req_socket, suppress_event=True)
                            self._start_recording(req_socket)
                        except Exception:
                            pass
                        if self._current_tree:
                            self._log_info("Sending initial tree event with statuses")
                            _miss_count = 0
                            self._emit_current_tree_event()
                    
                    # Poll transitions (exact 1:1 match with terminal output)
                    self._poll_transitions(req_socket)
                    
                    # Periodic status snapshot for tree-change detection only (no emit)
                    now = time.time()
                    if now - self._last_snapshot_poll >= SNAPSHOT_POLL_INTERVAL:
                        self._last_snapshot_poll = now
                        self._poll_status(req_socket, suppress_event=True)
                    
                    # Blackboard
                    if self._subtree_names:
                        self._request_blackboard(req_socket, self._subtree_names)
                    
                    time.sleep(TRANSITION_POLL_INTERVAL)
                    
                except zmq.Again:
                    # Timeout - need to recreate socket because REQ is in bad state
                    had_tree = bool(self._current_tree)
                    if had_tree:
                        # We had a live tree and lost it — always warn
                        self._log_warn("Timeout - recreating socket...")
                    elif _miss_count == 0:
                        # First miss only: let the user know we're waiting
                        self._log_info(f"BT executor not found on tcp://{self._host}:{self._server_port}, will keep retrying silently...")
                    # else: no log — avoid spam while waiting for executor to start
                    _miss_count += 1
                    req_socket.close()
                    req_socket = self._context.socket(zmq.REQ)
                    req_socket.setsockopt(zmq.RCVTIMEO, ZMQ_RECV_TIMEOUT_MS)
                    req_socket.setsockopt(zmq.LINGER, 0)
                    req_socket.connect(server_addr)
                    tree_received = False  # Need to re-request tree after reconnect
                    if self._current_tree:
                        self._emit_bt_gone_event()
                        self._current_tree = None
                        self._last_statuses.clear()
                        self._blackboard = {}
                        self._subtree_names = ""
                        self._last_emitted_blackboard = None
                        self._recording = False
                    time.sleep(ZMQ_RECONNECT_INTERVAL)
                    
                except zmq.ZMQError as e:
                    if "current state" in str(e):
                        # Socket in bad state, recreate it
                        self._log_warn(f"Socket in bad state, recreating: {e}")
                        req_socket.close()
                        req_socket = self._context.socket(zmq.REQ)
                        req_socket.setsockopt(zmq.RCVTIMEO, ZMQ_RECV_TIMEOUT_MS)
                        req_socket.setsockopt(zmq.LINGER, 0)
                        req_socket.connect(server_addr)
                        tree_received = False
                        if self._current_tree:
                            self._emit_bt_gone_event()
                            self._current_tree = None
                            self._last_statuses.clear()
                            self._blackboard = {}
                            self._subtree_names = ""
                            self._recording = False
                            self._last_emitted_blackboard = None
                        time.sleep(ZMQ_RECONNECT_INTERVAL)
                    else:
                        self._log_error(f"ZMQ error: {e}")
                        time.sleep(0.5)
                        
                except Exception as e:
                    self._log_error(f"Error in collection loop: {e}")
                    time.sleep(0.5)
                    
        finally:
            req_socket.close()

    def _poll_status(self, socket: zmq.Socket, suppress_event: bool = False):
        """Poll for status update via REQ/REP."""
        # Send STATUS request: protocol=2, type='S'
        protocol = 2
        req_type = ord('S')  # 'S' = STATUS request
        unique_id = random.getrandbits(32)
        header = struct.pack('<BBI', protocol, req_type, unique_id)
        
        socket.send(header, zmq.SNDMORE)
        socket.send(b"")
        
        parts = socket.recv_multipart()
        if parts and len(parts) >= 2:
            resp_tree_id = self._extract_tree_id_from_header(parts[0])
            if resp_tree_id and (not self._current_tree or self._current_tree.tree_id != resp_tree_id):
                self._log_warn(
                    f"Tree changed behind endpoint: {self._current_tree.tree_id if self._current_tree else None} -> {resp_tree_id}. Refreshing FULLTREE..."
                )
                if self._request_tree_structure(socket):
                    self._emit_current_tree_event()
            self._handle_status_message(parts, resp_tree_id, suppress_event=suppress_event)

    def _extract_tree_id_from_header(self, hdr: bytes) -> Optional[str]:
        """
        Groot2 multipart header (22 bytes):
          protocol(1), type(1), unique_id(4), tree_id(16)
        Returns tree_id as hex string or None.
        """
        if not hdr or len(hdr) < 22:
            return None
        return hdr[6:22].hex()

    def _emit_bt_gone_event(self):
        """Emit a bt_tree event with empty values to signal the BT is no longer available."""
        self._event_callback({
            "type": "bt_tree",
            "timestamp": time.time(),
            "tree_id": None,
            "tree": None,
            "nodes": [],
        })

    def _emit_current_tree_event(self):
        if not self._current_tree:
            return
        nodes_with_status = []
        for nd in getattr(self._current_tree, "nodes_list", []):
            uid = nd.get("uid")
            nd_copy = dict(nd)
            nd_copy["status"] = self._last_statuses.get(uid, NodeStatus.IDLE.name)
            nodes_with_status.append(nd_copy)
        event = {
            "type": "bt_tree",
            "timestamp": time.time(),
            "tree_id": self._current_tree.tree_id,
            "tree": self._current_tree.structure,
            "nodes": nodes_with_status,
            "blackboard": self._blackboard,
        }
        self._log_debug(f"Sending tree event ({len(nodes_with_status)} nodes, {len(self._blackboard)} bb entries)")
        self._event_callback(event)

    def _request_tree_structure(self, socket: zmq.Socket) -> bool:
        """Request and parse tree structure from Groot2 server. Returns True if successful."""
        # Groot2 protocol: multipart message with binary header
        # Header: protocol(uint8)=2, type(uint8)='T', unique_id(uint32)
        protocol = 2
        req_type = ord('T')  # 'T' = FULLTREE request
        unique_id = random.getrandbits(32)
        header = struct.pack('<BBI', protocol, req_type, unique_id)
        
        self._log_debug(f"Requesting tree structure...")
        socket.send(header, zmq.SNDMORE)
        socket.send(b"")  # Empty body
        
        # Receive multipart response
        parts = socket.recv_multipart()
        self._log_debug(f"Tree response: {len(parts)} parts")
        
        if parts and len(parts) >= 2:
            return self._parse_tree_response(parts, socket)
        else:
            self._log_warn("Invalid tree response (expected 2+ parts)")
            return False

    def _parse_tree_response(self, parts: list, socket: zmq.Socket) -> bool:
        """
        Parse tree structure response from Groot2.
        Returns True if successful.
        
        Response format (multipart):
        - Part 0: Header (22 bytes): protocol(1), type(1), unique_id(4), tree_id(16)
        - Part 1: XML string of the tree
        """
        if not parts:
            self._log_warn("Empty tree response")
            return False
        
        self._log_debug(f"Parsing tree response: {len(parts)} parts")
        
        try:
            # Part 0: Header
            hdr = parts[0]
            tree_id = None
            if len(hdr) >= 22:
                resp_protocol, resp_type, resp_id = struct.unpack('<BBI', hdr[:6])
                tree_id = hdr[6:22].hex()
                self._log_info(f"Tree header: protocol={resp_protocol}, type={chr(resp_type)}, tree_id={tree_id[:16]}...")
            elif len(hdr) >= 6:
                # Check if error response
                try:
                    err = hdr.decode('utf-8')
                    self._log_error(f"Error response: {err}")
                    if len(parts) > 1:
                        self._log_error(f"Detail: {parts[1].decode('utf-8')}")
                    return False
                except:
                    self._log_warn(f"Short header: {len(hdr)} bytes")
            
            # Part 1: XML
            if len(parts) < 2:
                self._log_warn("No XML part in response")
                return False
            
            xml = parts[1].decode('utf-8', errors='replace')
            self._log_info(f"Received tree XML: {len(xml)} chars")
            
            # Parse XML to extract nodes
            tree = BTTree()
            tree.tree_id = tree_id  # Set the tree_id from header
            tree.xml = xml
            nodes_list = self._parse_xml_tree(xml, tree)

            if not nodes_list:
                self._log_warn("No nodes extracted from tree XML")
                return False

            # store parsed tree and node list; do not emit tree event here
            tree.nodes_list = nodes_list
            self._current_tree = tree
            self._last_statuses.clear()
            self._log_info(f"Tree structure received: {len(nodes_list)} nodes")

            # Request blackboard data using subtree names from XML.
            # Seed _last_emitted_blackboard so the next poll cycle doesn't
            # emit a duplicate bt_blackboard (data already in bt_tree).
            subtree_names = self._extract_subtree_names(xml)
            self._subtree_names = subtree_names
            if subtree_names:
                self._log_debug(f"Requesting blackboard for subtrees: {subtree_names}")
                self._request_blackboard(socket, subtree_names)
                self._last_emitted_blackboard = json.dumps(self._blackboard, sort_keys=True, default=str)

            return True
            
        except Exception as e:
            import traceback
            self._log_error(f"Error parsing tree response: {e}\n{traceback.format_exc()}")
            return False

    def _parse_xml_tree(self, xml: str, tree: BTTree) -> list:
        """Parse XML tree and extract nodes with UIDs."""
        import xml.etree.ElementTree as ET
        
        nodes_list = []
        try:
            root = ET.fromstring(xml)
            self._extract_nodes_from_xml(root, tree, nodes_list)
            
            # Also build hierarchical tree structure
            tree.structure = self._build_tree_structure(root)
            
            self._log_info(f"Extracted {len(nodes_list)} nodes from XML")
        except ET.ParseError as e:
            self._log_error(f"XML parse error: {e}")
        
        return nodes_list
    
    def _build_tree_structure(self, elem) -> dict:
        """Recursively build hierarchical tree structure from XML."""
        # Find the BehaviorTree element
        behavior_tree = elem.find('.//BehaviorTree')
        if behavior_tree is None:
            return {}
        
        # Build from the first child of BehaviorTree (the root node)
        children = list(behavior_tree)
        if not children:
            return {}
        
        return self._element_to_tree_node(children[0])
    
    def _element_to_tree_node(self, elem) -> dict:
        """Convert an XML element to a tree node dict with children."""
        node = {
            'tag': elem.tag,
            'name': elem.attrib.get('name', elem.attrib.get('ID', elem.tag)),
            'attributes': dict(elem.attrib),
        }
        
        # Add uid if present
        uid_str = elem.attrib.get('_uid')
        if uid_str:
            try:
                node['uid'] = int(uid_str)
            except ValueError:
                pass
        
        # Recursively add children
        children = []
        for child in elem:
            children.append(self._element_to_tree_node(child))
        
        if children:
            node['children'] = children
        
        return node
    
    def _extract_nodes_from_xml(self, elem, tree: BTTree, nodes_list: list):
        """Recursively extract nodes from XML element."""
        # Check if this element has a _uid attribute
        uid_str = elem.attrib.get('_uid')
        if uid_str:
            try:
                uid = int(uid_str)
                name = elem.attrib.get('name', elem.attrib.get('ID', elem.tag))
                tag = elem.tag
                
                node = BTNode(uid=uid, name=name, tag=tag)
                tree.nodes[uid] = node
                nodes_list.append({
                    'uid': uid,
                    'name': name,
                    'tag': tag
                })
                self._log_info(f"  Parsed node {uid}: {name} ({tag})")
            except ValueError:
                pass
        
        # Recurse into children
        for child in elem:
            self._extract_nodes_from_xml(child, tree, nodes_list)

    def _handle_status_message(self, parts: list, tree_id: Optional[str] = None, suppress_event: bool = False):
        """
        Parse status update message from Groot2 REQ/REP response.
        
        Response format (multipart):
        - Part 0: Header (22 bytes): protocol(1), type(1), unique_id(4), tree_id(16)
        - Part 1: Status data: repeated (uid: uint16, status: uint8) tuples
        """
        if not parts or len(parts) < 2:
            return
        
        # Part 1 contains the status data
        data = parts[1]
        if len(data) < 3:
            return
        
        try:
            offset = 0
            changes = []
            
            # Parse status updates: (uid: uint16, status: uint8) pairs
            while offset + 3 <= len(data):
                uid = struct.unpack('<H', data[offset:offset+2])[0]
                status_val = data[offset+2]
                offset += 3
                
                status_str = NodeStatus.to_string(status_val)
                
                # Only report changes
                previous_status = self._last_statuses.get(uid)
                if previous_status != status_str:
                    self._last_statuses[uid] = status_str
                    
                    # Get node info if available
                    node_name = ""
                    node_tag = ""
                    if self._current_tree and uid in self._current_tree.nodes:
                        node = self._current_tree.nodes[uid]
                        node_name = node.name
                        node_tag = node.tag
                    
                    self._log_debug(f"  Node {uid} ({node_name}): -> {status_str}")
                    
                    changes.append({
                        'uid': uid,
                        'name': node_name,
                        'tag': node_tag,
                        'previous_status': previous_status,
                        'status': status_str
                    })
            
            if changes and not suppress_event:
                event = {
                    'type': 'bt_status',
                    'timestamp': time.time(),
                    'tree_id': tree_id or (self._current_tree.tree_id if self._current_tree else None),
                    'changes': changes
                }
                
                self._log_debug(f"Status update: {len(changes)} changes")
                self._log_debug(f"Sending status event: {json.dumps(event, indent=2)}")
                self._event_callback(event)
                
        except Exception as e:
            self._log_error(f"Error parsing status message: {e}")

    def _extract_subtree_names(self, xml: str) -> str:
        """
        Extract BehaviorTree IDs from the XML to build the bb_list for
        the blackboard request. Returns a semicolon-separated string.
        """
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(xml)
            ids = []
            for bt_elem in root.iter('BehaviorTree'):
                bt_id = bt_elem.attrib.get('ID', '')
                if bt_id:
                    ids.append(bt_id)
            return ';'.join(ids) if ids else ''
        except ET.ParseError:
            self._log_warn("Could not parse XML for subtree names")
            return ''

    def _request_blackboard(self, socket: zmq.Socket, bb_list: str):
        """
        Request blackboard data from the Groot2 server.
        
        Protocol: 2-part ZMQ message
          Part 0: Header (6 bytes): protocol(1)=2, type(1)='B', unique_id(4)
          Part 1: Semicolon-separated list of subtree names (e.g. "MainTree;SubA")
        
        Response:
          Part 0: Reply header (22 bytes)
          Part 1: MessagePack-encoded JSON of blackboard key-value pairs
        
        Emits a bt_blackboard event only when the data differs from the
        previously emitted snapshot.
        """
        if not bb_list:
            return
        
        try:
            protocol = 2
            req_type = ord('B')  # 'B' = BLACKBOARD request
            unique_id = random.getrandbits(32)
            header = struct.pack('<BBI', protocol, req_type, unique_id)
            
            socket.send(header, zmq.SNDMORE)
            socket.send(bb_list.encode('utf-8'))
            
            parts = socket.recv_multipart()
            if not parts or len(parts) < 2:
                return
            
            # Part 1 is MessagePack-encoded JSON
            bb_data = msgpack.unpackb(parts[1])
            if isinstance(bb_data, dict):
                self._blackboard = bb_data
                total_keys = sum(len(v) if isinstance(v, dict) else 0 for v in bb_data.values())
                
                # Only emit if the data actually changed
                if total_keys > 0:
                    bb_hash = json.dumps(bb_data, sort_keys=True, default=str)
                    if bb_hash != self._last_emitted_blackboard:
                        self._last_emitted_blackboard = bb_hash
                        self._log_debug(f"Blackboard changed: {len(bb_data)} subtree(s), {total_keys} keys")
                        self._event_callback({
                            "type": "bt_blackboard",
                            "timestamp": time.time(),
                            "tree_id": self._current_tree.tree_id if self._current_tree else None,
                            "blackboard": bb_data,
                        })
            else:
                self._log_warn(f"Unexpected blackboard response type: {type(bb_data)}")
                
        except zmq.Again:
            pass  # normal — tree may have finished, socket will be recreated by outer loop
        except Exception as e:
            self._log_warn(f"Error requesting blackboard: {e}")

    def _start_recording(self, socket: zmq.Socket):
        """Enable transition recording so we get every status change, not just snapshots."""
        try:
            protocol = 2
            header = struct.pack('<BBI', protocol, ord('r'), random.getrandbits(32))
            socket.send(header, zmq.SNDMORE)
            socket.send(b"start")
            parts = socket.recv_multipart()
            if parts and len(parts) >= 2:
                self._recording = True
                self._log_info("Transition recording started")
        except Exception as e:
            self._log_warn(f"Failed to start recording: {e}")

    def _poll_transitions(self, socket: zmq.Socket):
        """
        Fetch all status transitions since last poll.
        
        Uses Groot2 GET_TRANSITIONS ('t') which returns every individual
        status change (e.g. RUNNING→FAILURE, FAILURE→IDLE) — exactly what
        the terminal StdCoutLogger shows.
        
        Response format: 9 bytes per transition:
          6 bytes: timestamp_usec (relative to recording start)
          2 bytes: node_uid (uint16)
          1 byte:  status (uint8)
            Values 0-4: IDLE, RUNNING, SUCCESS, FAILURE, SKIPPED
            Values 11-13: IDLE (was RUNNING/SUCCESS/FAILURE) — subtract 10
        """
        if not self._recording:
            return
        
        try:
            protocol = 2
            header = struct.pack('<BBI', protocol, ord('t'), random.getrandbits(32))
            socket.send(header)
            
            parts = socket.recv_multipart()
            if not parts or len(parts) < 2:
                return
            
            data = parts[1]
            if len(data) < 9:
                return  # no transitions
            
            changes = []
            offset = 0
            while offset + 9 <= len(data):
                ts_usec = int.from_bytes(data[offset:offset+6], 'little')
                uid = struct.unpack('<H', data[offset+6:offset+8])[0]
                raw_status = data[offset+8]
                offset += 9
                
                # Decode status: values >= 10 mean "IDLE (was X)"
                if raw_status >= 10:
                    prev_val = raw_status - 10
                    new_status = "IDLE"
                else:
                    prev_val = None  # derived from _last_statuses below
                    new_status = NodeStatus.to_string(raw_status)
                
                previous_status = self._last_statuses.get(uid) if prev_val is None else NodeStatus.to_string(prev_val)
                
                # Get node info
                node_name = ""
                node_tag = ""
                if self._current_tree and uid in self._current_tree.nodes:
                    node = self._current_tree.nodes[uid]
                    node_name = node.name
                    node_tag = node.tag
                
                self._last_statuses[uid] = new_status
                changes.append({
                    'uid': uid,
                    'name': node_name,
                    'tag': node_tag,
                    'previous_status': previous_status,
                    'status': new_status,
                })
            
            if changes:
                self._event_callback({
                    'type': 'bt_status',
                    'timestamp': time.time(),
                    'tree_id': self._current_tree.tree_id if self._current_tree else None,
                    'changes': changes,
                })
                
        except zmq.Again:
            pass  # normal — tree may have finished
        except Exception as e:
            self._log_warn(f"Error polling transitions: {e}")

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