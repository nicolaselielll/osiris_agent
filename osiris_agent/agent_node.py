import os
import asyncio
import threading
import time
import rclpy
from collections import deque
from rcl_interfaces.msg import ParameterEvent
from rcl_interfaces.srv import GetParameters, ListParameters
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.parameter import parameter_value_to_python
import websockets
import json
from rosidl_runtime_py.utilities import get_message
from rosidl_runtime_py import message_to_ordereddict
import psutil

from .bt_collector import BTCollector
from .ros2_control_collector import Ros2ControlCollector
from .tf_tree_collector import TfTreeCollector

# Security and configuration constants
MAX_SUBSCRIPTIONS = 100
ALLOWED_TOPIC_PREFIXES = ['/', ]
PARAMETER_REFRESH_INTERVAL = 5.0
GRAPH_CHECK_INTERVAL = 0.1
TELEMETRY_INTERVAL = 1.0
RECONNECT_INITIAL_DELAY = 1
RECONNECT_MAX_DELAY = 10

class WebBridge(Node):
    # Initialize node, validate token, setup timers and start websocket thread
    def __init__(self):
        super().__init__('osiris_node')
        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")
    
        # self.ws_url = f'wss://osiris-gateway.fly.dev?robot=true&token={auth_token}'
        self.ws_url = f'ws://host.docker.internal:8080?robot=true&token={auth_token}'
        self.ws = None
        self._topic_subs = {}
        self._topic_subs_lock = threading.Lock()
        self.loop = None
        self._send_queue = None
        self._active_nodes = {
            self._node_full_name(name, ns)
            for name, ns in self.get_node_names_and_namespaces()
        }
        self._active_topics = set(dict(self.get_topic_names_and_types()).keys())
        self._active_actions = {
            t.replace('/_action/status', '')
            for t in self._active_topics
            if t.endswith('/_action/status')
        }
        self._active_services = {
            service_name
            for service_name, _ in self.get_service_names_and_types()
        }
        self._action_status_subs = {}
        self._active_goals = {}
        self._topic_relations = {}
        self._action_relations = {}
        self._service_relations = {}
        # Pre-populate subscriber tracking so first tick doesn't emit spurious
        # 'subscribed' events for nodes that were already running before osiris.
        self._last_topic_subscribers = {
            topic: {
                self._node_full_name(sub.node_name, sub.node_namespace)
                for sub in self.get_subscriptions_info_by_topic(topic)
            }
            for topic in self._active_topics
        }
        self._telemetry_enabled = True
        self._topic_last_timestamp = {}
        self._topic_rate_history = {}
        self._rate_history_depth = 8
        # Fetch params synchronously so initial_state includes them on first connect
        self._node_parameter_cache = {
            node_name: self._get_node_parameters(node_name)
            for node_name in self._active_nodes
        }
        self.create_subscription(ParameterEvent, '/parameter_events', self._on_parameter_event, 10)
        
        self._last_sent_nodes = None
        self._last_sent_topics = None
        self._last_sent_actions = None
        self._last_sent_services = None
        self._cached_bt_tree_event = None  # Cache tree event until WS connects
        self._graph_dirty = False  # Debounce flag for graph snapshot sends

        # ros2_control collector (controllers + hardware panels)
        self._ros2_control = Ros2ControlCollector(
            node=self,
            event_callback=self._on_ros2_control_event,
            logger=self.get_logger(),
        )

        # TF tree collector
        self._tf_tree = TfTreeCollector(
            node=self,
            event_callback=self._on_tf_tree_event,
            logger=self.get_logger(),
        )
        
        self._check_graph_changes()
        self.create_timer(0.1, self._check_graph_changes)
        self.create_timer(1.0, self._collect_telemetry)
        self.create_timer(5.0, self._refresh_empty_param_caches)

        threading.Thread(target=self._run_ws_client, daemon=True).start()
        
        # Initialize BT Collector for Groot2 events (optional)
        bt_enabled = os.environ.get('OSIRIS_BT_COLLECTOR_ENABLED', '').lower() in ('true', '1', 'yes')
        if bt_enabled:
            self._bt_collector = BTCollector(
                event_callback=self._on_bt_event,
                logger=self.get_logger()
            )
            self._bt_collector.start()
        else:
            self._bt_collector = None

        # Initialize nav2 BT monitoring via /behavior_tree_log ROS topic
        self._init_nav2_bt_monitor()

        self.get_logger().info("🚀 Osiris agent running")

    # Create event loop and queue, run websocket client
    def _run_ws_client(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._send_queue = asyncio.Queue()
        self.loop.run_until_complete(self._client_loop_with_reconnect())

    # Wrapper for client loop with exponential backoff reconnection
    async def _client_loop_with_reconnect(self):
        """Wrapper that handles reconnection."""
        reconnect_delay = RECONNECT_INITIAL_DELAY
        
        while True:
            try:
                await self._client_loop()
            except Exception as e:
                pass  # Silent retry
            
            await asyncio.sleep(reconnect_delay)
            
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
            import random
            reconnect_delay += random.uniform(0, 1)  # Jitter prevents thundering herd

    # Main client loop for sending and receiving messages
    async def _client_loop(self):
        send_task = None
        try:
            async with websockets.connect(self.ws_url) as ws:
                # Wait for gateway auth response before sending initial state
                try:
                    auth_msg = await ws.recv()
                except Exception as e:
                    return

                try:
                    auth_data = json.loads(auth_msg)
                except Exception:
                    auth_data = None

                if not auth_data or auth_data.get('type') != 'auth_success':
                    return

                self.ws = ws

                send_task = asyncio.create_task(self._send_loop(ws))

                await self._send_initial_state()

                await self._receive_loop(ws)
        except Exception as e:
            raise
        finally:
            if send_task and not send_task.done():
                send_task.cancel()
                try:
                    await send_task
                except asyncio.CancelledError:
                    pass
            
            self.ws = None
            self.get_logger().info("Connection closed, cleaning up...")

    # Collect and send complete ROS graph state on connection
    async def _send_initial_state(self):
        """Send complete initial state as a single message"""
        nodes = self._get_nodes_with_relations()
        actions = self._get_actions_with_relations()
        services = self._get_services_with_relations()
        topics = self._get_topics_with_relations()

        # Force a tf_tree poll so the snapshot is current when sent
        self._tf_tree.poll(force=True)
        
        self._last_sent_nodes = nodes.copy()
        self._last_sent_actions = actions.copy()
        self._last_sent_services = services.copy()
        self._last_sent_topics = topics.copy()
        
        message = {
            'type': 'initial_state',
            'timestamp': time.time(),
            'data': {
                'nodes': nodes,
                'topics': topics,
                'actions': actions,
                'services': services,
                'telemetry': self._get_telemetry_snapshot(),
                'controllers': self._ros2_control.get_controllers_snapshot(),
                'hardware': self._ros2_control.get_hardware_snapshot(),
                'tf_tree': self._tf_tree.get_snapshot(),
            }
        }
        
        await self._send_queue.put(json.dumps(message))
        self.get_logger().info(f"Sent initial state: {len(nodes)} nodes, {len(topics)} topics, {len(actions)} actions, {len(services)} services")

        await self._send_queue.put(json.dumps(self._build_startup_bt_state_event()))
        self.get_logger().info("Sent startup bt_state event")
        
        # Send cached BT tree event if we have one
        if self._cached_bt_tree_event:
            self.get_logger().info("Sending cached BT tree event")
            await self._send_queue.put(json.dumps(self._cached_bt_tree_event))
            self._cached_bt_tree_event = None
        
        await self._send_bridge_subscriptions()

    # Build bt_state event for startup regardless of BT collector state
    def _build_startup_bt_state_event(self):
        # BT.CPP: use cached tree event if available
        source = self._cached_bt_tree_event
        if source:
            return {
                'type': 'bt_state',
                'timestamp': source.get('timestamp', time.time()),
                'tree_id': source.get('tree_id'),
                'tree': source.get('tree'),
                'nodes': source.get('nodes', []),
            }

        # Nav2 BT: if bt_navigator is running, build state from parsed XML + known statuses
        if hasattr(self, '_nav2_bt_tree_id') and self._nav2_bt_tree_id and self._nav2_bt_tree_structure:
            nodes_with_status = [
                {**nd, 'status': self._nav2_bt_statuses.get(nd['name'], 'IDLE')}
                for nd in self._nav2_bt_nodes_list
            ]
            return {
                'type': 'bt_state',
                'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'tree': self._nav2_bt_tree_structure,
                'nodes': nodes_with_status,
            }

        return {
            'type': 'bt_state',
            'timestamp': time.time(),
            'tree_id': None,
            'tree': None,
            'nodes': [],
        }

    # Send list of currently subscribed topics to gateway
    async def _send_bridge_subscriptions(self):
        """Send current bridge subscriptions as a separate message."""
        with self._topic_subs_lock:
            subscriptions = list(self._topic_subs.keys())
        
        message = {
            'type': 'bridge_subscriptions',
            'subscriptions': subscriptions,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        self.get_logger().debug(f"Sent bridge subscriptions: {len(subscriptions)} topics")

    # Receive and handle commands from gateway
    async def _receive_loop(self, ws):
        async for msg in ws:
            try:
                data = json.loads(msg)
                msg_type = data.get('type')
                
                if msg_type == 'subscribe':
                    topic = data.get('topic')
                    if topic:
                        self.get_logger().info(f"Subscribing to topic: {topic}")
                        self._subscribe_to_topic(topic)
                    else:
                        self.get_logger().warn("Subscribe message missing topic field")
                        
                elif msg_type == 'unsubscribe':
                    topic = data.get('topic')
                    if topic:
                        self._unsubscribe_from_topic(topic)
                    else:
                        self.get_logger().warn("Unsubscribe message missing topic field")
                        
                elif msg_type == 'start_telemetry':
                    self._telemetry_enabled = True
                    self.get_logger().info("Telemetry started")
                    
                elif msg_type == 'stop_telemetry':
                    self._telemetry_enabled = False
                    self.get_logger().info("Telemetry stopped")
                    
                elif msg_type == 'error':
                    error_msg = data.get('message', 'Unknown error')
                    self.get_logger().warn(f"Server error: {error_msg}")
                    
                else:
                    self.get_logger().debug(f"Unhandled message type: {msg_type}")
                    
            except json.JSONDecodeError as e:
                self.get_logger().error(f"Invalid JSON received: {e}")
            except Exception as e:
                self.get_logger().error(f"Error processing message: {e}")

    # Send messages out
    async def _send_loop(self, ws):
        while True:
            msg = await self._send_queue.get()
            try:
                # Log truncated message for debugging
                self.get_logger().debug(f"_send_loop: sending message (len={len(msg)}): {msg[:200]}")
                await ws.send(msg)
                self.get_logger().debug("_send_loop: message sent")
            except Exception as e:
                self.get_logger().error(f"_send_loop: failed to send message: {e}")
                # If sending fails, log and continue (do not drop the loop)
                try:
                    # small delay to avoid busy loop on persistent error
                    await asyncio.sleep(0.1)
                except Exception:
                    pass

    # Create ROS subscription for topic with validation and limits
    def _subscribe_to_topic(self, topic_name):
        if not topic_name or not isinstance(topic_name, str):
            self.get_logger().warn(f"Invalid topic name: {topic_name}")
            return
        
        with self._topic_subs_lock:
            if topic_name in self._topic_subs:
                return
            
            if len(self._topic_subs) >= MAX_SUBSCRIPTIONS:
                self.get_logger().error(f"Subscription limit reached ({MAX_SUBSCRIPTIONS}). Cannot subscribe to {topic_name}")
                return
        
        topic_types = dict(self.get_topic_names_and_types()).get(topic_name)
        if not topic_types:
            self.get_logger().warn(f"Topic {topic_name} not found in ROS graph")
            return
        
        msg_class = get_message(topic_types[0])
        sub = self.create_subscription(
            msg_class,
            topic_name,
            lambda msg, t=topic_name: self._on_topic_msg(msg, t),
            QoSProfile(depth=10)
        )

        with self._topic_subs_lock:
            self._topic_subs[topic_name] = sub

        self._update_topic_relations()
        self.get_logger().info(f"Subscribed to {topic_name}")
        
        asyncio.create_task(self._send_bridge_subscriptions())

    # Destroy ROS subscription and update gateway
    def _unsubscribe_from_topic(self, topic_name):
        with self._topic_subs_lock:
            if topic_name in self._topic_subs:
                self.destroy_subscription(self._topic_subs[topic_name])
                del self._topic_subs[topic_name]

        self._update_topic_relations()
        self.get_logger().info(f"Unsubscribed from {topic_name}")
        
        asyncio.run_coroutine_threadsafe(
            self._send_bridge_subscriptions(),
            self.loop
        )

    # Handle incoming topic message, calculate rate, send to gateway
    def _on_topic_msg(self, msg, topic_name):
        if not self.ws or not self.loop:
            return

        timestamp = time.time()
        last_timestamp = self._topic_last_timestamp.get(topic_name)
        if last_timestamp is not None:
            delta = timestamp - last_timestamp
            if delta > 0:
                history = self._topic_rate_history.setdefault(topic_name, deque(maxlen=self._rate_history_depth))
                history.append(delta)
        self._topic_last_timestamp[topic_name] = timestamp

        rate = None
        history = self._topic_rate_history.get(topic_name)
        if history:
            total = sum(history)
            if total > 0:
                rate = len(history) / total

        event = {
            'type': 'topic_data',
            'topic': topic_name,
            'data': message_to_ordereddict(msg),
            'rate_hz': rate,
            'timestamp': timestamp
        }
        self.get_logger().debug(f"Received message on {topic_name}")
        self._send_event_and_update(event, f"Topic data: {topic_name}")

    # Update topic publishers and subscribers
    def _update_topic_relations(self): 
        """Update the cached topic relations."""
        current_topics = set(dict(self.get_topic_names_and_types()).keys())
        current_topic_relations = {}
        
        for topic_name in current_topics:
            publishers = {
                self._node_full_name(pub.node_name, pub.node_namespace)
                for pub in self.get_publishers_info_by_topic(topic_name)
            }
            subscribers = {
                self._node_full_name(sub.node_name, sub.node_namespace)
                for sub in self.get_subscriptions_info_by_topic(topic_name)
            }
            current_topic_relations[topic_name] = {
                'publishers': publishers,
                'subscribers': subscribers
            }
        
        self._topic_relations = current_topic_relations

    # Get topics with publishers, subscribers, and QoS profiles
    def _get_topics_with_relations(self):
        """Get topics with their publishers and subscribers with QoS info (uses cached data)."""
        self._update_topic_relations()
        topics_with_relations = {}
        topic_types_dict = dict(self.get_topic_names_and_types())
        
        for topic_name, relations in self._topic_relations.items():
            publishers_list = []
            pub_info_list = self.get_publishers_info_by_topic(topic_name)
            for pub_info in pub_info_list:
                publishers_list.append({
                    'node': self._node_full_name(pub_info.node_name, pub_info.node_namespace),
                    'qos': self._qos_profile_to_dict(pub_info.qos_profile)
                })
            
            subscribers_list = []
            sub_info_list = self.get_subscriptions_info_by_topic(topic_name)
            for sub_info in sub_info_list:
                subscribers_list.append({
                    'node': self._node_full_name(sub_info.node_name, sub_info.node_namespace),
                    'qos': self._qos_profile_to_dict(sub_info.qos_profile)
                })
            
            topics_with_relations[topic_name] = {
                'type': topic_types_dict.get(topic_name, ['unknown'])[0],
                'publishers': publishers_list,
                'subscribers': subscribers_list,
            }
        return topics_with_relations

    # Build fully-qualified node name from short name and namespace
    @staticmethod
    def _node_full_name(name, namespace):
        """Return the fully-qualified node path, e.g. /ns/node or /node."""
        ns = namespace if namespace.endswith('/') else namespace + '/'
        return ns + name

    # Convert ROS QoS profile to dictionary
    def _qos_profile_to_dict(self, qos_profile):
        """Convert a QoS profile to a dictionary."""
        if not qos_profile:
            return None
        
        return {
            'reliability': qos_profile.reliability.name if hasattr(qos_profile.reliability, 'name') else str(qos_profile.reliability),
            'durability': qos_profile.durability.name if hasattr(qos_profile.durability, 'name') else str(qos_profile.durability),
            'history': qos_profile.history.name if hasattr(qos_profile.history, 'name') else str(qos_profile.history),
            'depth': qos_profile.depth,
            'liveliness': qos_profile.liveliness.name if hasattr(qos_profile.liveliness, 'name') else str(qos_profile.liveliness),
        }

    # Retry fetching parameters for nodes whose cache is still empty
    def _refresh_empty_param_caches(self):
        for node_name in list(self._active_nodes):
            if not self._node_parameter_cache.get(node_name):
                self._fetch_node_parameters_async(node_name)

    # Handle runtime parameter changes from any node
    def _on_parameter_event(self, msg):
        node_name = msg.node if msg.node.startswith('/') else f"/{msg.node}"
        if node_name in self._active_nodes:
            # Async fetch: avoids re-entrant spin_until_future_complete inside subscription callback
            self._fetch_node_parameters_async(node_name)

    # Non-blocking async parameter fetch (safe to call from timer/subscription callbacks)
    def _fetch_node_parameters_async(self, node_name):
        """Fetch parameters for node_name without blocking the executor."""
        service_prefix = node_name if node_name.startswith('/') else f"/{node_name}"

        list_client = self.create_client(ListParameters, f"{service_prefix}/list_parameters")
        if not list_client.service_is_ready():
            self.destroy_client(list_client)
            return

        req = ListParameters.Request()
        req.depth = 10
        future = list_client.call_async(req)

        def _on_list(fut):
            self.destroy_client(list_client)
            response = fut.result()
            if response is None or not response.result.names:
                return

            param_names = list(response.result.names)
            get_client = self.create_client(GetParameters, f"{service_prefix}/get_parameters")
            get_req = GetParameters.Request()
            get_req.names = param_names
            get_future = get_client.call_async(get_req)

            def _on_get(gfut):
                self.destroy_client(get_client)
                get_resp = gfut.result()
                if get_resp is None:
                    return
                params = {}
                for name, value in zip(param_names, get_resp.values):
                    try:
                        params[name] = parameter_value_to_python(value)
                    except Exception as e:
                        self.get_logger().debug(f"Could not convert parameter {name}: {e}")
                self._node_parameter_cache[node_name] = params
                self._graph_dirty = True

            get_future.add_done_callback(_on_get)

        future.add_done_callback(_on_list)

    # Get all parameters for a node using ROS services (sync – only safe before executor starts)
    def _get_node_parameters(self, node_name):
        """Get parameters for a specific node using the ROS parameter services."""
        service_prefix = node_name if node_name.startswith('/') else f"/{node_name}"
        param_names = self._list_node_parameters(service_prefix)
        if not param_names:
            return {}

        param_values = self._get_node_parameter_values(service_prefix, param_names)
        parameters = {}
        for name, value in zip(param_names, param_values):
            try:
                parameters[name] = parameter_value_to_python(value)
            except Exception as e:
                self.get_logger().debug(f"Could not convert parameter {name}: {e}")
        return parameters

    # Call list_parameters service for a node
    def _list_node_parameters(self, service_prefix, timeout_sec=0.2):
        service_name = f"{service_prefix}/list_parameters"
        client = self.create_client(ListParameters, service_name)
        if not client.wait_for_service(timeout_sec=timeout_sec):
            self.destroy_client(client)
            return []

        request = ListParameters.Request()
        request.depth = 10
        future = client.call_async(request)
        rclpy.spin_until_future_complete(self, future, timeout_sec=timeout_sec)
        response = future.result()
        self.destroy_client(client)

        if response is None:
            return []
        return list(response.result.names)

    # Call get_parameters service for a node
    def _get_node_parameter_values(self, service_prefix, names, timeout_sec=0.2):
        service_name = f"{service_prefix}/get_parameters"
        client = self.create_client(GetParameters, service_name)
        if not client.wait_for_service(timeout_sec=timeout_sec):
            self.destroy_client(client)
            return []

        request = GetParameters.Request()
        request.names = names
        future = client.call_async(request)
        rclpy.spin_until_future_complete(self, future, timeout_sec=timeout_sec)
        response = future.result()
        self.destroy_client(client)

        if response is None:
            return []
        return list(response.values)

    # Get nodes with their topics, actions, services, and parameters
    def _get_nodes_with_relations(self):
        """Get nodes with the topics they publish and subscribe to (derived from cached topic relations)."""
        nodes_with_relations = {}
        
        self._update_action_relations()
        self._update_service_relations()
        
        for node_name in self._active_nodes:
            nodes_with_relations[node_name] = {
                'publishes': [],
                'subscribes': [],
                'actions': [],
                'services': [],
                'parameters': {}
            }
        
        for topic_name, relations in self._topic_relations.items():
            for node_name in relations['publishers']:
                if node_name in nodes_with_relations:
                    pub_info_list = self.get_publishers_info_by_topic(topic_name)
                    qos_profile = None
                    for pub_info in pub_info_list:
                        if self._node_full_name(pub_info.node_name, pub_info.node_namespace) == node_name:
                            qos_profile = self._qos_profile_to_dict(pub_info.qos_profile)
                            break
                    
                    nodes_with_relations[node_name]['publishes'].append({
                        'topic': topic_name,
                        'qos': qos_profile
                    })
            
            for node_name in relations['subscribers']:
                if node_name in nodes_with_relations:
                    sub_info_list = self.get_subscriptions_info_by_topic(topic_name)
                    qos_profile = None
                    for sub_info in sub_info_list:
                        if self._node_full_name(sub_info.node_name, sub_info.node_namespace) == node_name:
                            qos_profile = self._qos_profile_to_dict(sub_info.qos_profile)
                            break
                    
                    nodes_with_relations[node_name]['subscribes'].append({
                        'topic': topic_name,
                        'qos': qos_profile
                    })
        
        for action_name, relations in self._action_relations.items():
            for node_name in relations['providers']:
                if node_name in nodes_with_relations:
                    nodes_with_relations[node_name]['actions'].append(action_name)
        
        for service_name, relations in self._service_relations.items():
            for node_name in relations['providers']:
                if node_name in nodes_with_relations:
                    nodes_with_relations[node_name]['services'].append(service_name)
        
        cache = self._node_parameter_cache
        for node_name in nodes_with_relations.keys():
            nodes_with_relations[node_name]['parameters'] = cache.get(node_name, {})
        return nodes_with_relations

    # Update cached action providers by detecting status topics
    def _update_action_relations(self):
        """Update the cached action relations."""
        action_relations = {}
        
        for topic_name in self.get_topic_names_and_types():
            if topic_name[0].endswith('/_action/status'):
                action_name = topic_name[0].replace('/_action/status', '')
                providers = [
                    self._node_full_name(info.node_name, info.node_namespace)
                    for info in self.get_publishers_info_by_topic(topic_name[0])
                ]
                action_relations[action_name] = {
                    'providers': set(providers),
                }
        
        self._action_relations = action_relations

    # Get actions with their provider nodes
    def _get_actions_with_relations(self):
        """Get actions from status topics and update cached action relations."""
        self._update_action_relations()
        
        actions_with_relations = {}
        for action_name, relations in self._action_relations.items():
            actions_with_relations[action_name] = {
                'providers': list(relations['providers']),
            }
        
        return actions_with_relations

    # Update cached service providers by querying nodes
    def _update_service_relations(self):
        """Update the cached service relations."""
        service_relations = {}
        
        all_services = self.get_service_names_and_types()
        
        for service_name, service_types in all_services:
            providers = set()
            for node_short_name, node_namespace in self.get_node_names_and_namespaces():
                node_name = self._node_full_name(node_short_name, node_namespace)
                try:
                    node_services = self.get_service_names_and_types_by_node(node_short_name, node_namespace)
                    if any(svc_name == service_name for svc_name, _ in node_services):
                        providers.add(node_name)
                except Exception as e:
                    self.get_logger().debug(f"Error checking services for node {node_name}: {e}")
            
            service_relations[service_name] = {
                'providers': providers,
                'type': service_types[0] if service_types else 'unknown'
            }
        
        self._service_relations = service_relations

    # Get services with their provider nodes and types
    def _get_services_with_relations(self):
        """Get services with their providers and update cached service relations."""
        self._update_service_relations()
        
        services_with_relations = {}
        for service_name, relations in self._service_relations.items():
            providers = list(relations['providers'])
            # Skip services with no live provider — DDS can lag behind and
            # keep reporting service endpoints for a few seconds after the
            # providing node has shut down.
            if not providers:
                continue
            services_with_relations[service_name] = {
                'providers': providers,
                'type': relations['type']
            }
        
        return services_with_relations

    # Poll ROS graph for changes and send events
    def _check_graph_changes(self):
        """Check for node, topic, action, and publisher/subscriber changes."""
        current_nodes = {
            self._node_full_name(name, ns)
            for name, ns in self.get_node_names_and_namespaces()
        }
        current_topics = set(dict(self.get_topic_names_and_types()).keys())

        current_topic_relations = {}

        if not hasattr(self, '_last_topic_subscribers'):
            self._last_topic_subscribers = {}

        for topic_name in current_topics:
            publishers = {
                self._node_full_name(pub.node_name, pub.node_namespace)
                for pub in self.get_publishers_info_by_topic(topic_name)
            }
            subscribers = {
                self._node_full_name(sub.node_name, sub.node_namespace)
                for sub in self.get_subscriptions_info_by_topic(topic_name)
            }
            current_topic_relations[topic_name] = {
                'publishers': publishers,
                'subscribers': subscribers
            }

            prev_subs = self._last_topic_subscribers.get(topic_name, set())
            new_subs = subscribers - prev_subs
            for node_name in new_subs:
                event = {
                    'type': 'topic_event',
                    'topic': topic_name,
                    'node': node_name,
                    'event': 'subscribed',
                    'timestamp': time.time()
                }
                self._send_event_and_update(event, f"Node {node_name} subscribed to {topic_name}")

            removed_subs = prev_subs - subscribers
            for node_name in removed_subs:
                event = {
                    'type': 'topic_event',
                    'topic': topic_name,
                    'node': node_name,
                    'event': 'unsubscribed',
                    'timestamp': time.time()
                }
                self._send_event_and_update(event, f"Node {node_name} unsubscribed from {topic_name}")

            if topic_name in self._topic_relations:
                old_pubs = self._topic_relations[topic_name]['publishers']
                if publishers != old_pubs:
                    self._send_event_and_update(None, f"Topic publishers changed: {topic_name}")
                    if topic_name == '/behavior_tree_log' and hasattr(self, '_nav2_bt_tree_id'):
                        if not publishers and old_pubs:
                            self._on_nav2_bt_gone()
                        elif publishers and not old_pubs:
                            self._load_and_parse_bt_xml()

        self._last_topic_subscribers = {topic: set(rel['subscribers']) for topic, rel in current_topic_relations.items()}

        # Only count actions whose /_action/status topic has at least one publisher.
        # A bare subscriber (like our own) must not prevent actions from being detected
        # as gone when the providing node stops.
        current_actions = {
            topic_name.replace('/_action/status', '')
            for topic_name, rel in current_topic_relations.items()
            if topic_name.endswith('/_action/status') and rel['publishers']
        }

        started_nodes = current_nodes - self._active_nodes
        for node_name in started_nodes:
            # Async fetch: avoids re-entrant spin_until_future_complete inside timer callback
            self._fetch_node_parameters_async(node_name)
            event = {
                'type': 'node_event',
                'node': node_name,
                'event': 'started',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Node started: {node_name}")
        
        stopped_nodes = self._active_nodes - current_nodes
        for node_name in stopped_nodes:
            self._node_parameter_cache.pop(node_name, None)
            event = {
                'type': 'node_event',
                'node': node_name,
                'event': 'stopped',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Node stopped: {node_name}")
        
        started_topics = current_topics - self._active_topics
        for topic_name in started_topics:
            event = {
                'type': 'topic_event',
                'topic': topic_name,
                'event': 'created',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Topic created: {topic_name}")
        
        stopped_topics = self._active_topics - current_topics
        for topic_name in stopped_topics:
            event = {
                'type': 'topic_event',
                'topic': topic_name,
                'event': 'destroyed',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Topic destroyed: {topic_name}")
        
        started_actions = current_actions - self._active_actions
        for action_name in started_actions:
            event = {
                'type': 'action_event',
                'action': action_name,
                'event': 'created',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Action created: {action_name}")
        
        stopped_actions = self._active_actions - current_actions
        if stopped_actions:
            self.get_logger().info(f"Actions stopped: {stopped_actions}")
        for action_name in stopped_actions:
            event = {
                'type': 'action_event',
                'action': action_name,
                'event': 'destroyed',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Action destroyed: {action_name}")
            if action_name in self._action_status_subs:
                self.destroy_subscription(self._action_status_subs[action_name])
                del self._action_status_subs[action_name]
                del self._active_goals[action_name]
        
        current_services = {service_name for service_name, _ in self.get_service_names_and_types()}

        # Eagerly evict services whose only provider was a node that just stopped.
        # get_service_names_and_types() can lag behind DDS, so without this those
        # services would linger in the services list until DDS catches up.
        for node_name in stopped_nodes:
            for service_name, rel in self._service_relations.items():
                if rel['providers'] == {node_name}:
                    current_services.discard(service_name)

        started_services = current_services - self._active_services
        for service_name in started_services:
            if service_name.startswith('/ros2cli_daemon'):
                continue
            event = {
                'type': 'service_event',
                'service': service_name,
                'event': 'created',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Service created: {service_name}")
        
        stopped_services = self._active_services - current_services
        for service_name in stopped_services:
            if service_name.startswith('/ros2cli_daemon'):
                continue
            event = {
                'type': 'service_event',
                'service': service_name,
                'event': 'destroyed',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Service destroyed: {service_name}")
            
        self._active_nodes = current_nodes
        self._active_topics = current_topics
        self._active_actions = current_actions
        self._active_services = current_services
        self._topic_relations = current_topic_relations

        # Flush graph snapshots once per tick if anything changed
        if self._graph_dirty and self.ws and self.loop:
            self._graph_dirty = False
            asyncio.run_coroutine_threadsafe(self._send_nodes(), self.loop)
            asyncio.run_coroutine_threadsafe(self._send_topics(), self.loop)
            asyncio.run_coroutine_threadsafe(self._send_actions(), self.loop)
            asyncio.run_coroutine_threadsafe(self._send_services(), self.loop)

        # Poll ros2_control state (internally throttled to 2 s)
        self._ros2_control.poll()

        # Poll TF tree state (internally throttled to 0.1 s)
        self._tf_tree.poll()

    # Send event to gateway and mark graph dirty for end-of-tick flush
    def _send_event_and_update(self, event, log_message):
        """Send event and mark graph as dirty (snapshot sent at end of tick)."""
        if not self.ws or not self.loop:
            return

        if event:
            asyncio.run_coroutine_threadsafe(self._send_queue.put(json.dumps(event)), self.loop)
        
        self._graph_dirty = True
        
        if log_message:
            self.get_logger().debug(log_message)

    # Send nodes to gateway if changed
    async def _send_nodes(self):
        """Send current nodes list to gateway (only when changed)."""
        nodes = self._get_nodes_with_relations()
        
        if self._last_sent_nodes == nodes:
            return
        
        self._last_sent_nodes = nodes.copy()
        
        message = {
            'type': 'nodes',
            'data': nodes,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        self.get_logger().debug(f"Sent nodes list: {list(nodes.keys())}")

    # Send topics to gateway if changed
    async def _send_topics(self):
        """Send current topics list to gateway (only when changed)."""
        topics = self._get_topics_with_relations()
        
        if self._last_sent_topics == topics:
            return
        
        self._last_sent_topics = topics.copy()
        
        message = {
            'type': 'topics',
            'data': topics,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        self.get_logger().debug(f"Sent topics list: {list(topics.keys())}")

    # Send actions to gateway if changed
    async def _send_actions(self):
        """Send current actions list to gateway (only when changed)."""
        actions = self._get_actions_with_relations()
        
        if self._last_sent_actions == actions:
            return
        
        self._last_sent_actions = actions.copy()
        
        message = {
            'type': 'actions',
            'data': actions,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        self.get_logger().debug(f"Sent actions list: {list(actions.keys())}")

    # Send services to gateway if changed
    async def _send_services(self):
        """Send current services list to gateway (only when changed)."""
        services = self._get_services_with_relations()
        
        if self._last_sent_services == services:
            return
        
        self._last_sent_services = services.copy()
        
        message = {
            'type': 'services',
            'data': services,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        self.get_logger().debug(f"Sent services list: {list(services.keys())}")

    # Collect and send system telemetry to gateway
    def _collect_telemetry(self):
        """Collect system telemetry (CPU, RAM) and send to queue."""
        if not self._telemetry_enabled or not self.ws or not self.loop:
            return
        data = self._get_telemetry_snapshot()
        telemetry = {
            'type': 'telemetry',
            'data': data,
            'timestamp': time.time()
        }

        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps(telemetry)),
            self.loop
        )

    # Return current CPU, RAM, and disk usage
    def _get_telemetry_snapshot(self):
        """Return a snapshot of system telemetry (CPU, RAM, disk)."""
        return {
            'cpu': {
                'percent': psutil.cpu_percent(interval=None),
                'cores': psutil.cpu_count(logical=False),
            },
            'ram': {
                'percent': psutil.virtual_memory().percent,
                'used_mb': psutil.virtual_memory().used / (1024 * 1024),
                'total_mb': psutil.virtual_memory().total / (1024 * 1024),
            },
            'disk': {
                'percent': psutil.disk_usage('/').percent,
                'used_gb': psutil.disk_usage('/').used / (1024 * 1024 * 1024),
                'total_gb': psutil.disk_usage('/').total / (1024 * 1024 * 1024),
            }
        }

    # Handle BT events from BTCollector
    def _on_bt_event(self, event):
        """Handle behavior tree events from BTCollector and forward to websocket."""
        event_type = event.get('type')
        
        # Update cache: store new tree, or clear it when BT is gone (tree_id is None)
        if event_type == 'bt_tree':
            self._cached_bt_tree_event = event if event.get('tree_id') else None

        # Cache tree events if WS not connected yet
        if not self.ws or not self.loop:
            return
        
        try:
            asyncio.run_coroutine_threadsafe(
                self._send_queue.put(json.dumps(event)),
                self.loop
            )
        except Exception as e:
            self.get_logger().error(f"Failed to queue BT event: {e}")

    # Nav2 BT monitoring via /behavior_tree_log ROS topic
    def _init_nav2_bt_monitor(self):
        """Subscribe to nav2's /behavior_tree_log topic for BT state monitoring."""
        try:
            from nav2_msgs.msg import BehaviorTreeLog
            from action_msgs.msg import GoalStatusArray
            self._nav2_bt_statuses = {}       # node_name -> current status string
            self._nav2_bt_session_active = False
            self._nav2_bt_tree_id = None      # set once XML is parsed
            self._nav2_bt_tree_structure = None
            self._nav2_bt_nodes_list = []
            self._nav2_bt_name_to_uid = {}
            self.create_subscription(
                BehaviorTreeLog,
                '/behavior_tree_log',
                self._on_nav2_bt_log,
                10
            )
            self.create_subscription(
                GoalStatusArray,
                '/navigate_to_pose/_action/status',
                self._on_nav2_goal_status,
                10
            )
            # If bt_navigator is already publishing when the agent starts, pre-parse
            # the XML so the startup bt_state event can include the nav2 tree structure.
            bt_log_publishers = self.get_publishers_info_by_topic('/behavior_tree_log')
            if bt_log_publishers:
                self._load_and_parse_bt_xml()
        except Exception as e:
            self.get_logger().debug(f"Nav2 BT monitoring unavailable: {e}")

    def _load_and_parse_bt_xml(self) -> bool:
        """Parse the BT XML file (once) and populate nav2 BT state fields. Returns True on success."""
        if self._nav2_bt_tree_id is not None:
            return True  # Already done

        import hashlib
        import xml.etree.ElementTree as ET

        # Determine XML file path: prefer bt_navigator parameter, fall back to nav2 default
        xml_path = self._node_parameter_cache.get('/bt_navigator', {}).get('default_nav_to_pose_bt_xml', '')
        if not xml_path:
            try:
                from ament_index_python.packages import get_package_share_directory
                nav2_share = get_package_share_directory('nav2_bt_navigator')
                xml_path = os.path.join(
                    nav2_share, 'behavior_trees',
                    'navigate_to_pose_w_replanning_and_recovery.xml'
                )
            except Exception:
                return False

        try:
            with open(xml_path, 'r') as f:
                xml_content = f.read()
        except Exception as e:
            self.get_logger().error(f"Cannot read BT XML '{xml_path}': {e}")
            return False

        try:
            root_elem = ET.fromstring(xml_content)
            bt_elem = root_elem.find('.//BehaviorTree')
            if bt_elem is None:
                return False

            nodes_list = []
            name_to_uid = {}
            uid_counter = [1]

            def elem_to_node(elem):
                name = elem.attrib.get('name', elem.attrib.get('ID', elem.tag))
                uid = uid_counter[0]; uid_counter[0] += 1
                name_to_uid[name] = uid
                nodes_list.append({'uid': uid, 'name': name, 'tag': elem.tag})
                node = {'tag': elem.tag, 'name': name, 'uid': uid, 'attributes': dict(elem.attrib)}
                kids = [elem_to_node(c) for c in elem]
                if kids:
                    node['children'] = kids
                return node

            bt_children = list(bt_elem)
            tree_structure = elem_to_node(bt_children[0]) if bt_children else {}

            self._nav2_bt_tree_structure = tree_structure
            self._nav2_bt_nodes_list = nodes_list
            self._nav2_bt_name_to_uid = name_to_uid
            self._nav2_bt_tree_id = hashlib.sha1(xml_content.encode()).hexdigest()[:16]
            return True
        except Exception as e:
            self.get_logger().error(f"Failed to parse BT XML: {e}")
            return False

    def _on_nav2_bt_log(self, msg):
        """Handle nav2 BehaviorTreeLog messages and emit bt_tree / bt_status events."""
        if not self._load_and_parse_bt_xml():
            return

        # Build changes list and update local status tracking.
        changes = []
        has_running = False
        for change in msg.event_log:
            self._nav2_bt_statuses[change.node_name] = change.current_status
            uid = self._nav2_bt_name_to_uid.get(change.node_name)
            if uid is not None:
                changes.append({
                    'uid': uid,
                    'name': change.node_name,
                    'tag': '',
                    'previous_status': change.previous_status,
                    'status': change.current_status,
                })
            if change.current_status == 'RUNNING':
                has_running = True

        # Detect navigation session start: emit bt_tree once per session so
        # the UI receives the full tree structure.
        if has_running and not self._nav2_bt_session_active:
            self._nav2_bt_session_active = True
            nodes_with_status = [
                {**nd, 'status': self._nav2_bt_statuses.get(nd['name'], 'IDLE')}
                for nd in self._nav2_bt_nodes_list
            ]
            self._on_bt_event({
                'type': 'bt_tree',
                'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'tree': self._nav2_bt_tree_structure,
                'nodes': nodes_with_status,
            })

        # Emit status changes
        if changes:
            self._on_bt_event({
                'type': 'bt_status',
                'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'changes': changes,
            })

    def _on_nav2_goal_status(self, msg):
        """Detect nav2 goal completion/cancellation via action status."""
        # action_msgs GoalStatus: ACCEPTED=1, EXECUTING=2, CANCELING=3
        has_active = any(s.status in (1, 2, 3) for s in msg.status_list)

        if self._nav2_bt_session_active and not has_active:
            self._nav2_bt_session_active = False
            self._nav2_bt_statuses.clear()
            if self._nav2_bt_tree_id:
                nodes_idle = [
                    {**nd, 'status': 'IDLE'}
                    for nd in self._nav2_bt_nodes_list
                ]
                self._on_bt_event({
                    'type': 'bt_tree',
                    'timestamp': time.time(),
                    'tree_id': self._nav2_bt_tree_id,
                    'tree': self._nav2_bt_tree_structure,
                    'nodes': nodes_idle,
                })

    def _on_nav2_bt_gone(self):
        """Called when the /behavior_tree_log topic disappears from the ROS graph."""
        if self._nav2_bt_tree_id is None:
            return  # Nothing was active, nothing to clear
        self.get_logger().info("Nav2 BT disappeared from ROS graph — clearing BT state")
        self._nav2_bt_session_active = False
        self._nav2_bt_statuses.clear()
        self._nav2_bt_tree_id = None
        self._nav2_bt_tree_structure = None
        self._nav2_bt_nodes_list = []
        self._nav2_bt_name_to_uid = {}
        self._on_bt_event({
            'type': 'bt_tree',
            'timestamp': time.time(),
            'tree_id': None,
            'tree': None,
            'nodes': [],
        })

    # Handle TF tree events
    def _on_tf_tree_event(self, event):
        """Forward tf_tree collector events to the websocket."""
        if not self.ws or not self.loop:
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._send_queue.put(json.dumps(event)),
                self.loop,
            )
        except Exception as e:
            self.get_logger().error(f"Failed to queue tf_tree event: {e}")

    # Handle ros2_control events (controllers_state / hardware_state)
    def _on_ros2_control_event(self, event):
        """Forward ros2_control collector events to the websocket."""
        if not self.ws or not self.loop:
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._send_queue.put(json.dumps(event)),
                self.loop,
            )
        except Exception as e:
            self.get_logger().error(f"Failed to queue ros2_control event: {e}")

    def destroy_node(self):
        """Clean up resources before destroying the node."""
        # Stop ros2_control collector
        self._ros2_control.destroy()

        # Stop TF tree collector
        self._tf_tree.destroy()

        # Stop BT collector
        if self._bt_collector:
            self._bt_collector.stop()
        
        super().destroy_node()


# Initialize ROS, create node, and run until shutdown
def main(args=None):
    rclpy.init(args=args)
    node = WebBridge()
    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, rclpy.executors.ExternalShutdownException):
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()