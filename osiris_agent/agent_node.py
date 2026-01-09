import os
import asyncio
import threading
import time
import rclpy
from collections import deque
from rcl_interfaces.srv import GetParameters, ListParameters
from rclpy.node import Node
from rclpy.qos import QoSProfile
from rclpy.parameter import parameter_value_to_python
import websockets
import json
from rosidl_runtime_py.utilities import get_message
from rosidl_runtime_py import message_to_ordereddict
import psutil

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
        super().__init__('bridge_node')
        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")
        
        self.ws_url = f'wss://osiris-gateway.fly.dev?robot=true&token={auth_token}'
        self.ws = None
        self._topic_subs = {}
        self._topic_subs_lock = threading.Lock()
        self.loop = None
        self._send_queue = None
        self._active_nodes = set(self.get_node_names())
        self._active_topics = set(dict(self.get_topic_names_and_types()).keys())
        self._active_actions = set()
        self._active_services = set()
        self._action_status_subs = {}
        self._active_goals = {}
        self._topic_relations = {}
        self._action_relations = {}
        self._service_relations = {}
        self._telemetry_enabled = True
        self._topic_last_timestamp = {}
        self._topic_rate_history = {}
        self._rate_history_depth = 8
        self._node_parameter_cache = {}
        self._parameter_fetch_inflight = {}
        
        self._last_sent_nodes = None
        self._last_sent_topics = None
        self._last_sent_actions = None
        self._last_sent_services = None
        
        self._check_graph_changes()
        self.create_timer(0.1, self._check_graph_changes)
        self.create_timer(5.0, self._refresh_all_parameters)
        self.create_timer(1.0, self._collect_telemetry)

        threading.Thread(target=self._run_ws_client, daemon=True).start()

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
                self.get_logger().info("Attempting to connect to gateway...")
                await self._client_loop()
            except Exception as e:
                self.get_logger().error(f"Connection failed: {e}")
            
            self.get_logger().info(f"Reconnecting in {reconnect_delay} seconds...")
            await asyncio.sleep(reconnect_delay)
            
            reconnect_delay = min(reconnect_delay * 2, RECONNECT_MAX_DELAY)
            import random
            reconnect_delay += random.uniform(0, 1)  # Jitter prevents thundering herd

    # Main client loop for sending and receiving messages
    async def _client_loop(self):
        send_task = None
        try:
            async with websockets.connect(self.ws_url) as ws:
                self.get_logger().info("Connected to gateway (socket opened)")
                # Wait for gateway auth response before sending initial state
                try:
                    auth_msg = await ws.recv()
                except Exception as e:
                    self.get_logger().error(f"Failed to receive auth message: {e}")
                    return

                try:
                    auth_data = json.loads(auth_msg)
                except Exception:
                    auth_data = None

                self.get_logger().debug(f"Gateway auth message received: {auth_msg}")

                if not auth_data or auth_data.get('type') != 'auth_success':
                    self.get_logger().error(f"Gateway did not authenticate: parsed={auth_data}")
                    return

                self.get_logger().info("Authenticated with gateway")

                self.ws = ws

                send_task = asyncio.create_task(self._send_loop(ws))

                await self._send_initial_state()

                await self._receive_loop(ws)
        except Exception as e:
            self.get_logger().error(f"Error in client loop: {e}")
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
            }
        }
        
        await self._send_queue.put(json.dumps(message))
        self.get_logger().info(f"Sent initial state: {len(nodes)} nodes, {len(topics)} topics, {len(actions)} actions, {len(services)} services")
        
        await self._send_bridge_subscriptions()

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
                else:
                    self.get_logger().warn(f"Unknown message type: {msg_type}")
                    
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
            publishers = {pub.node_name for pub in self.get_publishers_info_by_topic(topic_name)}
            subscribers = {sub.node_name for sub in self.get_subscriptions_info_by_topic(topic_name)}
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
                    'node': pub_info.node_name,
                    'qos': self._qos_profile_to_dict(pub_info.qos_profile)
                })
            
            subscribers_list = []
            sub_info_list = self.get_subscriptions_info_by_topic(topic_name)
            for sub_info in sub_info_list:
                subscribers_list.append({
                    'node': sub_info.node_name,
                    'qos': self._qos_profile_to_dict(sub_info.qos_profile)
                })
            
            topics_with_relations[topic_name] = {
                'type': topic_types_dict.get(topic_name, ['unknown'])[0],
                'publishers': publishers_list,
                'subscribers': subscribers_list,
            }
        return topics_with_relations

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

    # Get all parameters for a node using ROS services
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

    # Trigger async parameter fetch for all nodes
    def _refresh_all_parameters(self):
        for node_name in self.get_node_names():
            if node_name in self._parameter_fetch_inflight:
                continue
            self._start_parameter_fetch(node_name)

    # Begin async parameter fetch for a node
    def _start_parameter_fetch(self, node_name):
        service_prefix = node_name if node_name.startswith('/') else f"/{node_name}"
        service_name = f"{service_prefix}/list_parameters"
        client = self.create_client(ListParameters, service_name)
        if not client.wait_for_service(timeout_sec=0.2):
            self.destroy_client(client)
            return

        request = ListParameters.Request()
        request.depth = 10
        future = client.call_async(request)
        self._parameter_fetch_inflight[node_name] = {
            'list_client': client,
            'get_client': None,
            'get_names': None,
        }
        future.add_done_callback(
            lambda fut, node=node_name, client=client: self._on_list_parameters(node, fut, client)
        )

    # Handle list_parameters response, start get_parameters request
    def _on_list_parameters(self, node_name, future, client):
        inflight = self._parameter_fetch_inflight.get(node_name)
        if not inflight:
            self.destroy_client(client)
            return

        self.destroy_client(client)
        inflight['list_client'] = None

        response = None
        try:
            response = future.result()
        except Exception:
            pass

        if not response or not response.result.names:
            self._node_parameter_cache[node_name] = {}
            self._cleanup_parameter_fetch(node_name)
            return

        names = response.result.names
        inflight['get_names'] = names

        service_prefix = node_name if node_name.startswith('/') else f"/{node_name}"
        service_name = f"{service_prefix}/get_parameters"
        get_client = self.create_client(GetParameters, service_name)
        if not get_client.wait_for_service(timeout_sec=0.2):
            self.destroy_client(get_client)
            self._cleanup_parameter_fetch(node_name)
            return

        request = GetParameters.Request()
        request.names = names
        future = get_client.call_async(request)
        inflight['get_client'] = get_client
        future.add_done_callback(
            lambda fut, node=node_name, client=get_client: self._on_get_parameters(node, fut, client)
        )

    # Handle get_parameters response, update cache
    def _on_get_parameters(self, node_name, future, client):
        inflight = self._parameter_fetch_inflight.get(node_name)
        if not inflight:
            self.destroy_client(client)
            return

        self.destroy_client(client)
        inflight['get_client'] = None

        response = None
        try:
            response = future.result()
        except Exception:
            pass

        params = {}
        names = inflight.get('get_names') or []
        if response:
            for name, value in zip(names, response.values):
                try:
                    params[name] = parameter_value_to_python(value)
                except Exception as e:
                    self.get_logger().debug(f"Could not convert parameter {name} for {node_name}: {e}")

        self._node_parameter_cache[node_name] = params
        self._cleanup_parameter_fetch(node_name)

    # Clean up parameter fetch clients and state
    def _cleanup_parameter_fetch(self, node_name):
        inflight = self._parameter_fetch_inflight.pop(node_name, None)
        if not inflight:
            return

        for key in ('list_client', 'get_client'):
            client = inflight.get(key)
            if client:
                self.destroy_client(client)

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
                        if pub_info.node_name == node_name:
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
                        if sub_info.node_name == node_name:
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
                providers = [info.node_name for info in self.get_publishers_info_by_topic(topic_name[0])]
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
            for node_name in self.get_node_names():
                try:
                    # Extract namespace from node name (format: /namespace/node_name or /node_name)
                    node_namespace = '/'
                    if '/' in node_name[1:]:  # Has namespace
                        parts = node_name[1:].split('/', 1)
                        node_namespace = '/' + parts[0]
                        node_only = parts[1]
                    else:  # No namespace
                        node_only = node_name[1:] if node_name.startswith('/') else node_name
                    
                    node_services = self.get_service_names_and_types_by_node(node_only, node_namespace)
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
            services_with_relations[service_name] = {
                'providers': list(relations['providers']),
                'type': relations['type']
            }
        
        return services_with_relations

    # Poll ROS graph for changes and send events
    def _check_graph_changes(self):
        """Check for node, topic, action, and publisher/subscriber changes."""
        current_nodes = set(self.get_node_names())
        current_topics = set(dict(self.get_topic_names_and_types()).keys())

        current_actions = {t.replace('/_action/status', '') for t in current_topics if t.endswith('/_action/status')}

        current_topic_relations = {}

        if not hasattr(self, '_last_topic_subscribers'):
            self._last_topic_subscribers = {}

        for topic_name in current_topics:
            publishers = {pub.node_name for pub in self.get_publishers_info_by_topic(topic_name)}
            subscribers = {sub.node_name for sub in self.get_subscriptions_info_by_topic(topic_name)}
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

        self._last_topic_subscribers = {topic: set(rel['subscribers']) for topic, rel in current_topic_relations.items()}

        started_nodes = current_nodes - self._active_nodes
        for node_name in started_nodes:
            event = {
                'type': 'node_event',
                'node': node_name,
                'event': 'started',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Node started: {node_name}")
        
        stopped_nodes = self._active_nodes - current_nodes
        for node_name in stopped_nodes:
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
        if started_actions:
            self.get_logger().info(f"New actions detected: {started_actions}")
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

    # Send event to gateway and trigger graph updates
    def _send_event_and_update(self, event, log_message):
        """Send event and trigger update of all graph data."""
        if not self.ws or not self.loop:
            return

        if event:
            asyncio.run_coroutine_threadsafe(self._send_queue.put(json.dumps(event)), self.loop)
        
        asyncio.run_coroutine_threadsafe(self._send_topics(), self.loop)
        asyncio.run_coroutine_threadsafe(self._send_nodes(), self.loop)
        asyncio.run_coroutine_threadsafe(self._send_actions(), self.loop)
        asyncio.run_coroutine_threadsafe(self._send_services(), self.loop)
        
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


# Initialize ROS, create node, and run until shutdown
def main(args=None):
    rclpy.init(args=args)
    node = WebBridge()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()