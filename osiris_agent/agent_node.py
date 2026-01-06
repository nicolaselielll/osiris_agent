import os
import asyncio
import threading
import time
import rclpy, subprocess
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
import uuid

class WebBridge(Node):
    def __init__(self):
        super().__init__('bridge_node')
        # Get base URL and auth token from environment
        base_url = os.environ.get('ROS_BRIDGE_WS_URL', 'wss://osiris-gateway.fly.dev')
        auth_token = os.environ.get('ROBOT_AUTH_TOKEN', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ3b3Jrc3BhY2VfaWQiOiJ4ZnppRkxUUXBwTW1yakp1dzczYiIsInJvYm90X2lkIjoiVkd6VG1qdWk3Rk5CZXJSQ21RRTkiLCJpYXQiOjE3Njc1NTI3NTB9.cXMMaUaXtQAuTuJuZtkuMQYDUIjuu3Q-1V1032w9t7k')
        self.ws_url = f"{base_url}?robot=true&token={auth_token}"
        self.ws = None
        self._topic_subs = {}
        self.loop = None
        self._send_queue = asyncio.Queue()
        self._active_nodes = set(self.get_node_names())
        self._active_topics = set(dict(self.get_topic_names_and_types()).keys())
        self._active_actions = set()  # Track action names
        self._active_services = set()  # Track service names
        self._action_status_subs = {}  # Track subscriptions to action status topics
        self._active_goals = {}  # Track active goals per action
        self._topic_relations = {}  # Track publishers/subscribers per topic
        self._action_relations = {}  # Track providers per action
        self._service_relations = {}  # Track providers per service
        self._telemetry_enabled = True  # Start telemetry automatically
        self._topic_last_timestamp = {}
        self._topic_rate_history = {}
        self._rate_history_depth = 8
        self._node_parameter_cache = {}
        self._parameter_fetch_inflight = {}
        
        # Cache for detecting changes before sending
        self._last_sent_nodes = None
        self._last_sent_topics = None
        self._last_sent_actions = None
        self._last_sent_services = None
        
        # Subscribe to graph changes for real-time event detection
        self._check_graph_changes()
        self.create_timer(0.1, self._check_graph_changes)
        self.create_timer(5.0, self._refresh_all_parameters)
        
        # Telemetry timer (runs every 1 second when enabled)
        self.create_timer(1.0, self._collect_telemetry)

        threading.Thread(target=self._run_ws_client, daemon=True).start()

    def _run_ws_client(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._client_loop_with_reconnect())

    async def _client_loop_with_reconnect(self):
        """Wrapper that handles reconnection."""
        reconnect_delay = 1  # Start with 1 second
        max_delay = 10  # Cap at 30 seconds
        
        while True:
            try:
                print(f"Attempting to connect to gateway...")
                await self._client_loop()
            except Exception as e:
                print(f"Connection failed: {e}")
            
            # Connection lost or failed
            print(f"Reconnecting in {reconnect_delay} seconds...")
            await asyncio.sleep(reconnect_delay)
            
            # Exponential backoff
            reconnect_delay = min(reconnect_delay * 2, max_delay)

    async def _client_loop(self):
        send_task = None
        try:
            async with websockets.connect(self.ws_url) as ws:
                self.ws = ws
                print(f"Connected to gateway")
                
                auth_msg = await ws.recv()
                data = json.loads(auth_msg)
                if data.get('type') == 'auth_success':
                    print("Authenticated with gateway")
                    
                    # Send initial state as SINGLE message
                    await self._send_initial_state()
                    
                    # Start send loop and keep reference
                    send_task = asyncio.create_task(self._send_loop(ws))
                    
                    # Receive loop
                    await self._receive_loop(ws)
        except Exception as e:
            print(f"Error in client loop: {e}")
            raise  # Re-raise to trigger reconnection
        finally:
            # Cancel send loop task
            if send_task and not send_task.done():
                send_task.cancel()
                try:
                    await send_task
                except asyncio.CancelledError:
                    pass
            
            self.ws = None
            print("Connection closed, cleaning up...")

    async def _send_initial_state(self):
        """Send complete initial state as a single message"""
        nodes = self._get_nodes_with_relations()
        actions = self._get_actions_with_relations()
        services = self._get_services_with_relations()
        topics = self._get_topics_with_relations()
        
        # Update caches
        self._last_sent_nodes = nodes.copy()
        self._last_sent_actions = actions.copy()
        self._last_sent_services = services.copy()
        self._last_sent_topics = topics.copy()
        
        # Send as single complete snapshot
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
        print(f"Sent initial state: {len(nodes)} nodes, {len(topics)} topics, {len(actions)} actions, {len(services)} services")
        
        # Send bridge subscriptions separately
        await self._send_bridge_subscriptions()

    async def _send_bridge_subscriptions(self):
        """Send current bridge subscriptions as a separate message."""
        message = {
            'type': 'bridge_subscriptions',
            'subscriptions': list(self._topic_subs.keys()),
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        print(f"Sent bridge subscriptions: {len(self._topic_subs)} topics")
                                                                                                  
    async def _receive_loop(self, ws):
        async for msg in ws:
            try:
                data = json.loads(msg)
                if data.get('type') == 'subscribe':
                    print(f"Subscribing to topic: {data.get('topic')}")
                    topic = data.get('topic')
                    self._subscribe_to_topic(topic)
                elif data.get('type') == 'unsubscribe':
                    topic = data.get('topic')
                    self._unsubscribe_from_topic(topic)
                elif data.get('type') == 'start_telemetry':
                    self._telemetry_enabled = True
                    print("Telemetry started")
                elif data.get('type') == 'stop_telemetry':
                    self._telemetry_enabled = False
                    print("Telemetry stopped")
            except Exception:
                pass

    async def _send_loop(self, ws):
        while True:
            msg = await self._send_queue.get()
            await ws.send(msg)

    def _subscribe_to_topic(self, topic_name):
        if topic_name in self._topic_subs:
            return
        
        topic_types = dict(self.get_topic_names_and_types()).get(topic_name)
        if not topic_types:
            return
        
        msg_class = get_message(topic_types[0])
        sub = self.create_subscription(
            msg_class,
            topic_name,
            lambda msg, t=topic_name: self._on_topic_msg(msg, t),
            QoSProfile(depth=10)
        )

        self._topic_subs[topic_name] = sub

        self._update_topic_relations()
        print(f"Subscribed to {topic_name}")
        
        # Send updated bridge subscriptions
        asyncio.create_task(self._send_bridge_subscriptions())

    def _unsubscribe_from_topic(self, topic_name):
        if topic_name in self._topic_subs:
            self.destroy_subscription(self._topic_subs[topic_name])
            del self._topic_subs[topic_name]

            self._update_topic_relations()
            print(f"Unsubscribed from {topic_name}")
            
            # Send updated bridge subscriptions
            asyncio.create_task(self._send_bridge_subscriptions())

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
        print(f"Received message on {topic_name}")
        self._send_event_and_update(event, f"Topic data: {topic_name}")

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

    def _get_topics_with_relations(self):
        """Get topics with their publishers and subscribers with QoS info (uses cached data)."""
        self._update_topic_relations()
        topics_with_relations = {}
        topic_types_dict = dict(self.get_topic_names_and_types())
        
        for topic_name, relations in self._topic_relations.items():
            # Get publisher info with QoS
            publishers_list = []
            pub_info_list = self.get_publishers_info_by_topic(topic_name)
            for pub_info in pub_info_list:
                publishers_list.append({
                    'node': pub_info.node_name,
                    'qos': self._qos_profile_to_dict(pub_info.qos_profile)
                })
            
            # Get subscriber info with QoS
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
            except Exception:
                # Skip parameters that cannot be converted
                pass
        return parameters

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

    def _refresh_all_parameters(self):
        for node_name in self.get_node_names():
            if node_name in self._parameter_fetch_inflight:
                continue
            self._start_parameter_fetch(node_name)

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
                except Exception:
                    pass

        self._node_parameter_cache[node_name] = params
        self._cleanup_parameter_fetch(node_name)

    def _cleanup_parameter_fetch(self, node_name):
        inflight = self._parameter_fetch_inflight.pop(node_name, None)
        if not inflight:
            return

        for key in ('list_client', 'get_client'):
            client = inflight.get(key)
            if client:
                self.destroy_client(client)

    def _get_nodes_with_relations(self):
        """Get nodes with the topics they publish and subscribe to (derived from cached topic relations)."""
        nodes_with_relations = {}
        
        # Update action and service relations cache first
        self._update_action_relations()
        self._update_service_relations()
        
        # Initialize all active nodes
        for node_name in self._active_nodes:
            nodes_with_relations[node_name] = {
                'publishes': [],
                'subscribes': [],
                'actions': [],
                'services': [],
                'parameters': {}
            }
        
        # Derive node relations from topic relations with QoS info
        for topic_name, relations in self._topic_relations.items():
            # Add this topic to publishers' lists with QoS
            for node_name in relations['publishers']:
                if node_name in nodes_with_relations:
                    # Get QoS profile for this publisher
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
            
            # Add this topic to subscribers' lists with QoS
            for node_name in relations['subscribers']:
                if node_name in nodes_with_relations:
                    # Get QoS profile for this subscriber
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
        
        # Derive action providers from action relations
        for action_name, relations in self._action_relations.items():
            for node_name in relations['providers']:
                if node_name in nodes_with_relations:
                    nodes_with_relations[node_name]['actions'].append(action_name)
        
        # Derive service providers from service relations
        for service_name, relations in self._service_relations.items():
            for node_name in relations['providers']:
                if node_name in nodes_with_relations:
                    nodes_with_relations[node_name]['services'].append(service_name)
        
        # Get parameters for each node
        cache = self._node_parameter_cache
        for node_name in nodes_with_relations.keys():
            nodes_with_relations[node_name]['parameters'] = cache.get(node_name, {})
        return nodes_with_relations

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

    def _get_actions_with_relations(self):
        """Get actions from status topics and update cached action relations."""
        self._update_action_relations()
        
        actions_with_relations = {}
        for action_name, relations in self._action_relations.items():
            actions_with_relations[action_name] = {
                'providers': list(relations['providers']),
            }
        
        return actions_with_relations

    def _update_service_relations(self):
        """Update the cached service relations."""
        service_relations = {}
        
        # Get all services first
        all_services = self.get_service_names_and_types()
        
        # For each service, find which nodes provide it
        for service_name, service_types in all_services:
            providers = set()
            # Check each node to see if it provides this service
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
                except:
                    pass
            
            service_relations[service_name] = {
                'providers': providers,
                'type': service_types[0] if service_types else 'unknown'
            }
        
        self._service_relations = service_relations

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

    def _check_graph_changes(self):
        """Check for node, topic, action, and publisher/subscriber changes."""
        current_nodes = set(self.get_node_names())
        current_topics = set(dict(self.get_topic_names_and_types()).keys())

        # Detect actions by finding /_action/status topics
        current_actions = {t.replace('/_action/status', '') for t in current_topics if t.endswith('/_action/status')}

        # Track publisher/subscriber changes
        current_topic_relations = {}

        # Persistent previous subscribers per topic
        if not hasattr(self, '_last_topic_subscribers'):
            self._last_topic_subscribers = {}

        for topic_name in current_topics:
            publishers = {pub.node_name for pub in self.get_publishers_info_by_topic(topic_name)}
            subscribers = {sub.node_name for sub in self.get_subscriptions_info_by_topic(topic_name)}
            current_topic_relations[topic_name] = {
                'publishers': publishers,
                'subscribers': subscribers
            }

            # Compare to previous subscribers for this topic
            prev_subs = self._last_topic_subscribers.get(topic_name, set())
            # Detect new subscribers (nodes that subscribed)
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

            # Detect removed subscribers (nodes that unsubscribed)
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

            # Still trigger update if publishers changed (no specific event, just graph update)
            if topic_name in self._topic_relations:
                old_pubs = self._topic_relations[topic_name]['publishers']
                if publishers != old_pubs:
                    self._send_event_and_update(None, f"Topic publishers changed: {topic_name}")

        # Update previous subscribers for next check
        self._last_topic_subscribers = {topic: set(rel['subscribers']) for topic, rel in current_topic_relations.items()}

        # Detect started nodes
        started_nodes = current_nodes - self._active_nodes
        for node_name in started_nodes:
            event = {
                'type': 'node_event',
                'node': node_name,
                'event': 'started',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Node started: {node_name}")
        
        # Detect stopped nodes
        stopped_nodes = self._active_nodes - current_nodes
        for node_name in stopped_nodes:
            event = {
                'type': 'node_event',
                'node': node_name,
                'event': 'stopped',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Node stopped: {node_name}")
        
        # Detect new topics
        started_topics = current_topics - self._active_topics
        for topic_name in started_topics:
            event = {
                'type': 'topic_event',
                'topic': topic_name,
                'event': 'created',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Topic created: {topic_name}")
        
        # Detect removed topics
        stopped_topics = self._active_topics - current_topics
        for topic_name in stopped_topics:
            event = {
                'type': 'topic_event',
                'topic': topic_name,
                'event': 'destroyed',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Topic destroyed: {topic_name}")
        
        # Detect new actions
        started_actions = current_actions - self._active_actions
        if started_actions:
            print(f"New actions detected: {started_actions}")
        for action_name in started_actions:
            event = {
                'type': 'action_event',
                'action': action_name,
                'event': 'created',
                'timestamp': time.time()
            }
            self._send_event_and_update(event, f"Action created: {action_name}")
        
        # Detect removed actions
        stopped_actions = self._active_actions - current_actions
        if stopped_actions:
            print(f"Actions stopped: {stopped_actions}")
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
        
        # Detect services
        current_services = {service_name for service_name, _ in self.get_service_names_and_types()}
        
        # Detect new services
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
        
        # Detect removed services
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
            
        # Update tracked state
        self._active_nodes = current_nodes
        self._active_topics = current_topics
        self._active_actions = current_actions
        self._active_services = current_services
        self._topic_relations = current_topic_relations

    def _send_event_and_update(self, event, log_message):
        """Send event and trigger update of all graph data."""
        if not self.ws or not self.loop:
            return

        if event:
            asyncio.run_coroutine_threadsafe(self._send_queue.put(json.dumps(event)), self.loop)
        
        # Trigger updates (will only send if data changed)
        asyncio.run_coroutine_threadsafe(self._send_topics(), self.loop)
        asyncio.run_coroutine_threadsafe(self._send_nodes(), self.loop)
        asyncio.run_coroutine_threadsafe(self._send_actions(), self.loop)
        asyncio.run_coroutine_threadsafe(self._send_services(), self.loop)
        
        print(log_message)

    async def _send_nodes(self):
        """Send current nodes list to gateway (only when changed)."""
        nodes = self._get_nodes_with_relations()
        
        # Check if nodes data has changed
        if self._last_sent_nodes == nodes:
            return  # No change, skip sending
        
        # Data changed, update cache and send
        self._last_sent_nodes = nodes.copy()
        
        message = {
            'type': 'nodes',
            'data': nodes,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        print(f"Sent nodes list: {list(nodes.keys())}")

    async def _send_topics(self):
        """Send current topics list to gateway (only when changed)."""
        topics = self._get_topics_with_relations()
        
        # Check if topics data has changed
        if self._last_sent_topics == topics:
            return  # No change, skip sending
        
        # Data changed, update cache and send
        self._last_sent_topics = topics.copy()
        
        message = {
            'type': 'topics',
            'data': topics,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        print(f"Sent topics list: {list(topics.keys())}")

    async def _send_actions(self):
        """Send current actions list to gateway (only when changed)."""
        actions = self._get_actions_with_relations()
        
        # Check if actions data has changed
        if self._last_sent_actions == actions:
            return  # No change, skip sending
        
        # Data changed, update cache and send
        self._last_sent_actions = actions.copy()
        
        message = {
            'type': 'actions',
            'data': actions,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        print(f"Sent actions list: {list(actions.keys())}")

    async def _send_services(self):
        """Send current services list to gateway (only when changed)."""
        services = self._get_services_with_relations()
        
        # Check if services data has changed
        if self._last_sent_services == services:
            return  # No change, skip sending
        
        # Data changed, update cache and send
        self._last_sent_services = services.copy()
        
        message = {
            'type': 'services',
            'data': services,
            'timestamp': time.time()
        }
        await self._send_queue.put(json.dumps(message))
        print(f"Sent services list: {list(services.keys())}")

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

    def _get_telemetry_snapshot(self):
        """Return a snapshot of system telemetry (CPU, RAM, disk)."""
        return {
            'cpu': psutil.cpu_percent(interval=None),
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


def main(args=None):
    rclpy.init(args=args)
    node = WebBridge()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()