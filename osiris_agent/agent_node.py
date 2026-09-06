import asyncio
import base64
import http.client
import io
import math
import os
import platform
import random
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import zipfile
from collections import deque
from pathlib import Path

import psutil
import rclpy
import websockets

# Pillow re-encodes raw sensor_msgs/Image frames to JPEG before they hit the
# binary-framing path (see _reencode_image_jpeg) - a real install_requires
# dependency (setup.py), but imported defensively rather than unconditionally:
# an agent updated via `git pull` without a matching `pip install -e .` would
# otherwise hard-crash at startup over what's really just a bandwidth nicety.
# Missing Pillow just means Image topics keep sending raw pixel bytes, same
# as before this feature existed.
try:
    from PIL import Image as PILImage
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
import json

from rcl_interfaces.msg import ParameterEvent
from rcl_interfaces.srv import GetParameters, ListParameters
from rclpy.action import ActionClient
from rclpy.node import Node
from std_msgs.msg import Empty as EmptyMsg
from rclpy.parameter import Parameter, parameter_value_to_python
from rclpy.qos import QoSProfile, qos_profile_action_status_default
from rosidl_runtime_py import message_to_ordereddict
from rosidl_runtime_py.utilities import get_message

from osiris_agent import __version__ as AGENT_VERSION
from .bt_collector import BTCollector
from .ros2_control_collector import Ros2ControlCollector
from .tf_tree_collector import TfTreeCollector

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
TELEMETRY_INTERVAL         = 1.0   # seconds between telemetry samples
MAX_TELEMETRY_PROCESSES    = 15    # cap on processes reported per telemetry sample
# The process list is by far the most expensive part of a telemetry sample —
# a full psutil.process_iter() pass over every process on the host, then a
# oneshot() deep read (cmdline/memory/username/threads) for the top N — for
# something that doesn't meaningfully change second to second the way
# cpu/ram/disk/net do. Computed and sent only once every this many ticks;
# cpu/ram/disk/net/battery stay on the plain 1Hz TELEMETRY_INTERVAL. The
# client is expected to hold onto the last processes list it received
# in between (see stores/robot.js's telemetry handler), not clear it.
TELEMETRY_PROCESS_EVERY_N_TICKS = 5
MAX_SUBSCRIPTIONS          = 100   # hard cap on gateway-requested topic subs
RECONNECT_INITIAL_DELAY    = 1     # seconds
RECONNECT_MAX_DELAY        = 30    # seconds

# ROS2 message types whose payload is dominated by one large byte array. For
# these, _on_topic_msg strips that field out of the JSON topic_data message
# and sends it as a separate raw binary WS frame instead — see _on_topic_msg
# and BINARY_MARKER_KEY. message_to_ordereddict + json.dumps would otherwise
# turn e.g. a 640x480 rgb8 Image's ~920KB data field into a JSON array of
# ints (commas, digit characters, no base64 even) — 3-6x the raw byte count,
# and by far the single biggest source of avoidable bytes on the agent's
# uplink. All four types below happen to name the field 'data'; the field for
# OccupancyGrid is int8 (needs sign-aware decoding on the way back, see
# BINARY_MARKER_KEY), the other three are uint8.
BINARY_PAYLOAD_FIELD = 'data'
BINARY_PAYLOAD_TYPES = {
    'sensor_msgs/msg/Image':          False,  # uint8[]
    'sensor_msgs/msg/CompressedImage': False,  # uint8[]
    'sensor_msgs/msg/PointCloud2':    False,  # uint8[]
    'nav_msgs/msg/OccupancyGrid':     True,   # int8[] — signed, see BINARY_MARKER_KEY
}
# Marker substituted for BINARY_PAYLOAD_FIELD in the JSON header — must match
# the gateway's BINARY_MARKER_KEY (index.js) exactly, it's the two ends of one
# wire protocol. 'len' is the byte count the binary frame right behind this
# header is expected to carry (gateway-side sanity check only); 'signed' says
# whether those bytes are int8 (two's-complement, e.g. OccupancyGrid's -1
# "unknown" cells) or plain uint8.
BINARY_MARKER_KEY = '__osiris_binary__'

# sensor_msgs/Image encodings _reencode_image_jpeg knows how to decode -
# (PIL mode, PIL raw-decoder rawmode) pairs. PIL's 'raw' decoder accepts
# rawmode directly as e.g. 'BGR'/'BGRA' and does the channel reorder itself,
# so bgr8/bgra8 (by far the most common OpenCV-sourced encodings) need no
# manual byte-swapping - just the right rawmode string. Depth encodings
# (16UC1, 32FC1, ...) and bayer/YUV formats aren't listed; those topics keep
# sending raw bytes exactly as before this feature existed.
IMAGE_ENCODING_PIL_MODES = {
    'rgb8':  ('RGB', 'RGB'),
    'bgr8':  ('RGB', 'BGR'),
    'mono8': ('L', 'L'),
    'rgba8': ('RGBA', 'RGBA'),
    'bgra8': ('RGBA', 'BGRA'),
}
_IMAGE_ENCODING_CHANNELS = {'L': 1, 'RGB': 3, 'BGR': 3, 'RGBA': 4, 'BGRA': 4}

# Services to suppress from graph output (internal ROS2 plumbing)
_SUPPRESSED_SERVICE_PREFIXES = ('/ros2cli_daemon',)

ACTION_FEEDBACK_MIN_INTERVAL = 0.2  # seconds between forwarded feedback messages per action (5 Hz cap)

# Fixed enum of Nav2 actions send_command_request can trigger — deliberately
# not a generic "call any action with any payload" capability. Each entry:
# action_name = the ROS2 action server this sends goals to.
# module/cls   = lazily imported (nav2_msgs may not be installed everywhere).
# precondition_nodes = lifecycle-managed nodes that must be 'active' (per
#   _lifecycle_state_cache) beyond the action server itself existing.
#
# navigate_to_pose (absolute map-frame navigation) was deliberately left out
# for now — its precondition check would need more than a lifecycle-active
# check on /amcl (that only confirms AMCL is running, not that it's actually
# converged to a good estimate), it has no way to be pointed anywhere useful
# yet (no named-locations system resolving something like "kitchen" to real
# coordinates), and no established workflow needs it — route steps are
# drive/turn/lidar_snapshot only. Revisit once those exist.
COMMAND_DEFS = {
    'spin': {
        'action_name': '/spin',
        'module': 'nav2_msgs.action',
        'cls_name': 'Spin',
        'precondition_nodes': [],
    },
    # Straight-line relative drive, robot-base-frame — same nav2_behaviors
    # family as spin, no localization dependency. This is deliberately what
    # route "drive" steps use: it doesn't need AMCL to be trustworthy, and
    # it's guaranteed to be a straight line, not just *a* planned path — both
    # matter for the LiDAR wall-normal ground-truth technique specifically.
    'drive': {
        'action_name': '/drive_on_heading',
        'module': 'nav2_msgs.action',
        'cls_name': 'DriveOnHeading',
        'precondition_nodes': [],
    },
}
COMMAND_SERVER_DISCOVERY_GRACE_S = 5.0  # server_is_ready() can lag right after ActionClient creation

# Default time_allowance for Spin/DriveOnHeading goals when the caller doesn't
# specify one (see _build_command_goal) — Nav2 treats an unset/zero Duration
# as unbounded, which was the actual behavior before this default existed.
DEFAULT_TIME_ALLOWANCE_S = 60.0

# How old a cached battery/dock reading is allowed to be before a send_command
# request treats it as unknown rather than trusting it (see
# _check_battery_and_dock_preconditions). Covers both "never received a
# message" and "the publisher went away" with the same fail-closed check —
# either way, no reading newer than this means no trustworthy current answer.
STATUS_STALE_AFTER_S = 10.0

# Safety-valve for the "one command at a time" slot: normally it's released by
# _on_command_goal_result, which only fires once the goal's result actually
# arrives — itself dependent on the goal cleanly reaching a terminal state on
# the ROS2 side. A behavior that never calls succeed()/abort()/is never
# properly cancelled leaves that callback permanently unfired, wedging every
# future send_command_request behind 'command_already_in_progress' until the
# agent process is restarted. This bounds that: same generous "a slow command
# is normal, not a hang" reasoning as the gateway's own ROUTE_STEP_GOAL_
# TIMEOUT_MS, just enforced agent-side too since the gateway's wait is a
# separate, shorter, per-HTTP-request thing that timing out doesn't clear
# this slot either.
ACTIVE_COMMAND_TIMEOUT_S = 120.0

class WebBridge(Node):

    def __init__(self, watcher_proc=None):
        super().__init__('osiris_node')
        self._watcher_proc = watcher_proc

        # Names explicitly set via --params-file/CLI at launch — distinct
        # from self.get_parameter(name), which can't tell "the user
        # explicitly passed this" apart from "nothing was passed, this is
        # just the hardcoded declare_parameter default." _apply_agent_config
        # needs that distinction to give a deliberate local override
        # precedence over the cloud agent_config (see _resolve_config_value).
        # Node.get_parameter_overrides() isn't available on this rclpy
        # version — self._parameter_overrides is the underlying dict rclpy
        # itself populates from --params-file/CLI before any
        # declare_parameter() call consumes it, and is what that method
        # would have wrapped anyway.
        self._param_overrides = set(self._parameter_overrides.keys())

        auth_token = os.environ.get('OSIRIS_AUTH_TOKEN')
        if not auth_token:
            raise ValueError("OSIRIS_AUTH_TOKEN environment variable must be set")

        # Declare tunable parameters
        self.declare_parameter('telemetry_enabled',      True)
        self.declare_parameter('goals_enabled',          True)
        self.declare_parameter('params_enabled',         True)
        self.declare_parameter('tf_tree_enabled',        False)
        self.declare_parameter('ros2_control_enabled',        False)
        self.declare_parameter('ros2_control_poll_interval',    2.0)
        self.declare_parameter('battery_topic',          '/battery_state')
        # Toggle + value, same split as tf_tree_enabled/tf_tree_poll_interval
        # and ros2_control_enabled/ros2_control_poll_interval above — not a
        # 0/empty sentinel on the value itself, so a real threshold/topic can
        # be kept configured while toggled off, and so there's an actual
        # switch in the UI rather than "clear the field to disable it". Both
        # default off — forcing either on by default would start rejecting
        # drive/spin on any robot whose battery topic doesn't report a usable
        # percentage, or that has no dock at all. See
        # _check_battery_and_dock_preconditions.
        self.declare_parameter('battery_check_enabled',  False)
        self.declare_parameter('battery_min_percent',    30.0)
        self.declare_parameter('dock_check_enabled',     False)
        self.declare_parameter('dock_status_topic',      '/dock_status')
        # Replaces the old bt_collector_enabled boolean — Nav2 BT and BT.CPP
        # share a single event pipeline (_on_bt_event/_cached_bt_tree_event,
        # no source tagging), so both being active at once would already
        # corrupt each other's state. One three-way setting makes that
        # mutual exclusion structural instead of a UI convention to enforce.
        # Default 'nav2' matches today's actual behavior (Nav2 BT always-on,
        # BT.CPP off).
        self.declare_parameter('bt_mode',               'nav2')  # 'off' | 'nav2' | 'btcpp'
        self.declare_parameter('bt_host',               '127.0.0.1')
        self.declare_parameter('bt_server_port',         1667)
        self.declare_parameter('bt_publisher_port',      1668)
        self.declare_parameter('tf_tree_poll_interval',   0.2)
        self.declare_parameter('graph_debounce_interval',   1.0)
        self.declare_parameter('bag_output_dir',            '~/ros2_bags')
        # Max rate (Hz) topic_data messages get forwarded at, per topic; 0
        # disables the throttle entirely. See _on_topic_msg. This is the
        # DEFAULT applied to every topic - a specific topic in
        # self._topic_limit_overrides (set live via agent_config's
        # 'topic_limits', see _apply_agent_config) takes precedence over it.
        self.declare_parameter('topic_data_rate_hz',         50.0)
        # Same default-with-per-topic-override relationship as
        # topic_data_rate_hz above, just bytes/sec instead of messages/sec —
        # this is what actually protects against a single heavy topic (a
        # pointcloud, an uncompressed image) rather than just a chatty one. 0
        # disables it. See _topic_data_over_budget.
        self.declare_parameter('topic_data_max_bytes_per_sec', 0.0)
        # Connection-wide budgets, shared across every subscribed topic - the
        # actual constraint being protected is the agent's one uplink, which
        # no per-topic cap alone can protect (five topics each individually
        # "within budget" can still saturate the link combined). Only
        # topic_data competes for these; graph state, lifecycle events, etc.
        # go through plain _enqueue and are never throttled here on purpose,
        # so control-plane traffic never starves because the sensor stream is
        # busy. Both 0 = unlimited. See _topic_data_over_budget.
        self.declare_parameter('global_topic_data_max_bytes_per_sec', 0.0)
        self.declare_parameter('global_topic_data_max_msgs_per_sec',  0.0)
        # sensor_msgs/Image only (CompressedImage is already compressed by
        # the camera driver, left untouched) - re-encoded to JPEG before
        # binary framing, see _reencode_image_jpeg. Unlike the caps above,
        # these default ON rather than to a disabled/0 state: raw
        # uncompressed pixel bytes are close to worst case for a live feed,
        # so a sane default (unlike an opt-in cap) makes the agent better
        # out of the box, not just capable of being tuned. Same default-
        # with-per-topic-override relationship as topic_data_rate_hz - an
        # override in self._topic_limit_overrides ('jpeg_quality'/
        # 'max_dimension') takes precedence per topic.
        self.declare_parameter('image_jpeg_quality', 70)     # 1-95, JPEG quality
        self.declare_parameter('image_max_dimension', 640)   # long edge, px; 0 = never resize

        base_url = os.environ.get('OSIRIS_WS_URL', 'wss://osiris-gateway.fly.dev')
        self.ws_url = f'{base_url}?robot=true&token={auth_token}'
        # self.ws_url = f'ws://host.docker.internal:8080?robot=true&token={auth_token}'

        self.ws   = None
        self.loop = None
        self._send_queue: asyncio.Queue | None = None

        # ── Bag recording ─────────────────────────────────────────────────────
        self._bag_proc: subprocess.Popen | None = None
        self._bag_output_path: str | None = None
        self._bag_lock = threading.Lock()

        # ── Topic subscriptions (gateway-requested) ──────────────────────────
        self._topic_subs: dict[str, rclpy.subscription.Subscription] = {}
        self._topic_subs_lock = threading.Lock()
        # topic -> resolved ROS2 type string, set alongside _topic_subs. Lets
        # _on_topic_msg decide whether this topic's messages get the binary
        # WS-frame treatment (see BINARY_PAYLOAD_TYPES) without re-querying
        # the graph on every single message.
        self._topic_msg_types: dict[str, str] = {}
        # Rolling window of receipt timestamps per subscribed topic, used to
        # recompute each topic's rate_hz on a 1Hz timer (see _publish_topic_rates)
        # rather than piggybacking a value on topic_data itself — a piggybacked
        # rate only updates when a new message arrives, so it freezes at its last
        # value instead of decaying toward zero once a topic goes quiet.
        self._topic_rate_timestamps: dict[str, deque] = {}
        # Same idea as _topic_rate_timestamps, (ts, size) pairs instead of
        # bare timestamps - the bytes/sec counterpart shown next to rate_hz
        # in the UI (see _publish_topic_rates). Deliberately separate from
        # _topic_byte_bucket below: that one is a 1s enforcement bucket that
        # resets on a hard boundary (fine for a threshold check, useless for
        # display - read at the wrong instant it'd show a stale zero right
        # after a reset even at full rate). Same lock/window as
        # _topic_rate_timestamps since both only ever feed that one
        # 1Hz report.
        self._topic_byte_timestamps: dict[str, deque] = {}
        # Same two rolling windows again, but recorded only for a message
        # that actually gets sent (see _topic_data_over_budget's own
        # not-over-budget branch, the one place both the Hz throttle above
        # and the byte budget have already been survived) - the "what's
        # really going out" counterpart to _topic_rate_timestamps/
        # _topic_byte_timestamps above, which measure demand before either
        # gate. Shown in the UI as "source -> delivered" instead of
        # "source -> configured limit", since delivered can genuinely sit
        # anywhere under a byte cap depending on how frames happen to size
        # that second - a hardcoded target was never an honest answer to
        # "what's actually arriving".
        self._topic_delivered_rate_timestamps: dict[str, deque] = {}
        self._topic_delivered_byte_timestamps: dict[str, deque] = {}
        self._topic_rate_lock = threading.Lock()
        self._RATE_WINDOW_S = 5.0
        # Last-forwarded time per topic, used to cap how often topic_data
        # actually gets serialized and sent (see _on_topic_msg) — independent
        # of _topic_rate_timestamps above, which keeps sampling every real
        # receipt so rate_hz stays accurate regardless of this throttle.
        self._topic_data_throttle: dict[str, float] = {}
        # topic -> (window_start_ts, bytes_forwarded_in_window), and the
        # connection-wide equivalent right below — both only ever touched
        # from _on_topic_msg, which (like _topic_data_throttle above) only
        # ever runs on the single ROS executor thread, so neither needs a
        # lock. See _topic_data_over_budget.
        self._topic_byte_bucket: dict[str, tuple[float, int]] = {}
        self._global_topic_data_bucket: tuple[float, int, int] = (0.0, 0, 0)  # (window_start_ts, bytes, msgs)
        # topic -> {'rate_hz': float, 'max_bytes_per_sec': float} — per-topic
        # overrides of the topic_data_rate_hz/topic_data_max_bytes_per_sec
        # defaults above, set live from agent_config's 'topic_limits' (see
        # _apply_agent_config). Written from the asyncio-loop thread
        # (_apply_agent_config, via _receive_loop) and read from the ROS
        # executor thread (_on_topic_msg) — genuinely cross-thread, unlike
        # the two buckets above, hence the lock (same reasoning as
        # _topic_subs_lock guarding _topic_msg_types).
        self._topic_limit_overrides: dict[str, dict] = {}
        self._topic_limit_lock = threading.Lock()
        # Topics whose Image encoding _reencode_image_jpeg doesn't recognize
        # (a depth/bayer/YUV camera, say) - warned once per topic rather than
        # once per frame, since the encoding is a fixed property of the
        # topic and will never suddenly become supported mid-stream.
        self._image_reencode_unsupported_warned: set[str] = set()
        # topic -> last time _topic_data_over_budget logged a drop for it,
        # throttled to once per 5s per topic rather than once per dropped
        # frame (which, under a tight budget, could be every single frame).
        self._budget_drop_logged: dict[str, float] = {}
        # Same idea, for the Hz throttle in _on_topic_msg below - a topic
        # forwarding far below its real publish rate with nothing in the log
        # explaining why is exactly what made a low topic_data_rate_hz/
        # per-topic rate_hz override (agent_config, possibly set weeks ago
        # for a since-forgotten reason) hard to tell apart from a genuine
        # processing bottleneck or a slow source.
        self._rate_throttle_drop_logged: dict[str, float] = {}

        # ── Existence caches (set of fully-qualified names) ───────────────────
        self._active_nodes:    set[str] = set()
        self._active_topics:   set[str] = set()
        self._active_services: dict[str, str] = {}
        self._active_actions:  set[str] = set()

        # ── Action type cache ─────────────────────────────────────────────────
        # None  = fetch attempted but failed (package not installed, etc.)
        # dict  = { goal_type, result_type, feedback_type,
        #           goal_fields, result_fields, feedback_fields }
        self._action_type_cache: dict[str, dict | None] = {}

        # ── Action monitoring (status + feedback) ─────────────────────────────
        self._action_status_subs:       dict[str, rclpy.subscription.Subscription] = {}
        self._action_feedback_subs:     dict[str, rclpy.subscription.Subscription] = {}
        self._action_goal_states:       dict[str, dict[str, int]] = {}  # action → {uuid_hex → status_int}
        self._action_feedback_throttle: dict[str, float] = {}           # action → last_sent_time
        # None = not yet resolved by _apply_agent_config — the graph scan
        # still runs and populates _active_actions (Graph's action listing
        # needs that regardless), but _subscribe_action_status is skipped
        # until this becomes True, same "don't do the work just to undo it"
        # reasoning as TF tree. Once resolved, toggling calls _subscribe_
        # action_status/_unsubscribe_action_monitoring for every action
        # already known in _active_actions to catch up/tear down.
        self._goals_enabled_default = self.get_parameter('goals_enabled').get_parameter_value().bool_value
        self._goals_enabled = None

        # ── Count sentinels (cheap change detection) ─────────────────────────
        self._topic_counts: dict[str, tuple[int, int]] = {}  # topic → (pub_n, sub_n)

        # ── Relation caches (populated by Tier-2 enrichment) ─────────────────
        self._topic_relations: dict[str, dict] = {}

        # ── Enrichment pending queues ─────────────────────────────────────────
        self._pending_topic_enrichment: set[str] = set()

        # ── Parameters (lazy-loaded, async) ──────────────────────────────────
        self._node_parameter_cache: dict[str, dict | None] = {}  # None = not yet fetched, {} = fetched but empty
        self._pending_param_fetches: set[str] = set()
        self._nodes_no_param_service: set[str] = set()  # nodes whose list_parameters was never ready
        # None = not yet resolved by _apply_agent_config. Requires graph_enabled
        # in spirit — new nodes' params are only ever discovered via the graph
        # tick, so if the reactive tick were ever disabled this would just
        # cover whatever nodes existed at first tick (not built yet, see
        # earlier discussion — no separate graph toggle exists today).
        self._params_enabled_default = self.get_parameter('params_enabled').get_parameter_value().bool_value
        self._params_enabled = None

        # ── Lifecycle subscriptions (auto-detected managed nodes) ─────────────
        self._lifecycle_subs: dict[str, rclpy.subscription.Subscription] = {}  # topic → sub
        self._lifecycle_state_cache: dict[str, str] = {}   # node_fqn → state label
        self._pending_lifecycle_fetches: set[str] = set()  # node_fqns with in-flight get_state calls

        # ── Command sending (send_command_request / cancel_command_request) ───
        # Fixed enum of known Nav2 actions the agent will send goals to —
        # deliberately not a generic "call any action" capability, same
        # bounded-blast-radius reasoning that ruled out generic CLI/service
        # execution elsewhere in this project. Goal PROGRESS/OUTCOME is not
        # re-reported here — it already flows through the existing generic
        # _on_action_status/_on_action_feedback pipeline (goal_event etc.),
        # which monitors every action in the graph regardless of who sent the
        # goal. This section only handles: precondition check, sending,
        # accept/reject, and the manual-cancel channel.
        self._command_action_clients: dict[str, object] = {}  # command name → ActionClient
        self._active_command_lock = threading.Lock()
        self._active_command_pending = False       # True from claim until accept/reject is known
        self._active_command_goal_handle = None     # set once accepted, cleared on terminal/cancel
        self._active_command_timeout_timer = None   # force-releases the slot if the goal never
                                                      # produces a result at all (see _on_command_
                                                      # send_response / _force_release_active_command)

        # ── Snapshot & dirty-flag ─────────────────────────────────────────────
        self._last_sent_nodes:    dict | None = None
        self._last_sent_topics:   dict | None = None
        self._last_sent_actions:  dict | None = None
        self._last_sent_services: dict | None = None
        self._graph_dirty = False
        self._graph_debounce_timer: threading.Timer | None = None
        self._graph_check_pending = False  # set when a trigger arrives while a check is already running

        # ── Service scan throttle ─────────────────────────────────────────────
        self._service_rescan_ticks: int = 0

        # ── Initial scan synchronization ──────────────────────────────────────
        self._initial_scan_complete = threading.Event()
        self._first_graph_check_done = False
        self._graph_check_lock = threading.Lock()  # serializes concurrent _check_graph_changes calls
        self._param_fetch_timer = None  # one-shot timer for delayed initial param fetch
        # ── BT state ─────────────────────────────────────────────────────────
        self._cached_bt_tree_event: dict | None = None

        # ── Telemetry ─────────────────────────────────────────────────────────
        self._last_disk_io      = None
        self._last_net_io       = None
        self._last_io_time:     float | None = None
        self._last_battery_state: dict | None = None
        self._last_battery_state_time: float | None = None
        self._telemetry_tick = 0  # counts periodic samples — see TELEMETRY_PROCESS_EVERY_N_TICKS
        psutil.cpu_percent(interval=None)  # prime — first call always returns 0.0
        # Starts at the local ROS param (yaml value if the user set one, else
        # its declared default of True) — overridden by the gateway's
        # agent_config push once connected, see _apply_agent_config. If that
        # push never arrives (offline, gateway didn't have a config saved,
        # etc.) this local value is simply never overwritten, so the yaml/
        # default value stands. The timer itself always runs; this just gates
        # whether each tick actually sends anything, so toggling it takes
        # effect on the very next tick with no timer start/stop bookkeeping.
        self._telemetry_enabled_default = self.get_parameter('telemetry_enabled').get_parameter_value().bool_value
        self._telemetry_enabled = self._telemetry_enabled_default

        # ── Collectors ────────────────────────────────────────────────────────
        # Not constructed here — same reasoning as TF Tree below: building a
        # Ros2ControlCollector now from the local param and possibly tearing
        # it straight back down once agent_config arrives would be wasted
        # work. Left as None until _apply_agent_config resolves the final
        # answer (config override, else this local default) and constructs
        # it at most once. self._ros2_control is already checked for None
        # everywhere it's read, so nothing else needs to change.
        self._ros2_control_enabled_default = self.get_parameter('ros2_control_enabled').get_parameter_value().bool_value
        self._ros2_control_poll_interval_default = self.get_parameter('ros2_control_poll_interval').get_parameter_value().double_value
        self._ros2_control = None
        self._ros2_control_poll_interval = None
        # Not constructed here — TfTreeCollector isn't free (real TF buffer/
        # listener), so building it now from the local param and possibly
        # tearing it straight down once agent_config arrives would be wasted
        # work. Left as None until _apply_agent_config resolves the final
        # answer (config override, else this local default) and constructs
        # it at most once. self._tf_tree is already checked for None
        # everywhere it's read, so nothing else needs to change.
        self._tf_tree_enabled_default = self.get_parameter('tf_tree_enabled').get_parameter_value().bool_value
        self._tf_tree = None

        # ── Timers ────────────────────────────────────────────────────────────
        # Subscribe to C++ graph watcher events — event-driven polls.
        # The one-shot startup timer guarantees an initial scan even when the
        # C++ watcher binary is unavailable (e.g. pip install without binary).
        # Use VOLATILE (default) depth=10: the startup timer handles initial state,
        # and live events are reliably received without TRANSIENT_LOCAL replay
        # which would race with the 1s startup timer on a background thread.
        self.create_subscription(
            EmptyMsg, '/osiris/graph_changed',
            self._on_graph_changed, 10,
        )
        self.create_subscription(
            ParameterEvent, '/parameter_events',
            self._on_parameter_event, 100,
        )
        self._startup_check_timer = self.create_timer(1.0, self._do_startup_check)
        self.create_timer(TELEMETRY_INTERVAL,          self._collect_telemetry)
        self.create_timer(1.0,                         self._publish_topic_rates)
        # Not created here — its period is a constructor arg to create_timer,
        # so like TF Tree/ros2_control above it's deferred to
        # _apply_agent_config, which resolves config override vs. this local
        # default and creates the timer at most once per resolved interval.
        self._tf_tree_poll_interval_default = self.get_parameter('tf_tree_poll_interval').get_parameter_value().double_value
        self._tf_tree_poll_timer = None
        self._tf_tree_poll_interval = None

        # One-time safety net: if the gateway never delivers agent_config (no
        # network, gateway down, whatever) within a few seconds of startup,
        # resolve every toggle to its local param default rather than leaving
        # things like TF tree stuck at None forever. Not needed on later
        # reconnects — once a real config has arrived once, this is a no-op.
        # Only relevant when actually waiting on the cloud in the first
        # place — a yaml params file means there's nothing to wait for (see
        # the immediate _apply_agent_config({}) call at the end of __init__),
        # so no fallback timer is created in that case either.
        self._agent_config_received = False
        self._agent_config_fallback_timer = None if self._param_overrides else self.create_timer(5.0, self._apply_agent_config_fallback)

        # ── Battery state subscription ────────────────────────────────────────
        # Not subscribed here — the topic name is a constructor arg to
        # create_subscription, so like the timers above it's deferred to
        # _apply_agent_config, which resolves config override vs. this local
        # default and (re)subscribes at most once per resolved topic.
        self._battery_topic_default = self.get_parameter('battery_topic').get_parameter_value().string_value
        self._battery_sub = None
        self._battery_topic = None
        self._battery_check_enabled_default = self.get_parameter('battery_check_enabled').get_parameter_value().bool_value
        self._battery_check_enabled = self._battery_check_enabled_default
        self._battery_min_percent_default = self.get_parameter('battery_min_percent').get_parameter_value().double_value
        self._battery_min_percent = self._battery_min_percent_default

        # ── Dock status subscription ────────────────────────────────────────
        # Same deferred-construction reasoning as battery above — topic name
        # is a constructor arg, resolved once by _apply_agent_config.
        self._dock_check_enabled_default = self.get_parameter('dock_check_enabled').get_parameter_value().bool_value
        self._dock_check_enabled = self._dock_check_enabled_default
        self._dock_status_topic_default = self.get_parameter('dock_status_topic').get_parameter_value().string_value
        self._dock_status_sub = None
        self._dock_status_topic = None
        self._last_dock_status: dict | None = None
        self._last_dock_status_time: float | None = None

        # ── WebSocket thread ──────────────────────────────────────────────────
        threading.Thread(target=self._run_ws_client, daemon=True).start()

        # ── Optional BT collectors ────────────────────────────────────────────
        # Neither constructed here — resolved once by _apply_agent_config
        # (config override, else these local defaults), same deferred
        # reasoning as TF tree/goals/params. self._bt_mode = None means "not
        # yet resolved"; self._nav2_bt_monitor_initialized replaces the old
        # hasattr(self, '_nav2_bt_tree_id')-as-a-proxy checks throughout the
        # file, now that construction is no longer guaranteed to have
        # happened by the time any of that code runs.
        self._bt_mode_default = self.get_parameter('bt_mode').get_parameter_value().string_value
        self._bt_host_default = self.get_parameter('bt_host').get_parameter_value().string_value
        self._bt_server_port_default = self.get_parameter('bt_server_port').get_parameter_value().integer_value
        self._bt_publisher_port_default = self.get_parameter('bt_publisher_port').get_parameter_value().integer_value
        self._bt_mode = None
        self._bt_collector = None
        self._bt_collector_conn = None  # (host, server_port, publisher_port) BTCollector is currently connected with
        self._nav2_bt_monitor_initialized = False

        # A yaml params file means the operator already fully specified how
        # this run should behave — there's nothing to wait on the cloud for,
        # so resolve and construct everything right now instead of leaving
        # every feature at None until a WS connection (or the 5s fallback
        # timer, not even created in this case) gets around to it. Passing
        # {} as config makes _resolve_config_value fall through to each
        # field's local_default unconditionally, same as it always would
        # once self._param_overrides is non-empty.
        if self._param_overrides:
            self._apply_agent_config({})

        _watcher_status = (
            f'pid={watcher_proc.pid}' if watcher_proc is not None else 'not started'
        )
        self.get_logger().info(
            f"🚀 Osiris agent v{AGENT_VERSION} — event based graph monitoring "
            f"(graph_watcher {_watcher_status})"
        )

    # ──────────────────────────────────────────────
    # WebSocket client
    # ──────────────────────────────────────────────

    def _run_ws_client(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._send_queue = asyncio.Queue()
        self.loop.run_until_complete(self._client_loop_with_reconnect())

    async def _client_loop_with_reconnect(self):
        delay = RECONNECT_INITIAL_DELAY
        while self.context.ok():
            try:
                await self._client_loop()
            except Exception as e:
                if self.context.ok():
                    self.get_logger().warning(
                        f"WebSocket error: {e}; retrying in {delay:.1f}s"
                    )
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX_DELAY) + random.uniform(0, 1)

    async def _client_loop(self):
        send_task = None
        self.get_logger().info('Connecting to gateway...')
        try:
            async with websockets.connect(self.ws_url) as ws:
                try:
                    auth_msg = await ws.recv()
                    auth_data = json.loads(auth_msg)
                except Exception:
                    self.get_logger().error('Failed to receive auth response from gateway')
                    return

                if not auth_data or auth_data.get('type') != 'auth_success':
                    error_msg = auth_data.get('message', 'unknown') if auth_data else 'no response'
                    self.get_logger().error(f'Authentication failed: {error_msg}')
                    return

                self.get_logger().info('Connected and authenticated to gateway')
                self.ws = ws
                send_task = asyncio.create_task(self._send_loop(ws))

                await self._send_initial_state()
                await self._receive_loop(ws)
        finally:
            if send_task and not send_task.done():
                send_task.cancel()
                try:
                    await send_task
                except (asyncio.CancelledError, Exception):
                    pass
            if self.ws is not None:
                if self.ws.close_code == 4429:
                    self.get_logger().warning('Storage limit reached')
                else:
                    self.get_logger().warning('Disconnected from gateway')
            self.ws = None

    async def _send_loop(self, ws):
        while True:
            msg = await self._send_queue.get()
            try:
                await ws.send(msg)
            except Exception as e:
                self.get_logger().error(f"WS send failed: {e}")
                raise

    async def _receive_loop(self, ws):
        async for raw in ws:
            if not self.context.ok():
                break
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            msg_type = data.get('type')
            if msg_type == 'agent_config':
                # _apply_agent_config does an int()/float() conversion per
                # field with nothing catching a malformed one (a corrupted
                # Firestore doc, say) - left uncaught, that exception would
                # propagate out of this whole loop and be treated as a
                # WebSocket error by _client_loop_with_reconnect, tearing
                # down and reconnecting the entire connection over what
                # should be a one-field problem. Caught here instead: this
                # one push is dropped (the agent keeps whatever config it
                # already had), the connection and everything else on it
                # keeps running.
                try:
                    self._apply_agent_config(data.get('config') or {})
                except Exception as e:
                    self.get_logger().error(f'Failed to apply agent_config: {e}')
            elif msg_type == 'subscribe':
                topic = data.get('topic')
                if topic:
                    self._subscribe_to_topic(topic)
            elif msg_type == 'unsubscribe':
                topic = data.get('topic')
                if topic:
                    self._unsubscribe_from_topic(topic)
            elif msg_type == 'lidar_snapshot_request':
                self._handle_lidar_snapshot_request(data)
            elif msg_type == 'topic_live_snapshot_request':
                self._handle_topic_live_snapshot_request(data)
            elif msg_type == 'bundle_capture_request':
                self._handle_bundle_capture_request(data)
            elif msg_type == 'send_command_request':
                self._handle_send_command_request(data)
            elif msg_type == 'cancel_command_request':
                self._handle_cancel_command_request(data)
            elif msg_type == 'error':
                self.get_logger().warning(f"Gateway error: {data.get('message', '')}")
            elif msg_type == 'bag_start_record':
                topics = data.get('topics', [])
                self.get_logger().info(
                    f"[bag] bag_start_record received  topics={topics or 'all'}"
                )
                self._start_bag_recording(data)
            elif msg_type == 'bag_stop_record':
                self.get_logger().info("[bag] bag_stop_record received")
                await asyncio.to_thread(self._stop_bag_recording)
            elif msg_type == 'bag_download_request':
                path       = data.get('path', '')
                request_id = data.get('request_id', '')
                upload_url = data.get('upload_url', '')
                self.get_logger().info(
                    f"[bag] bag_download_request received  path={path}  request_id={request_id}"
                )
                asyncio.ensure_future(
                    asyncio.to_thread(self._send_bag_download, path, request_id, upload_url)
                )

    async def _send_initial_state(self):
        # Wait for the first _check_graph_changes tick to populate all caches.
        await asyncio.to_thread(self._initial_scan_complete.wait, 15.0)

        # Reset delta caches so _flush_graph_snapshots treats everything as
        # "unsent" after this reconnect.
        self._last_sent_nodes    = None
        self._last_sent_topics   = None
        self._last_sent_actions  = None
        self._last_sent_services = None
        self._graph_dirty        = True

        nodes, topics, actions, services = await asyncio.to_thread(self._get_graph_snapshot_locked)
        controllers = self._ros2_control.get_controllers_snapshot() if self._ros2_control is not None else []
        hardware    = self._ros2_control.get_hardware_snapshot() if self._ros2_control is not None else []
        telemetry   = self._get_telemetry_snapshot()
        tf_tree     = self._tf_tree.get_snapshot() if self._tf_tree is not None else None
        bt_state    = self._build_startup_bt_state_event()
        bt          = self._bt_snapshot_from_state_event(bt_state)
        initial_timestamp = time.time()

        self._last_sent_nodes    = nodes.copy()
        self._last_sent_topics   = topics.copy()
        self._last_sent_actions  = actions.copy()
        self._last_sent_services = services.copy()

        await self._send_queue.put(json.dumps({
            'type': 'agent_version',
            'version': AGENT_VERSION,
        }))

        await self._send_queue.put(json.dumps({
            'type': 'initial_state',
            'timestamp': initial_timestamp,
            'data': {
                'timestamp': initial_timestamp,
                'graph': {
                    'nodes':       nodes,
                    'topics':      topics,
                    'actions':     actions,
                    'services':    services,
                    'controllers': controllers,
                    'hardware':    hardware,
                },
                'meta':      self._get_initial_state_meta(telemetry),
                'telemetry': telemetry,
                'tf_tree':   tf_tree,
                'bt':        bt,
            },
        }))

        await self._send_queue.put(json.dumps(bt_state))

        if self._cached_bt_tree_event:
            await self._send_queue.put(json.dumps(self._cached_bt_tree_event))
            self._cached_bt_tree_event = None

        await self._send_bridge_subscriptions()
        await self._send_bag_files()

        self.get_logger().info(
            f"Sent initial_state: {len(nodes)} nodes, {len(topics)} topics, "
            f"{len(actions)} actions, {len(services)} services"
        )

    async def _send_bridge_subscriptions(self):
        with self._topic_subs_lock:
            subs = list(self._topic_subs.keys())
        await self._send_queue.put(json.dumps({
            'type': 'bridge_subscriptions',
            'subscriptions': subs,
            'timestamp': time.time(),
        }))

    async def _send_subscribe_failed(self, topic_name: str, reason: str):
        await self._send_queue.put(json.dumps({
            'type': 'subscribe_failed',
            'topic': topic_name,
            'reason': reason,
            'timestamp': time.time(),
        }))

    # ──────────────────────────────────────────────
    # Tier-1: cheap existence detection
    # ──────────────────────────────────────────────

    def _check_graph_changes(self):
        if not self._graph_check_lock.acquire(blocking=False):
            # A check is already running (e.g. slow introspection during
            # heavy graph churn). Whatever triggered this call must not be
            # silently dropped — flag it so the in-flight run loops back
            # around and re-checks before releasing the lock. This keeps
            # the pipeline purely event-driven (no periodic polling) while
            # guaranteeing every trigger is eventually acted on.
            self._graph_check_pending = True
            return
        try:
            self._check_graph_changes_locked()
            while self._graph_check_pending:
                self._graph_check_pending = False
                self._check_graph_changes_locked()
        finally:
            self._graph_check_lock.release()

    def _check_graph_changes_locked(self):
        # ── 1. Node + topic queries (always, both cheap) ──────────────────────
        _t0 = time.time()
        node_pairs      = list(self.get_node_names_and_namespaces())
        topic_type_list = self.get_topic_names_and_types()
        _t1 = time.time()

        # Build a flat map for O(1) type lookup throughout this method
        topic_type_map = {t: types for t, types in topic_type_list}

        current_nodes   = {self._node_full_name(n, ns) for n, ns in node_pairs}
        current_topics  = {t for t, _ in topic_type_list}
        current_actions = {
            t.replace('/_action/status', '')
            for t in current_topics
            if t.endswith('/_action/status')
        }
        self.get_logger().info(
            f"[poll] node+topic: {(_t1-_t0)*1000:.1f}ms "
            f"({len(current_nodes)} nodes, {len(current_topics)} topics, {len(current_actions)} actions)"
        )

        # ── 1b. Service scan ─── on node changes and follow-up ticks only ────────
        _nodes_stopped  = self._first_graph_check_done and bool(self._active_nodes - current_nodes)
        _nodes_started  = self._first_graph_check_done and bool(current_nodes - self._active_nodes)
        _do_service_scan = (
            not self._first_graph_check_done
            or _nodes_stopped
            or _nodes_started
            or self._service_rescan_ticks > 0
        )
        if _do_service_scan:
            if _nodes_stopped:
                # Schedule follow-up scans to catch DDS endpoint lag.
                self._service_rescan_ticks = 4
            elif self._service_rescan_ticks > 0:
                self._service_rescan_ticks -= 1
            _ts0 = time.time()
            service_type_list = self.get_service_names_and_types()
            _ts1 = time.time()
            current_services = {
                s: types[0] if types else 'unknown'
                for s, types in service_type_list
                if not any(s.startswith(p) for p in _SUPPRESSED_SERVICE_PREFIXES)
            }
            self.get_logger().info(
                f"[poll] service_scan: {(_ts1-_ts0)*1000:.1f}ms ({len(current_services)} services)"
            )
        else:
            current_services = self._active_services

        # ── FIRST TICK: silently populate caches, no events ───────────────────
        if not self._first_graph_check_done:
            self._first_graph_check_done = True
            self._active_nodes    = current_nodes
            self._active_topics   = current_topics
            self._active_services = current_services
            self._active_actions  = current_actions
            _te0 = time.time()
            self._do_full_initial_enrichment(topic_type_list, node_pairs)
            _te1 = time.time()
            # Stagger parameter fetches: fire them on the ROS executor thread
            # 5 s after the first tick so we don't hammer lifecycle nodes that
            # are still in the middle of configuring/activating.
            def _fetch_all_params_delayed():
                for fqn in list(current_nodes):
                    if self._params_enabled:
                        self._fetch_node_parameters_async(fqn)
                    self._fetch_lifecycle_state_async(fqn)
            self._param_fetch_timer = self.create_timer(5.0, lambda: (self._cancel_param_fetch_timer(), _fetch_all_params_delayed()))
            for _t in current_topics:
                if _t.endswith('/transition_event'):
                    self._subscribe_lifecycle_topic(_t)
                    self._fetch_lifecycle_state_async(_t[:-len('/transition_event')])
            # Resolve action types for all actions at startup — Graph's action
            # listing needs this regardless of goals_enabled. Status
            # subscription (actual goal tracking) is gated separately; see
            # self._goals_enabled.
            for a in current_actions:
                self._fetch_action_types(a, topic_type_map)
                if self._goals_enabled:
                    self._subscribe_action_status(a)
            if self._ros2_control is not None:
                self._ros2_control.poll(force=True)
            if self._tf_tree is not None:
                self._tf_tree.poll(force=True)
            self._initial_scan_complete.set()
            self.get_logger().info(
                f"[poll] first tick complete: {len(current_nodes)} nodes, {len(current_topics)} topics, "
                f"{len(current_services)} services, {len(current_actions)} actions — "
                f"node+topic={(_t1-_t0)*1000:.1f}ms enrichment={(_te1-_te0)*1000:.1f}ms"
            )
            return

        # ── 2. Node events ────────────────────────────────────────────────────
        started_nodes = current_nodes - self._active_nodes
        if started_nodes:
            self.get_logger().info(f"[poll] {len(started_nodes)} node(s) started: {sorted(started_nodes)}")
            self._pending_topic_enrichment.update(self._active_topics)
            self._graph_dirty = True
        for fqn in started_nodes:
            self._nodes_no_param_service.discard(fqn)  # allow retry after restart
            if self._params_enabled:
                self._fetch_node_parameters_async(fqn)
            self._fetch_lifecycle_state_async(fqn)

        stopped_nodes = self._active_nodes - current_nodes
        if stopped_nodes:
            self.get_logger().info(f"[poll] {len(stopped_nodes)} node(s) stopped: {sorted(stopped_nodes)}")
            self._graph_dirty = True
        for fqn in stopped_nodes:
            for topic, rel in self._topic_relations.items():
                if fqn in rel.get('publishers', set()) or fqn in rel.get('subscribers', set()):
                    self._pending_topic_enrichment.add(topic)
            self._node_parameter_cache.pop(fqn, None)
            self._pending_param_fetches.discard(fqn)
            self._lifecycle_state_cache.pop(fqn, None)
            self._pending_lifecycle_fetches.discard(fqn)

        # ── 3. Topic events ───────────────────────────────────────────────────
        for t in current_topics - self._active_topics:
            self._pending_topic_enrichment.add(t)
            self._graph_dirty = True
            if t.endswith('/transition_event'):
                self._subscribe_lifecycle_topic(t)
            # Nav2 BT edge-case: /behavior_tree_log just appeared
            if t == '/behavior_tree_log' and self._nav2_bt_monitor_initialized:
                if self.count_publishers(t) > 0:
                    self._nav2_bt_publisher_active = True
                    if self._load_and_parse_bt_xml():
                        self._on_bt_event({
                            'type': 'bt_tree', 'timestamp': time.time(),
                            'tree_id': self._nav2_bt_tree_id,
                            'tree': self._nav2_bt_tree_structure,
                            'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                        })

        for t in self._active_topics - current_topics:
            self._topic_relations.pop(t, None)
            self._topic_counts.pop(t, None)
            self._pending_topic_enrichment.discard(t)
            self._graph_dirty = True
            if t.endswith('/transition_event'):
                lc_sub = self._lifecycle_subs.pop(t, None)
                if lc_sub:
                    self.destroy_subscription(lc_sub)
            if t == '/behavior_tree_log' and self._nav2_bt_monitor_initialized:
                self._on_nav2_bt_gone()

        # ── 4. Service changes ─────────────────────────────────────────────────────────────
        if _do_service_scan:
            if set(current_services) != set(self._active_services):
                self._graph_dirty = True

        # ── 5. Action events ──────────────────────────────────────────────────
        for a in current_actions - self._active_actions:
            self._fetch_action_types(a, topic_type_map)
            if self._goals_enabled:
                self._subscribe_action_status(a)
            self._graph_dirty = True

        for a in self._active_actions - current_actions:
            self._action_type_cache.pop(a, None)
            self._unsubscribe_action_monitoring(a)
            self._graph_dirty = True

        # ── 6. Update existence caches ────────────────────────────────────────
        self._active_nodes    = current_nodes
        self._active_topics   = current_topics
        if _do_service_scan:
            self._active_services = current_services
        self._active_actions  = current_actions

        # ── 7. Re-enrich only topics whose pub/sub count changed ─────────────
        if self._pending_topic_enrichment:
            self._enrich_pending_relations(topic_type_list)

        # ── 8. Retry action type resolution for any unresolved actions ────────
        for a in current_actions:
            if a not in self._action_type_cache:
                self._fetch_action_types(a, topic_type_map)

        # ── 9. Nav2 BT publisher liveness check ──────────────────────────────
        if self._nav2_bt_monitor_initialized:
            bt_rel = self._topic_relations.get('/behavior_tree_log', {})
            bt_pubs = bt_rel.get('publishers', set()) & current_nodes
            if self._nav2_bt_publisher_active and not bt_pubs:
                self._on_nav2_bt_gone()
            elif self._nav2_bt_publisher_active and bt_pubs and self._nav2_bt_tree_id is None:
                if self._load_and_parse_bt_xml():
                    self._on_bt_event({
                        'type': 'bt_tree', 'timestamp': time.time(),
                        'tree_id': self._nav2_bt_tree_id,
                        'tree': self._nav2_bt_tree_structure,
                        'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                    })
            elif not self._nav2_bt_publisher_active and bt_pubs:
                self._nav2_bt_publisher_active = True
                if self._load_and_parse_bt_xml():
                    self._on_bt_event({
                        'type': 'bt_tree', 'timestamp': time.time(),
                        'tree_id': self._nav2_bt_tree_id,
                        'tree': self._nav2_bt_tree_structure,
                        'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                    })

        # ── 10. Flush graph snapshots if anything changed ─────────────────────
        self._flush_graph_snapshots()

        # ── 11. Poll collectors ───────────────────────────────────────────────
        if self._ros2_control is not None:
            self._ros2_control.poll()
        if self._tf_tree is not None:
            self._tf_tree.poll()

    # ──────────────────────────────────────────────
    # Initial full enrichment (called once on first tick)
    # ──────────────────────────────────────────────

    def _do_full_initial_enrichment(self, topic_type_list, node_pairs):
        topic_type_map = dict(topic_type_list)
        self._pending_topic_enrichment.clear()
        for topic in self._active_topics:
            try:
                pub_infos = self.get_publishers_info_by_topic(topic)
                sub_infos = self.get_subscriptions_info_by_topic(topic)
            except Exception:
                continue
            publishers  = {self._node_full_name(p.node_name, p.node_namespace) for p in pub_infos}
            subscribers = {self._node_full_name(s.node_name, s.node_namespace) for s in sub_infos}
            self._topic_relations[topic] = {
                'publishers':       publishers,
                'subscribers':      subscribers,
                'publisher_infos':  pub_infos,
                'subscriber_infos': sub_infos,
                'type': topic_type_map.get(topic, ['unknown'])[0],
            }
            self._topic_counts[topic] = (len(pub_infos), len(sub_infos))

    # ──────────────────────────────────────────────
    # Tier-2: batched relation enrichment (inert with R1 gate)
    # ──────────────────────────────────────────────

    def _enrich_pending_relations(self, topic_type_list=None):
        if not self._pending_topic_enrichment:
            return

        batch = set(self._pending_topic_enrichment)
        self._pending_topic_enrichment.clear()
        _t0 = time.time()
        self.get_logger().info(f"[enrich] {len(batch)} topics")

        if topic_type_list is not None:
            topic_type_map = dict(topic_type_list)
        else:
            topic_type_map = dict(self.get_topic_names_and_types())

        for topic in batch:
            if topic not in self._active_topics:
                continue
            try:
                pub_infos = self.get_publishers_info_by_topic(topic)
                sub_infos = self.get_subscriptions_info_by_topic(topic)
            except Exception as e:
                self.get_logger().debug(f"Enrichment failed for {topic}: {e}")
                continue

            publishers  = {self._node_full_name(p.node_name, p.node_namespace) for p in pub_infos}
            subscribers = {self._node_full_name(s.node_name, s.node_namespace) for s in sub_infos}
            old = self._topic_relations.get(topic)
            new_rel = {
                'publishers':       publishers,
                'subscribers':      subscribers,
                'publisher_infos':  pub_infos,
                'subscriber_infos': sub_infos,
                'type': topic_type_map.get(topic, ['unknown'])[0],
            }
            self._topic_relations[topic] = new_rel
            self._topic_counts[topic] = (len(pub_infos), len(sub_infos))

            if old is not None:
                if subscribers != old['subscribers']:
                    self._graph_dirty = True

                # Nav2 BT: publisher appeared/vanished on /behavior_tree_log
                if topic == '/behavior_tree_log' and self._nav2_bt_monitor_initialized:
                    old_pubs = old['publishers']
                    if publishers and not old_pubs:
                        self._nav2_bt_publisher_active = True
                        if self._load_and_parse_bt_xml():
                            self._on_bt_event({
                                'type': 'bt_tree', 'timestamp': time.time(),
                                'tree_id': self._nav2_bt_tree_id,
                                'tree': self._nav2_bt_tree_structure,
                                'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                            })
                    elif old_pubs and not publishers:
                        self._on_nav2_bt_gone()

        self.get_logger().info(f"[enrich] done in {(time.time()-_t0)*1000:.1f}ms")

    # ──────────────────────────────────────────────
    # Graph snapshot builders
    # ──────────────────────────────────────────────

    def _get_nodes_with_relations(self) -> dict:
        result = {}
        for fqn in self._active_nodes:
            result[fqn] = {
                'publishes':  [],
                'subscribes': [],
                'actions':    [],
                'services':   [],
                'parameters': self._node_parameter_cache.get(fqn, {}),
                'lifecycle_state': self._lifecycle_state_cache.get(fqn, None),
            }

        for topic, rel in self._topic_relations.items():
            pub_infos = rel.get('publisher_infos', [])
            sub_infos = rel.get('subscriber_infos', [])
            for p in pub_infos:
                fqn = self._node_full_name(p.node_name, p.node_namespace)
                if fqn in result:
                    result[fqn]['publishes'].append({
                        'topic': topic,
                        'qos': self._qos_to_dict(p.qos_profile),
                    })
            for s in sub_infos:
                fqn = self._node_full_name(s.node_name, s.node_namespace)
                if fqn in result:
                    result[fqn]['subscribes'].append({
                        'topic': topic,
                        'qos': self._qos_to_dict(s.qos_profile),
                    })

        for topic, rel in self._topic_relations.items():
            if topic.endswith('/_action/status') and rel['publishers']:
                action = topic.replace('/_action/status', '')
                for p in rel['publisher_infos']:
                    fqn = self._node_full_name(p.node_name, p.node_namespace)
                    if fqn in result and action not in result[fqn]['actions']:
                        result[fqn]['actions'].append(action)

        return result

    def _get_topics_with_relations(self) -> dict:
        result = {}
        for topic in self._active_topics:
            rel = self._topic_relations.get(topic, {})
            result[topic] = {
                'type': rel.get('type', 'unknown'),
                'publishers': [
                    {
                        'node': self._node_full_name(p.node_name, p.node_namespace),
                        'qos': self._qos_to_dict(p.qos_profile),
                    }
                    for p in rel.get('publisher_infos', [])
                ],
                'subscribers': [
                    {
                        'node': self._node_full_name(s.node_name, s.node_namespace),
                        'qos': self._qos_to_dict(s.qos_profile),
                    }
                    for s in rel.get('subscriber_infos', [])
                ],
            }
        return result

    def _get_actions_with_relations(self) -> dict:
        result = {}
        for action in self._active_actions:
            rel = self._topic_relations.get(f'{action}/_action/status', {})
            providers = [
                self._node_full_name(p.node_name, p.node_namespace)
                for p in rel.get('publisher_infos', [])
            ]
            result[action] = {
                'providers': providers,
                **(self._action_type_cache.get(action) or {}),
            }
        return result

    def _get_services_with_relations(self) -> dict:
        return {
            name: {'type': type_str, 'providers': []}
            for name, type_str in self._active_services.items()
        }

    def _get_graph_snapshot_locked(self) -> tuple[dict, dict, dict, dict]:
        """Gather all four graph relation dicts under _graph_check_lock.

        _check_graph_changes_locked() mutates _topic_relations / _active_*
        in place on the timer/executor thread; without this lock a caller
        on another thread (e.g. _send_initial_state on the websocket thread)
        can observe a torn dict mid-mutation (RuntimeError: dictionary
        changed size during iteration, or a silently incomplete snapshot).
        """
        with self._graph_check_lock:
            return (
                self._get_nodes_with_relations(),
                self._get_topics_with_relations(),
                self._get_actions_with_relations(),
                self._get_services_with_relations(),
            )

    # ──────────────────────────────────────────────
    # Delta-send: flush graph snapshots after each tick
    # ──────────────────────────────────────────────

    def _flush_graph_snapshots(self):
        if not self._graph_dirty or not self.ws or not self.loop:
            return

        # Only clear the dirty flag once every snapshot has actually been
        # built and enqueued. If anything below raises, the flag is put
        # back so the next trigger retries instead of the client silently
        # never receiving this update.
        try:
            nodes = self._get_nodes_with_relations()
            if nodes != self._last_sent_nodes:
                self.get_logger().info(f"[flush] nodes ({len(nodes)} nodes)")
                self._last_sent_nodes = nodes.copy()
                self._enqueue({'type': 'nodes', 'data': nodes, 'timestamp': time.time()})

            topics = self._get_topics_with_relations()
            if topics != self._last_sent_topics:
                self.get_logger().info(f"[flush] topics ({len(topics)} topics)")
                self._last_sent_topics = topics.copy()
                self._enqueue({'type': 'topics', 'data': topics, 'timestamp': time.time()})

            actions = self._get_actions_with_relations()
            if actions != self._last_sent_actions:
                self.get_logger().info(f"[flush] actions ({len(actions)} actions)")
                self._last_sent_actions = actions.copy()
                self._enqueue({'type': 'actions', 'data': actions, 'timestamp': time.time()})

            services = self._get_services_with_relations()
            if services != self._last_sent_services:
                self.get_logger().info(f"[flush] services ({len(services)} services)")
                self._last_sent_services = services.copy()
                self._enqueue({'type': 'services', 'data': services, 'timestamp': time.time()})
        except Exception:
            self._graph_dirty = True
            raise
        else:
            self._graph_dirty = False

    # ──────────────────────────────────────────────
    # Topic subscriptions (gateway-requested)
    # ──────────────────────────────────────────────

    def _subscribe_to_topic(self, topic_name: str):
        if not topic_name or not isinstance(topic_name, str):
            return
        with self._topic_subs_lock:
            if topic_name in self._topic_subs:
                return
            if len(self._topic_subs) >= MAX_SUBSCRIPTIONS:
                self.get_logger().error(
                    f"Subscription limit ({MAX_SUBSCRIPTIONS}) reached; "
                    f"cannot subscribe to {topic_name}"
                )
                if self.loop:
                    asyncio.run_coroutine_threadsafe(
                        self._send_subscribe_failed(topic_name, 'subscription_limit_reached'), self.loop
                    )
                return

        types = dict(self.get_topic_names_and_types()).get(topic_name)
        if not types:
            self.get_logger().warning(f"Topic not found: {topic_name}")
            if self.loop:
                asyncio.run_coroutine_threadsafe(
                    self._send_subscribe_failed(topic_name, 'topic_not_found'), self.loop
                )
            return

        msg_class = get_message(types[0])
        sub = self.create_subscription(
            msg_class,
            topic_name,
            lambda msg, t=topic_name: self._on_topic_msg(msg, t),
            QoSProfile(depth=10),
        )
        with self._topic_subs_lock:
            self._topic_subs[topic_name] = sub
            self._topic_msg_types[topic_name] = types[0]

        self.get_logger().info(f"Subscribed to {topic_name}")
        if self.loop:
            asyncio.run_coroutine_threadsafe(
                self._send_bridge_subscriptions(), self.loop
            )

    def _unsubscribe_from_topic(self, topic_name: str):
        with self._topic_subs_lock:
            sub = self._topic_subs.pop(topic_name, None)
            self._topic_msg_types.pop(topic_name, None)
        if sub:
            self.destroy_subscription(sub)
            self._topic_data_throttle.pop(topic_name, None)
            self.get_logger().info(f"Unsubscribed from {topic_name}")
            if self.loop:
                asyncio.run_coroutine_threadsafe(
                    self._send_bridge_subscriptions(), self.loop
                )

    # ──────────────────────────────────────────────
    # LiDAR one-shot snapshot (wall line-fitting ground truth)
    # ──────────────────────────────────────────────

    def _handle_lidar_snapshot_request(self, data: dict):
        """Fire-once LaserScan capture: subscribe, wait for exactly one
        message, unsubscribe, relay it back. Deliberately separate from
        _subscribe_to_topic/_topic_subs (the continuous gateway-requested
        subscription path) — /scan only needs a point-in-time read for wall
        line-fitting, not continuous logging, and keeping it out of
        _topic_subs means it never counts against MAX_SUBSCRIPTIONS or shows
        up in bridge_subscriptions. See project_lidar_heading_plan_executor
        memory for why.
        """
        topic_name = data.get('topic') or '/scan'
        request_id = data.get('request_id', '')
        timeout_s = 3.0

        types = dict(self.get_topic_names_and_types()).get(topic_name)
        if not types:
            self.get_logger().warning(f"[lidar_snapshot] topic not found: {topic_name}")
            self._send_lidar_snapshot_failed(request_id, 'topic_not_found')
            return
        if 'sensor_msgs/msg/LaserScan' not in types:
            self.get_logger().warning(f"[lidar_snapshot] {topic_name} is not a LaserScan ({types})")
            self._send_lidar_snapshot_failed(request_id, 'not_a_laserscan')
            return

        msg_class = get_message(types[0])
        lock = threading.Lock()
        state = {'done': False}
        sub_holder = {}

        # Best-effort live TF lookup, started alongside the /scan
        # subscription so /tf_static (latched, arrives near-instantly to a
        # fresh subscriber) has the whole scan-capture window to show up
        # before on_scan actually needs it. See _start_lidar_tf_listener for
        # why this is its own Buffer/Listener rather than self._tf_tree.
        tf_buffer, tf_holder = self._start_lidar_tf_listener()

        # Guards against both the scan callback and the timeout firing —
        # whichever gets here first wins, the other is a no-op. Both can run
        # on different threads (rclpy executor thread vs. the timer's own
        # thread), so this needs the lock, not just a plain flag check.
        def finish(success, msg=None, reason=None, front_angle_rad=None):
            with lock:
                if state['done']:
                    return
                state['done'] = True
            sub = sub_holder.pop('sub', None)
            if sub is not None:
                # Same destroy_subscription call _unsubscribe_from_topic already
                # makes elsewhere in this file — just invoked from inside the
                # subscription's own callback here instead of a separate
                # gateway-driven unsubscribe. Worth double-checking on real
                # hardware if this ever hangs instead of returning cleanly.
                self.destroy_subscription(sub)
            timer = sub_holder.pop('timer', None)
            if timer is not None:
                timer.cancel()
            # Same "drop the reference, destructor unregisters the
            # subscriptions" cleanup TfTreeCollector.destroy() itself uses.
            tf_holder.pop('listener', None)
            if success:
                self._send_lidar_snapshot_result(request_id, topic_name, msg, front_angle_rad)
            else:
                self._send_lidar_snapshot_failed(request_id, reason)

        def on_scan(msg):
            front_angle_rad = self._lookup_lidar_front_angle(tf_buffer, msg.header.frame_id)
            finish(True, msg=msg, front_angle_rad=front_angle_rad)

        sub_holder['sub'] = self.create_subscription(msg_class, topic_name, on_scan, QoSProfile(depth=1))

        timer = threading.Timer(timeout_s, lambda: finish(False, reason='timeout'))
        timer.daemon = True
        sub_holder['timer'] = timer
        timer.start()

    @staticmethod
    def _wrap_to_pi(angle: float) -> float:
        while angle > math.pi:
            angle -= 2 * math.pi
        while angle <= -math.pi:
            angle += 2 * math.pi
        return angle

    def _lookup_lidar_front_angle(self, tf_buffer, scan_frame_id: str):
        """Best-effort: the robot's true front, expressed as an angle in the
        LiDAR's own raw scan coordinates. Angle 0 in a LaserScan always
        points along the sensor frame's own local +X axis — if that frame
        is itself mounted rotated by some yaw relative to base_link (a real
        situation on custom builds, not just a theoretical one — see
        project_lidar_front_angle memory), true front ends up at -yaw in
        the scan's own numbering, not at 0.

        Returns None on any failure (no listener, frame not published yet,
        base_link doesn't exist) — this only ever reports what TF actually
        said, never a fallback guess. The caller/gateway decides what to do
        with "unknown".
        """
        if tf_buffer is None or not scan_frame_id:
            return None
        try:
            from rclpy.time import Time as RclpyTime
            # No timeout arg: this is a non-blocking cache read against
            # whatever /tf_static has already delivered, not a spin-and-wait
            # call — safe to call from inside a message callback.
            t = tf_buffer.lookup_transform('base_link', scan_frame_id, RclpyTime())
        except Exception:
            return None
        q = t.transform.rotation
        yaw = math.atan2(2 * (q.w * q.z + q.x * q.y), 1 - 2 * (q.y * q.y + q.z * q.z))
        return self._wrap_to_pi(-yaw)

    @staticmethod
    def _json_safe_floats(values):
        """Replaces inf/-inf/nan with None. json.dumps happily emits the bare
        Infinity/NaN tokens for these (Python-specific, not valid JSON), which
        makes the gateway's JSON.parse throw on the whole message — and
        LaserScan.ranges is exactly where this bites, since out-of-range
        readings are routinely inf, not just an edge case."""
        return [v if isinstance(v, (int, float)) and math.isfinite(v) else None for v in values]

    def _send_lidar_snapshot_result(self, request_id: str, topic_name: str, msg, front_angle_rad=None):
        if not self.loop:
            return
        data = message_to_ordereddict(msg)
        if 'ranges' in data:
            data['ranges'] = self._json_safe_floats(data['ranges'])
        if 'intensities' in data:
            data['intensities'] = self._json_safe_floats(data['intensities'])
        payload = {
            'type': 'lidar_snapshot_result',
            'request_id': request_id,
            'topic': topic_name,
            'data': data,
            'timestamp': time.time(),
        }
        # Only present when a live TF lookup actually succeeded — the
        # gateway falls back to its own cached/default value otherwise, so
        # this must be omitted rather than sent as 0 when unknown.
        if front_angle_rad is not None:
            payload['front_angle_rad'] = front_angle_rad
        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps(payload)),
            self.loop,
        )

    def _send_lidar_snapshot_failed(self, request_id: str, reason: str):
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps({
                'type': 'lidar_snapshot_failed',
                'request_id': request_id,
                'reason': reason,
                'timestamp': time.time(),
            })),
            self.loop,
        )

    @staticmethod
    def _json_safe_deep(value):
        """Recursively replaces inf/-inf/nan with None throughout an
        arbitrary nested dict/list structure, as message_to_ordereddict
        produces for ANY message type — same reasoning as _json_safe_floats
        (LaserScan.ranges), generalized because a generic topic snapshot has
        no fixed field name to special-case; any float field on any message
        type could be non-finite."""
        if isinstance(value, float):
            return value if math.isfinite(value) else None
        if isinstance(value, dict):
            return {k: WebBridge._json_safe_deep(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [WebBridge._json_safe_deep(v) for v in value]
        return value

    def _start_one_shot_capture(self, topic_name, on_message, on_timeout, on_not_found, timeout_s=3.0):
        """Shared subscribe-wait-one-message-unsubscribe mechanics, factored
        out of _handle_topic_live_snapshot_request so _handle_bundle_capture_
        request can run several of these in parallel behind a shared
        completion barrier instead of duplicating the subscribe/timeout/
        cleanup dance per capture. Whichever of on_message/on_timeout fires
        is entirely this function's own business — the caller's callbacks
        just react, they don't need to track the subscription or timer
        themselves. on_not_found fires synchronously, before anything is
        subscribed, if the topic doesn't currently exist."""
        types = dict(self.get_topic_names_and_types()).get(topic_name)
        if not types:
            on_not_found()
            return

        msg_class = get_message(types[0])
        lock = threading.Lock()
        state = {'done': False}
        holder = {}

        # Same "whichever fires first wins" guard as the lidar snapshot's
        # own finish() — the message callback and the timeout can each fire
        # from a different thread.
        def finish(success, msg=None):
            with lock:
                if state['done']:
                    return
                state['done'] = True
            sub = holder.pop('sub', None)
            if sub is not None:
                self.destroy_subscription(sub)
            timer = holder.pop('timer', None)
            if timer is not None:
                timer.cancel()
            if success:
                on_message(msg)
            else:
                on_timeout()

        holder['sub'] = self.create_subscription(msg_class, topic_name, lambda msg: finish(True, msg), QoSProfile(depth=1))

        timer = threading.Timer(timeout_s, lambda: finish(False))
        timer.daemon = True
        holder['timer'] = timer
        timer.start()

    def _start_lidar_tf_listener(self):
        """Best-effort TF listener for a single scan capture, factored out of
        _handle_lidar_snapshot_request so _handle_bundle_capture_request's own
        lidar_wall_fit capture can reuse it too. Deliberately its own Buffer/
        Listener, not self._tf_tree — see project_lidar_front_angle memory for
        why. Returns (tf_buffer, holder); tf_buffer is None on any failure.
        Caller drops the reference (holder.pop('listener', None)) once done
        with it, same cleanup TfTreeCollector.destroy() itself uses."""
        tf_buffer = None
        tf_holder = {}
        try:
            import tf2_ros
            candidate_buffer = tf2_ros.Buffer()
            tf_holder['listener'] = tf2_ros.TransformListener(candidate_buffer, self, spin_thread=False)
            tf_buffer = candidate_buffer
        except Exception as e:
            tf_buffer = None
            tf_holder.pop('listener', None)
            self.get_logger().debug(f"[tf_listener] unavailable: {e}")
        return tf_buffer, tf_holder

    def _handle_topic_live_snapshot_request(self, data: dict):
        """Fire-once capture for ANY topic: subscribe, wait for exactly one
        message, unsubscribe, relay it back. Generalized from
        _handle_lidar_snapshot_request (no LaserScan type check, no TF
        front-angle lookup — those are wall-fit-specific) so the AI can read
        a topic's current value (e.g. /odom) without paying for a lasting
        subscription and continuous logging when it only needs one moment
        in time. Deliberately separate from _subscribe_to_topic/_topic_subs
        (the continuous gateway-requested subscription path) for the same
        reason the lidar one is: never counts against MAX_SUBSCRIPTIONS,
        never shows up in bridge_subscriptions, never persisted as a log
        row — this is a pure ephemeral request/response value, not meant to
        be a durable history."""
        topic_name = data.get('topic')
        request_id = data.get('request_id', '')

        if not topic_name:
            self._send_topic_live_snapshot_failed(request_id, 'topic_required')
            return

        self._start_one_shot_capture(
            topic_name,
            on_message=lambda msg: self._send_topic_live_snapshot_result(request_id, topic_name, msg),
            on_timeout=lambda: self._send_topic_live_snapshot_failed(request_id, 'timeout'),
            on_not_found=lambda: self._send_topic_live_snapshot_failed(request_id, 'topic_not_found'),
        )

    def _handle_bundle_capture_request(self, data: dict):
        """Captures any number of topics + optionally a lidar wall-fit's raw
        scan, all in parallel, joined into ONE combined result once every
        requested capture has resolved (success or failure each) — see
        project_lidar_heading_plan_executor memory (bundles): this is what
        lets the AI correlate values by exact-match run_id/moment instead of
        timestamp proximity, which is ambiguous once more than one run
        happens in the same session.

        Each capture reuses _start_one_shot_capture — this only coordinates
        them behind a shared pending set, it doesn't reimplement the
        subscribe/timeout/cleanup mechanics. The wall-fit computation itself
        does NOT happen here — this only captures the raw /scan message and,
        best-effort, its TF front-angle; the gateway resolves the actual
        heading/distance (see resolveWallFit), same division of
        responsibility as the dedicated lidar-snapshot path.
        """
        request_id = data.get('request_id', '')
        topics = data.get('topics') or []
        lidar_wall_fit = data.get('lidar_wall_fit')
        lidar_topic = (lidar_wall_fit or {}).get('topic') or '/scan'

        lock = threading.Lock()
        results = {}
        pending = set(topics)
        if lidar_wall_fit is not None:
            pending.add('lidar_wall_fit')

        if not pending:
            self._send_bundle_capture_result(request_id, {})
            return

        def resolve(name, success, value=None, reason=None):
            with lock:
                if name not in pending:
                    return
                pending.discard(name)
                results[name] = {'status': 'success', **(value or {})} if success else {'status': 'failed', 'reason': reason}
                still_pending = bool(pending)
            if not still_pending:
                self._send_bundle_capture_result(request_id, results)

        for t in topics:
            self._start_one_shot_capture(
                t,
                on_message=lambda msg, t=t: resolve(t, True, {'data': self._encode_bundle_capture_data(t, msg)}),
                on_timeout=lambda t=t: resolve(t, False, reason='timeout'),
                on_not_found=lambda t=t: resolve(t, False, reason='topic_not_found'),
            )

        if lidar_wall_fit is not None:
            tf_buffer, tf_holder = self._start_lidar_tf_listener()

            def _on_lidar_msg(msg):
                front_angle_rad = self._lookup_lidar_front_angle(tf_buffer, msg.header.frame_id)
                tf_holder.pop('listener', None)
                resolve('lidar_wall_fit', True, {
                    'raw_scan': self._json_safe_deep(message_to_ordereddict(msg)),
                    'front_angle_rad': front_angle_rad,
                })

            def _on_lidar_timeout():
                tf_holder.pop('listener', None)
                resolve('lidar_wall_fit', False, reason='timeout')

            def _on_lidar_not_found():
                tf_holder.pop('listener', None)
                resolve('lidar_wall_fit', False, reason='topic_not_found')

            self._start_one_shot_capture(lidar_topic, _on_lidar_msg, _on_lidar_timeout, _on_lidar_not_found)

    def _encode_bundle_capture_data(self, topic_name: str, msg) -> dict:
        """Converts one bundle-captured message to a JSON-safe dict, with an
        Image/CompressedImage frame embedded as an inline base64 string +
        __osiris_binary__ marker (see _capture_image_as_base64) instead of a
        giant array of decimal ints - the same problem
        _send_topic_live_snapshot_result was fixed for (see its own
        docstring), reached here by a different path since
        _handle_bundle_capture_request builds each capture's data inline
        rather than going through that function.

        Deliberately NOT a separate binary WS frame the way the continuous
        topic_data stream and the single-topic one-shot snapshot both do it
        - a bundle joins several topics into ONE combined message, and the
        existing protocol only pairs one pending binary frame per JSON
        header (see the gateway's ws._pendingBinaryTopicData), so it has no
        room for several at once without a real protocol extension. Base64
        inline costs ~1.37x over raw bytes, same as the gateway's own
        relay - irrelevant for an occasional labeled capture (max 10 topics,
        never a 10Hz stream), so this is the pragmatic fix rather than
        building multi-binary-frame support nothing else needs yet.

        Only Image/CompressedImage get this treatment. PointCloud2/
        OccupancyGrid are deliberately left alone: message_to_ordereddict
        already gives their byte field as a plain int list, which is the
        exact same shape a "fix" would produce anyway (unlike Image, there's
        no compression step to gain anything from) - see
        _capture_image_as_base64's own None return for these.
        """
        data = message_to_ordereddict(msg)
        types = dict(self.get_topic_names_and_types()).get(topic_name)
        msg_type = types[0] if types else None
        encoded = self._capture_image_as_base64(topic_name, data, msg_type)
        if encoded is not None:
            base64_str, marker = encoded
            data[BINARY_PAYLOAD_FIELD] = base64_str
            data[BINARY_MARKER_KEY] = marker
        return self._json_safe_deep(data)

    def _capture_image_as_base64(self, topic_name: str, data: dict, msg_type: str):
        """For an Image/CompressedImage message already run through
        message_to_ordereddict, returns (base64_str, marker) - marker
        shaped exactly like what a client already gets from the gateway's
        own binary reconstruction (format/width/height/len/encoding), so
        any existing consumer's `__osiris_binary__.encoding === 'base64'`
        check (Camera.vue's isRenderable, get_camera_snapshot's own
        renderability check) works identically regardless of which path
        produced it. Returns None for any other type, or if an Image's
        JPEG re-encode itself fails (unsupported encoding, corrupt frame,
        Pillow unavailable) - callers leave `data` untouched in that case,
        same fallback-to-raw behavior _on_topic_msg/_send_topic_live_snapshot_
        result already have.

        Uses the agent-wide default quality/dimension, same as
        _send_topic_live_snapshot_result - a one-shot/bundle capture isn't
        tied to a specific subscription's topic_limits override.
        """
        raw = data.get(BINARY_PAYLOAD_FIELD)
        if raw is None:
            return None
        payload_bytes = bytes(b & 0xFF for b in raw)
        if msg_type == 'sensor_msgs/msg/Image':
            quality = self.get_parameter('image_jpeg_quality').get_parameter_value().integer_value
            max_dimension = self.get_parameter('image_max_dimension').get_parameter_value().integer_value
            reencoded = self._reencode_image_jpeg(
                payload_bytes, data.get('width'), data.get('height'), data.get('encoding'),
                quality, max_dimension, topic_name,
            )
            if reencoded is None:
                return None
            payload_bytes, out_width, out_height = reencoded
            marker = {'format': 'jpeg', 'width': out_width, 'height': out_height}
        elif msg_type == 'sensor_msgs/msg/CompressedImage':
            marker = {'format': data.get('format') or 'unknown'}
        else:
            return None
        marker['len'] = len(payload_bytes)
        marker['encoding'] = 'base64'
        return base64.b64encode(payload_bytes).decode('ascii'), marker

    def _send_bundle_capture_result(self, request_id: str, captures: dict):
        if not self.loop:
            return
        payload = {
            'type': 'bundle_capture_result',
            'request_id': request_id,
            'captures': captures,
            'timestamp': time.time(),
        }
        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps(payload)),
            self.loop,
        )

    def _send_bundle_capture_failed(self, request_id: str, reason: str):
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps({
                'type': 'bundle_capture_failed',
                'request_id': request_id,
                'reason': reason,
                'timestamp': time.time(),
            })),
            self.loop,
        )

    def _send_topic_live_snapshot_result(self, request_id: str, topic_name: str, msg):
        """Sends a one-shot capture's result. Binary-payload types (Image/
        CompressedImage/PointCloud2/OccupancyGrid) get the same treatment
        _on_topic_msg gives the continuous topic_data stream - a raw pixel/
        point byte array as a JSON array of decimal ints is exactly the
        3-6x-inflated waste here it would be there, and for Image
        specifically a giant int array isn't something a vision model can
        even look at; it needs to actually be a JPEG (get_camera_snapshot's
        whole reason for existing). Deliberately no budget check here - a
        one-shot capture isn't part of the rate/byte-budget system that
        governs the continuous stream, and shouldn't be silently dropped by
        a cap that has nothing to do with this specific request. Type is
        re-resolved fresh (this topic was never added to _topic_msg_types -
        that's populated by the continuous, gateway-requested subscription
        path only) via the same lookup _start_one_shot_capture itself uses.
        """
        if not self.loop:
            return
        data = message_to_ordereddict(msg)

        types = dict(self.get_topic_names_and_types()).get(topic_name)
        msg_type = types[0] if types else None
        signed = BINARY_PAYLOAD_TYPES.get(msg_type)
        raw = data.get(BINARY_PAYLOAD_FIELD) if signed is not None else None
        if raw is not None:
            payload_bytes = bytes(b & 0xFF for b in raw)
            image_marker_extra = None
            if msg_type == 'sensor_msgs/msg/Image':
                quality = self.get_parameter('image_jpeg_quality').get_parameter_value().integer_value
                max_dimension = self.get_parameter('image_max_dimension').get_parameter_value().integer_value
                reencoded = self._reencode_image_jpeg(
                    payload_bytes, data.get('width'), data.get('height'), data.get('encoding'),
                    quality, max_dimension, topic_name,
                )
                if reencoded is not None:
                    payload_bytes, out_width, out_height = reencoded
                    image_marker_extra = {'format': 'jpeg', 'width': out_width, 'height': out_height}
            elif msg_type == 'sensor_msgs/msg/CompressedImage':
                image_marker_extra = {'format': data.get('format') or 'unknown'}

            marker = {'len': len(payload_bytes), 'signed': signed}
            if image_marker_extra is not None:
                marker.update(image_marker_extra)
            # Replace the (potentially huge) raw array with the small marker
            # dict BEFORE the recursive NaN-scrub below, not after - no
            # reason to walk hundreds of thousands of already-extracted ints
            # a second time.
            data[BINARY_PAYLOAD_FIELD] = {BINARY_MARKER_KEY: marker}
            data = self._json_safe_deep(data)
            header = {
                'type': 'topic_live_snapshot_result',
                'request_id': request_id,
                'topic': topic_name,
                'data': data,
                'timestamp': time.time(),
            }
            self._enqueue_binary_topic_data(header, payload_bytes)
            return

        data = self._json_safe_deep(data)
        payload = {
            'type': 'topic_live_snapshot_result',
            'request_id': request_id,
            'topic': topic_name,
            'data': data,
            'timestamp': time.time(),
        }
        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps(payload)),
            self.loop,
        )

    def _send_topic_live_snapshot_failed(self, request_id: str, reason: str):
        if not self.loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._send_queue.put(json.dumps({
                'type': 'topic_live_snapshot_failed',
                'request_id': request_id,
                'reason': reason,
                'timestamp': time.time(),
            })),
            self.loop,
        )

    # ── Bag recording ──────────────────────────────────────────────────────────

    def _get_bag_files_snapshot(self) -> list[dict]:
        """Return metadata for every completed bag in the output directory."""
        bag_dir = os.path.expanduser(
            self.get_parameter('bag_output_dir').get_parameter_value().string_value
        )
        bags = []
        try:
            entries = sorted(Path(bag_dir).iterdir())
        except (FileNotFoundError, OSError):
            return bags
        for entry in entries:
            if not entry.is_dir():
                continue
            # Skip the directory that is currently being recorded.
            with self._bag_lock:
                if str(entry) == self._bag_output_path:
                    continue
            try:
                size_bytes = sum(
                    f.stat().st_size for f in entry.rglob('*') if f.is_file()
                )
            except OSError:
                size_bytes = 0
            # Prefer the timestamp embedded in the directory name (bag_<ts>),
            # fall back to the directory mtime.
            name = entry.name
            try:
                created_at = float(name.split('_', 1)[1])
            except (IndexError, ValueError):
                created_at = entry.stat().st_mtime
            bags.append({
                'name':       name,
                'path':       str(entry),
                'size_bytes': size_bytes,
                'created_at': created_at,
            })
        return bags

    async def _send_bag_files(self):
        bags = await asyncio.to_thread(self._get_bag_files_snapshot)
        await self._send_queue.put(json.dumps({
            'type':      'bag_files',
            'bags':      bags,
            'timestamp': time.time(),
        }))
        self.get_logger().info(f"[bag] bag_files sent to gateway  count={len(bags)}")

    def _send_bag_download(self, path: str, request_id: str, upload_url: str):
        """Zip the bag directory and POST it to the gateway upload endpoint."""

        # ── Path traversal guard ──────────────────────────────────────────────
        bag_dir = os.path.expanduser(
            self.get_parameter('bag_output_dir').get_parameter_value().string_value
        )
        bag_dir_real = os.path.realpath(bag_dir)
        path_real    = os.path.realpath(path)
        if not (path_real == bag_dir_real or
                path_real.startswith(bag_dir_real + os.sep)):
            self.get_logger().error(
                f"[bag] download rejected — path outside bag_output_dir: {path}"
            )
            return

        if not os.path.isdir(path_real):
            self.get_logger().error(
                f"[bag] download rejected — not a directory: {path}"
            )
            return

        tmp_path = None
        try:
            name = os.path.basename(path_real)
            self.get_logger().info(
                f"[bag] zipping {path_real}  request_id={request_id}"
            )

            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_f:
                tmp_path = tmp_f.name

            with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for file in sorted(Path(path_real).rglob('*')):
                    if file.is_file():
                        zf.write(file, file.relative_to(Path(path_real).parent))

            zip_size = os.path.getsize(tmp_path)
            self.get_logger().info(
                f"[bag] zip ready  name={name}.zip  size={zip_size} bytes  "
                f"uploading to {upload_url}"
            )

            # ── Stream-upload via stdlib http.client (no extra dependencies) ───
            # Derive host/scheme from the active WS URL so the upload reaches
            # the same endpoint even when the gateway sends 'localhost' (which
            # would resolve to the container itself, not the host).
            ws_parsed     = urllib.parse.urlparse(self.ws_url)
            http_scheme   = 'https' if ws_parsed.scheme == 'wss' else 'http'
            netloc        = ws_parsed.netloc.split('?')[0]  # strip any query fragment
            upload_path   = urllib.parse.urlparse(upload_url).path
            qs            = urllib.parse.urlencode({'request_id': request_id})
            path_q        = f"{upload_path}?{qs}"

            self.get_logger().info(
                f"[bag] effective upload target: {http_scheme}://{netloc}{path_q}"
            )

            conn = (
                http.client.HTTPSConnection(netloc, timeout=120)
                if http_scheme == 'https'
                else http.client.HTTPConnection(netloc, timeout=120)
            )
            try:
                with open(tmp_path, 'rb') as f:
                    conn.request(
                        'POST', path_q, body=f,
                        headers={
                            'Content-Type':   'application/zip',
                            'Content-Length': str(zip_size),
                        },
                    )
                resp = conn.getresponse()
                resp.read()  # drain so the connection can be reused / closed cleanly
                self.get_logger().info(
                    f"[bag] upload complete  status={resp.status}  "
                    f"request_id={request_id}"
                )
            finally:
                conn.close()

        except Exception as e:
            self.get_logger().error(
                f"[bag] download upload failed  request_id={request_id}: {e}"
            )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                    self.get_logger().debug(f"[bag] temp zip deleted: {tmp_path}")
                except OSError:
                    pass

    def _start_bag_recording(self, data: dict):
        with self._bag_lock:
            if self._bag_proc is not None and self._bag_proc.poll() is None:
                self.get_logger().warning(
                    "[bag] rejected start — recording already in progress"
                )
                self._enqueue({
                    'type': 'error',
                    'message': 'Bag recording already in progress',
                })
                return

            topics: list[str] = data.get('topics', [])
            bag_dir = os.path.expanduser(
                self.get_parameter('bag_output_dir').get_parameter_value().string_value
            )
            os.makedirs(bag_dir, exist_ok=True)
            output_path = os.path.join(bag_dir, f'bag_{int(time.time())}')

            if topics:
                cmd = ['ros2', 'bag', 'record', '-o', output_path] + topics
            else:
                cmd = ['ros2', 'bag', 'record', '-a', '-o', output_path]

            self.get_logger().info(
                f"[bag] launching subprocess: {' '.join(cmd)}"
            )
            self._bag_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            self._bag_output_path = output_path
            self.get_logger().info(
                f"[bag] subprocess started  pid={self._bag_proc.pid}  output={output_path}"
            )

        self._enqueue({
            'type':      'bag_record_started',
            'path':      output_path,
            'topics':    topics if topics else 'all',
            'timestamp': time.time(),
        })
        self.get_logger().info(
            f"[bag] bag_record_started sent to gateway  topics={topics or 'all'}"
        )

    def _stop_bag_recording(self):
        with self._bag_lock:
            if self._bag_proc is None or self._bag_proc.poll() is not None:
                self.get_logger().warning(
                    "[bag] rejected stop — no recording is currently in progress"
                )
                self._enqueue({
                    'type':    'error',
                    'message': 'No bag recording is currently in progress',
                })
                return

            proc = self._bag_proc
            output_path = self._bag_output_path
            self._bag_proc = None
            self._bag_output_path = None

        # SIGINT lets ros2 bag flush the SQLite index before exiting.
        self.get_logger().info(
            f"[bag] sending SIGINT to pid={proc.pid}  output={output_path}"
        )
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=10)
            self.get_logger().info("[bag] subprocess exited cleanly")
        except subprocess.TimeoutExpired:
            self.get_logger().warning(
                "[bag] subprocess did not exit within 10 s — sending SIGKILL"
            )
            proc.kill()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass

        size_bytes = 0
        try:
            size_bytes = sum(
                f.stat().st_size
                for f in Path(output_path).rglob('*')
                if f.is_file()
            )
        except (FileNotFoundError, OSError):
            pass

        name = os.path.basename(output_path)
        self.get_logger().info(
            f"[bag] recording finalised  name={name}  size={size_bytes} bytes"
        )
        self._enqueue({
            'type':       'bag_record_stopped',
            'name':       name,
            'path':       output_path,
            'size_bytes': size_bytes,
            'timestamp':  time.time(),
        })
        self.get_logger().info(
            f"[bag] bag_record_stopped sent to gateway  size={size_bytes} bytes"
        )

    def _on_topic_msg(self, msg, topic_name: str):
        if not self.ws or not self.loop:
            return

        ts = time.time()
        with self._topic_rate_lock:
            self._topic_rate_timestamps.setdefault(topic_name, deque()).append(ts)

        # Per-topic overrides (set live via agent_config's 'topic_limits',
        # see _apply_agent_config) take precedence over the two defaults
        # below when present for this topic.
        with self._topic_limit_lock:
            override = self._topic_limit_overrides.get(topic_name, {})

        # Cap how often a given topic's data actually gets forwarded — a fast
        # topic (odom, joint_states, ...) publishing at 30-50+ Hz is far past
        # what's perceptible in Watch/Plot, and every message costs a DB row
        # on the gateway besides. rate_hz itself stays accurate since it's
        # computed from _topic_rate_timestamps above, sampled every receipt
        # regardless of this throttle. Full-fidelity capture, when actually
        # needed, goes through bag recording instead of this live stream.
        # Read fresh (not cached) same as graph_debounce_interval/bag_output_dir
        # — cheap local lookup, and lets a config change take effect without
        # a reconnect. Hz (not a raw interval) since that's the unit rate_hz
        # is already shown in everywhere else in the UI; 0 means uncapped.
        rate_hz = override.get('rate_hz')
        if rate_hz is None:
            rate_hz = self.get_parameter('topic_data_rate_hz').get_parameter_value().double_value
        if rate_hz > 0 and ts - self._topic_data_throttle.get(topic_name, 0.0) < 1.0 / rate_hz:
            if ts - self._rate_throttle_drop_logged.get(topic_name, 0.0) >= 5.0:
                self._rate_throttle_drop_logged[topic_name] = ts
                source = 'per-topic override' if override.get('rate_hz') is not None else 'topic_data_rate_hz default'
                self.get_logger().warning(f'[topic_data] {topic_name}: Hz-throttled to {rate_hz:.2f} Hz ({source})')
            return
        self._topic_data_throttle[topic_name] = ts

        data = message_to_ordereddict(msg)

        # For the byte-array-dominated types (see BINARY_PAYLOAD_TYPES), pull
        # the array out and send it as a raw binary WS frame instead of
        # leaving it for json.dumps to inflate into a comma-separated array
        # of ints. See BINARY_MARKER_KEY and the gateway's isBinary handler
        # for the other half of this.
        with self._topic_subs_lock:
            msg_type = self._topic_msg_types.get(topic_name)
        signed = BINARY_PAYLOAD_TYPES.get(msg_type)
        raw = data.get(BINARY_PAYLOAD_FIELD) if signed is not None else None
        if raw is not None:
            # message_to_ordereddict gives back a plain list of already
            # correctly-signed Python ints (e.g. -1 for an OccupancyGrid
            # "unknown" cell). `& 0xFF` takes each one to its two's-complement
            # byte value regardless of signedness — the same bit pattern
            # either way — which is exactly what bytes() needs and exactly
            # what the gateway's Int8Array/Uint8Array reinterprets on the way
            # back using the 'signed' flag below.
            payload = bytes(b & 0xFF for b in raw)

            # Image: re-encode to JPEG before the budget check below, so the
            # cap is enforced against the actual bytes going out, not the raw
            # pixel size this replaces. Falls through with the original raw
            # payload untouched if Pillow isn't installed, the encoding isn't
            # one this recognizes, or the buffer doesn't match the declared
            # dimensions. CompressedImage: already compressed by the camera
            # driver, nothing to re-encode, just marked the same way (see
            # the elif below) for the gateway's benefit.
            image_marker_extra = None
            if msg_type == 'sensor_msgs/msg/Image':
                quality = override.get('jpeg_quality')
                if quality is None:
                    quality = self.get_parameter('image_jpeg_quality').get_parameter_value().integer_value
                max_dimension = override.get('max_dimension')
                if max_dimension is None:
                    max_dimension = self.get_parameter('image_max_dimension').get_parameter_value().integer_value
                reencoded = self._reencode_image_jpeg(
                    payload, data.get('width'), data.get('height'), data.get('encoding'),
                    quality, max_dimension, topic_name,
                )
                if reencoded is not None:
                    payload, out_width, out_height = reencoded
                    image_marker_extra = {'format': 'jpeg', 'width': out_width, 'height': out_height}
            elif msg_type == 'sensor_msgs/msg/CompressedImage':
                # Already compressed by the driver - nothing to re-encode,
                # just marked the same way a re-encoded Image is so the
                # gateway relays it as base64 instead of reconstructing a
                # JSON array of decimal ints (see index.js's isBinary
                # handler) - the same win Image gets, for the same reason
                # (it's already-compressed opaque bytes, not raw pixels a
                # future consumer would index into numerically). No
                # width/height here - CompressedImage's own ROS message
                # doesn't carry them, they're implicit in the compressed
                # bytes themselves.
                image_marker_extra = {'format': data.get('format') or 'unknown'}

            if self._topic_data_over_budget(topic_name, len(payload), ts, override):
                return
            marker = {'len': len(payload), 'signed': signed}
            if image_marker_extra is not None:
                marker.update(image_marker_extra)
            data[BINARY_PAYLOAD_FIELD] = {BINARY_MARKER_KEY: marker}
            self._enqueue_binary_topic_data({
                'type': 'topic_data',
                'topic': topic_name,
                'data': data,
                'timestamp': ts,
            }, payload)
            return

        body = json.dumps({
            'type': 'topic_data',
            'topic': topic_name,
            'data': data,
            'timestamp': ts,
        })
        if self._topic_data_over_budget(topic_name, len(body), ts, override):
            return
        asyncio.run_coroutine_threadsafe(self._send_queue.put(body), self.loop)

    def _topic_data_over_budget(self, topic_name: str, size: int, ts: float, override: dict) -> bool:
        """True if forwarding `size` more bytes for `topic_name` right now
        would exceed that topic's own bytes/sec budget or the connection
        -wide budget shared across every subscribed topic (see the three
        topic_data_max_bytes_per_sec / global_topic_data_max_* parameters
        and self._topic_limit_overrides). Only topic_data competes for
        either budget — graph state, lifecycle events, etc. go through plain
        _enqueue and are never throttled here, on purpose: the point is
        protecting the heavy sensor stream without ever starving the small
        control-plane traffic that the rest of the UI depends on.

        Both budgets are simple 1-second buckets that reset once a full
        second has elapsed since the window opened, rather than a true
        sliding window — coarser, but consistent with the Hz throttle's own
        granularity above and much cheaper than tracking individual message
        timestamps per topic.
        """
        # Recorded unconditionally, before any cap is even read - same
        # "measure the real thing, not what Osiris decided to let through"
        # reasoning as _topic_rate_timestamps in _on_topic_msg. A topic
        # capped hard enough to drop every message would otherwise report
        # 0 B/s, hiding exactly the number someone raising that cap needs.
        with self._topic_rate_lock:
            self._topic_byte_timestamps.setdefault(topic_name, deque()).append((ts, size))

        topic_cap = override.get('max_bytes_per_sec')
        if topic_cap is None:
            topic_cap = self.get_parameter('topic_data_max_bytes_per_sec').get_parameter_value().double_value
        global_byte_cap = self.get_parameter('global_topic_data_max_bytes_per_sec').get_parameter_value().double_value
        global_msg_cap  = self.get_parameter('global_topic_data_max_msgs_per_sec').get_parameter_value().double_value

        t_start, t_bytes = self._topic_byte_bucket.get(topic_name, (ts, 0))
        if ts - t_start >= 1.0:
            t_start, t_bytes = ts, 0

        g_start, g_bytes, g_msgs = self._global_topic_data_bucket
        if ts - g_start >= 1.0:
            g_start, g_bytes, g_msgs = ts, 0, 0

        # Which specific cap tripped, if any - not just a bool. A silent
        # bool here was exactly what made an unexpectedly-low forwarded rate
        # (a topic capped far below its real publish rate, with nothing in
        # the log explaining why) hard to tell apart from a genuinely slow
        # source, a Hz-throttle, or a processing bottleneck elsewhere.
        reason = None
        if topic_cap > 0 and t_bytes + size > topic_cap:
            reason = f'topic byte budget ({t_bytes + size:.0f}/{topic_cap:.0f} B/s)'
        elif global_byte_cap > 0 and g_bytes + size > global_byte_cap:
            reason = f'global byte budget ({g_bytes + size:.0f}/{global_byte_cap:.0f} B/s)'
        elif global_msg_cap > 0 and g_msgs + 1 > global_msg_cap:
            reason = f'global msg-rate budget ({g_msgs + 1:.0f}/{global_msg_cap:.0f} msg/s)'
        over_budget = reason is not None
        if over_budget and ts - self._budget_drop_logged.get(topic_name, 0.0) >= 5.0:
            self._budget_drop_logged[topic_name] = ts
            self.get_logger().warning(f'[topic_data] {topic_name}: dropping ({size} bytes) - over {reason}')
        # Window boundaries roll over either way, even when dropping this
        # message — otherwise a window that opened before a cap was ever hit
        # could never advance past its 1s mark while messages keep getting
        # dropped, wedging the topic permanently. Only the running totals
        # skip the increment when the message is actually dropped.
        if over_budget:
            self._topic_byte_bucket[topic_name] = (t_start, t_bytes)
            self._global_topic_data_bucket = (g_start, g_bytes, g_msgs)
        else:
            self._topic_byte_bucket[topic_name] = (t_start, t_bytes + size)
            self._global_topic_data_bucket = (g_start, g_bytes + size, g_msgs + 1)
            # This message is actually being sent - the one point that's
            # true for every topic_data message, binary or plain JSON,
            # regardless of which of the two callers above got here. A
            # message dropped by the Hz throttle earlier in _on_topic_msg
            # never reaches this function at all, so recording only here
            # already accounts for both gates without checking either
            # explicitly.
            with self._topic_rate_lock:
                self._topic_delivered_rate_timestamps.setdefault(topic_name, deque()).append(ts)
                self._topic_delivered_byte_timestamps.setdefault(topic_name, deque()).append((ts, size))
        return over_budget

    def _reencode_image_jpeg(self, raw: bytes, width, height, encoding, quality, max_dimension, topic_name: str):
        """Re-encodes a raw sensor_msgs/Image byte buffer to JPEG via Pillow
        - a raw rgb8/bgr8 frame is close to the worst case possible per byte
        actually sent, so this is the real bandwidth win for a live camera
        feed (the binary framing above just removes JSON's own encoding
        overhead, it doesn't touch the pixel data's size at all).

        Returns (jpeg_bytes, out_width, out_height) on success, None if
        Pillow isn't installed, the encoding isn't one of the common ones in
        IMAGE_ENCODING_PIL_MODES (a depth/bayer/YUV camera), or the buffer's
        length doesn't match what width/height/encoding declare (a corrupt
        or partial frame) - callers fall back to sending the original raw
        bytes exactly as before this feature existed. Never raises; a
        genuine encode failure is caught and treated the same as an
        unsupported encoding.
        """
        if not _PIL_AVAILABLE or not width or not height:
            return None
        modes = IMAGE_ENCODING_PIL_MODES.get(encoding)
        if modes is None:
            if topic_name not in self._image_reencode_unsupported_warned:
                self._image_reencode_unsupported_warned.add(topic_name)
                self.get_logger().info(
                    f"[image] {topic_name}: encoding '{encoding}' not supported for JPEG "
                    f"re-encode, sending raw pixel bytes"
                )
            return None
        pil_mode, raw_mode = modes
        expected_len = width * height * _IMAGE_ENCODING_CHANNELS[raw_mode]
        if len(raw) != expected_len:
            return None
        try:
            img = PILImage.frombuffer(pil_mode, (width, height), raw, 'raw', raw_mode, 0, 1)
            if pil_mode == 'RGBA':
                # JPEG has no alpha channel - drop it rather than fail the
                # whole re-encode over a channel nothing downstream reads yet.
                img = img.convert('RGB')
            if max_dimension and max(img.width, img.height) > max_dimension:
                scale = max_dimension / max(img.width, img.height)
                new_size = (max(1, round(img.width * scale)), max(1, round(img.height * scale)))
                img = img.resize(new_size, PILImage.BILINEAR)
            buf = io.BytesIO()
            img.save(buf, format='JPEG', quality=quality)
            return buf.getvalue(), img.width, img.height
        except Exception as e:
            self.get_logger().warning(f'[image] {topic_name}: JPEG re-encode failed: {e}')
            return None

    def _publish_topic_rates(self):
        """Periodic 1Hz timer callback — recomputes every subscribed topic's
        rate_hz/bytes_per_sec (demand: sampled on receipt, before the Hz
        throttle or byte budget get a say) and delivered_rate/delivered_bytes
        (reality: sampled only for a message that actually got sent, see
        _topic_data_over_budget) from rolling windows, and pushes all four as
        one message - independent of whether that topic published anything
        this tick, which is what makes a quiet topic's numbers correctly
        decay to 0 instead of freezing at their last computed value."""
        if not self.ws or not self.loop:
            return

        with self._topic_subs_lock:
            subscribed = list(self._topic_subs.keys())

        now = time.time()
        cutoff = now - self._RATE_WINDOW_S

        def _drain(buckets: dict, topic: str, is_pair: bool):
            """Shared cutoff/average logic for all four rolling windows below
            - rates/byte_rates (demand, before either gate) and
            delivered_rates/delivered_byte_rates (after both gates) all have
            the exact same shape, just fed from a different timestamp buffer."""
            buf = buckets.get(topic)
            if not buf:
                return 0.0
            if is_pair:
                while buf and buf[0][0] < cutoff:
                    buf.popleft()
                return round(sum(size for _, size in buf) / self._RATE_WINDOW_S, 1)
            while buf and buf[0] < cutoff:
                buf.popleft()
            return round(len(buf) / self._RATE_WINDOW_S, 2)

        rates = {}
        byte_rates = {}
        delivered_rates = {}
        delivered_byte_rates = {}
        with self._topic_rate_lock:
            for topic in subscribed:
                rates[topic] = _drain(self._topic_rate_timestamps, topic, is_pair=False)
                byte_rates[topic] = _drain(self._topic_byte_timestamps, topic, is_pair=True)
                delivered_rates[topic] = _drain(self._topic_delivered_rate_timestamps, topic, is_pair=False)
                delivered_byte_rates[topic] = _drain(self._topic_delivered_byte_timestamps, topic, is_pair=True)
            # Drop buffers for topics no longer subscribed so these dicts don't
            # grow unbounded across repeated subscribe/unsubscribe cycles.
            for bucket in (
                self._topic_rate_timestamps, self._topic_byte_timestamps,
                self._topic_delivered_rate_timestamps, self._topic_delivered_byte_timestamps,
            ):
                for stale in set(bucket) - set(subscribed):
                    del bucket[stale]

        self._enqueue({
            'type': 'topic_rates',
            'rates': rates,
            'byte_rates': byte_rates,
            'delivered_rates': delivered_rates,
            'delivered_byte_rates': delivered_byte_rates,
            'timestamp': now,
        })

    # ──────────────────────────────────────────────
    # Lifecycle (managed nodes)
    # ──────────────────────────────────────────────

    def _subscribe_lifecycle_topic(self, topic: str):
        """Subscribe to a /<node>/transition_event topic."""
        if topic in self._lifecycle_subs:
            return
        try:
            from lifecycle_msgs.msg import TransitionEvent
            node_fqn = topic[:-len('/transition_event')]
            sub = self.create_subscription(
                TransitionEvent,
                topic,
                lambda msg, n=node_fqn: self._on_lifecycle_transition(msg, n),
                QoSProfile(depth=10),
            )
            self._lifecycle_subs[topic] = sub
            self.get_logger().info(f'[lifecycle] subscribed to {topic}')
        except Exception as e:
            self.get_logger().debug(f'[lifecycle] could not subscribe to {topic}: {e}')

    def _fetch_lifecycle_state_async(self, node_fqn: str):
        """Query /<node>/get_state to populate _lifecycle_state_cache."""
        if node_fqn in self._lifecycle_state_cache:
            return
        if node_fqn in self._pending_lifecycle_fetches:
            return
        try:
            from lifecycle_msgs.srv import GetState
        except ImportError:
            return
        client = self.create_client(GetState, f'{node_fqn}/get_state')
        if not client.service_is_ready():
            self.destroy_client(client)
            return
        self._pending_lifecycle_fetches.add(node_fqn)
        future = client.call_async(GetState.Request())

        def _on_get_state(fut):
            self._pending_lifecycle_fetches.discard(node_fqn)
            self.destroy_client(client)
            try:
                resp = fut.result()
                if resp is not None:
                    self._lifecycle_state_cache[node_fqn] = resp.current_state.label
                    self._graph_dirty = True
                    self.get_logger().debug(
                        f'[lifecycle] initial state for {node_fqn}: {resp.current_state.label}'
                    )
            except Exception as e:
                self.get_logger().debug(f'[lifecycle] get_state failed for {node_fqn}: {e}')

        future.add_done_callback(_on_get_state)

    def _on_lifecycle_transition(self, msg, node_fqn: str):
        self._lifecycle_state_cache[node_fqn] = msg.goal_state.label
        self.get_logger().info(
            f'[lifecycle] {node_fqn}: {msg.start_state.label} → {msg.goal_state.label} '
            f'(transition: {msg.transition.label})'
        )
        self._enqueue({
            'type': 'lifecycle_event',
            'node': node_fqn,
            'transition': msg.transition.label,
            'from_state': msg.start_state.label,
            'to_state': msg.goal_state.label,
            'timestamp': time.time(),
        })
        self._graph_dirty = True
        self._trigger_graph_poll()

    # ──────────────────────────────────────────────
    # Action monitoring (status + feedback)
    # ──────────────────────────────────────────────

    def _subscribe_action_status(self, action_name: str):
        if action_name in self._action_status_subs:
            return
        try:
            from action_msgs.msg import GoalStatusArray
            # Action status publishers use RELIABLE + TRANSIENT_LOCAL
            # (rcl_action_qos_profile_status_default), not the plain default
            # (VOLATILE) QoSProfile(depth=10) used elsewhere — a durability
            # mismatch here means a subscription created after a goal already
            # reached a terminal state (agent restart, resubscribe, or any
            # gap in this specific subscription's lifetime) would never
            # receive that goal's actual last status. qos_profile_action_
            # status_default is rclpy's own copy of that exact profile.
            sub = self.create_subscription(
                GoalStatusArray,
                f'{action_name}/_action/status',
                lambda msg, a=action_name: self._on_action_status(msg, a),
                qos_profile_action_status_default,
            )
            self._action_status_subs[action_name] = sub
            self.get_logger().info(f'[actions] subscribed to status for {action_name}')
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to subscribe to status for {action_name}: {e}')

    def _subscribe_action_feedback(self, action_name: str, feedback_msg_cls):
        if action_name in self._action_feedback_subs:
            return
        try:
            sub = self.create_subscription(
                feedback_msg_cls,
                f'{action_name}/_action/feedback',
                lambda msg, a=action_name: self._on_action_feedback(msg, a),
                QoSProfile(depth=10),
            )
            self._action_feedback_subs[action_name] = sub
            self.get_logger().info(f'[actions] subscribed to feedback for {action_name}')
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to subscribe to feedback for {action_name}: {e}')

    def _catch_up_action_feedback(self, action_name: str):
        """Subscribes to feedback for an action whose type was already
        resolved (cached) while goals_enabled was False. _fetch_action_types
        early-returns once an action is cached, so simply re-calling it here
        would silently no-op instead of subscribing — this reuses the
        cached _feedback_msg_type instead of redoing type resolution."""
        type_info = self._action_type_cache.get(action_name)
        if not type_info:
            return
        try:
            feedback_msg_cls = get_message(type_info['_feedback_msg_type'])
            self._subscribe_action_feedback(action_name, feedback_msg_cls)
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to catch up feedback subscription for {action_name}: {e}')

    def _unsubscribe_action_monitoring(self, action_name: str):
        sub = self._action_status_subs.pop(action_name, None)
        if sub:
            self.destroy_subscription(sub)
        sub = self._action_feedback_subs.pop(action_name, None)
        if sub:
            self.destroy_subscription(sub)
        self._action_goal_states.pop(action_name, None)
        self._action_feedback_throttle.pop(action_name, None)

    def _on_action_status(self, msg, action_name: str):
        prev = self._action_goal_states.get(action_name, {})
        current = {bytes(s.goal_info.goal_id.uuid).hex(): s.status for s in msg.status_list}
        for uuid_hex, status in current.items():
            if uuid_hex not in prev or prev[uuid_hex] != status:
                self._enqueue({
                    'type': 'goal_event',
                    'action': action_name,
                    'goal_id': uuid_hex,
                    'status': status,
                    'timestamp': time.time(),
                })
        self._action_goal_states[action_name] = current

    def _on_action_feedback(self, msg, action_name: str):
        now = time.time()
        if now - self._action_feedback_throttle.get(action_name, 0.0) < ACTION_FEEDBACK_MIN_INTERVAL:
            return
        self._action_feedback_throttle[action_name] = now
        try:
            feedback_data = message_to_ordereddict(msg.feedback)
        except Exception as e:
            self.get_logger().warning(f'[actions] feedback serialization failed for {action_name}: {e}')
            return
        self._enqueue({
            'type': 'action_feedback',
            'action': action_name,
            'goal_id': bytes(msg.goal_id.uuid).hex(),
            'feedback': feedback_data,
            'timestamp': now,
        })

    # ──────────────────────────────────────────────
    # Command sending (send_command_request / cancel_command_request)
    # ──────────────────────────────────────────────

    def _get_command_action_client(self, command: str):
        """Lazily imports the action type and creates (once) the ActionClient
        for this command. Returns None if the type isn't importable (package
        not installed) — a permanent condition for this process, not worth
        retrying on every request."""
        if command in self._command_action_clients:
            return self._command_action_clients[command]

        command_def = COMMAND_DEFS[command]
        try:
            module = __import__(command_def['module'], fromlist=[command_def['cls_name']])
            msg_cls = getattr(module, command_def['cls_name'])
        except ImportError as e:
            self.get_logger().warning(f'[command] {command} unavailable, import failed: {e}')
            self._command_action_clients[command] = None
            return None

        client = ActionClient(self, msg_cls, command_def['action_name'])
        self._command_action_clients[command] = client
        return client

    @staticmethod
    def _seconds_to_duration(seconds: float):
        from builtin_interfaces.msg import Duration
        d = Duration()
        d.sec = int(seconds)
        d.nanosec = int(round((seconds - d.sec) * 1e9))
        return d

    def _apply_shared_goal_fields(self, goal, command: str, time_allowance_s: float, disable_collision_checks: bool):
        # time_allowance and disable_collision_checks are shared by both
        # Spin.action and DriveOnHeading.action (verified against the real
        # nav2_msgs action definitions, same as target_yaw/target/speed in
        # the callers below). time_allowance previously went unset — a zero
        # Duration, which nav2_behaviors treats as unbounded — meaning a
        # behavior stuck never reaching its stop condition (a real bug seen
        # on this exact robot) had no time-based backstop at all.
        # DEFAULT_TIME_ALLOWANCE_S gives every command a bounded worst-case
        # runtime by default while staying overridable; still comfortably
        # under the gateway's own ROUTE_STEP_GOAL_TIMEOUT_MS wait-for-
        # terminal ceiling (120s), so Nav2 gets a chance to self-abort before
        # that gives up waiting.
        goal.time_allowance = self._seconds_to_duration(time_allowance_s)

        # disable_collision_checks was added to Spin.action/DriveOnHeading.
        # action after Humble (confirmed absent there against the real
        # nav2_msgs source — Humble's goal only has target_yaw/target/speed/
        # time_allowance) — a real version this project needs to keep
        # working against, not a hypothetical. Older Nav2 installs have no
        # way to disable the check at all, so it's always effectively off
        # regardless of what's asked; hasattr avoids the AttributeError
        # ROS2's slotted generated message classes raise for an unknown
        # field. An explicit request that can't be honored is logged rather
        # than silently dropped, but still proceeds with checking ON (the
        # safe fallback, and the only option this install actually has).
        if hasattr(goal, 'disable_collision_checks'):
            goal.disable_collision_checks = disable_collision_checks
        elif disable_collision_checks:
            self.get_logger().warning(
                f'[command] disable_collision_checks was requested for {command} but this '
                f"Nav2 install's {type(goal).__name__} has no such field (added after Humble) "
                f'— proceeding with collision checking ON.'
            )

    def _build_command_goal(self, command: str, params: dict, msg_cls):
        time_allowance_s = float(params.get('time_allowance_s', DEFAULT_TIME_ALLOWANCE_S))
        disable_collision_checks = bool(params.get('disable_collision_checks', False))

        if command == 'spin':
            goal = msg_cls.Goal()
            goal.target_yaw = float(params.get('target_yaw_rad', 0.0))
            self._apply_shared_goal_fields(goal, command, time_allowance_s, disable_collision_checks)
            return goal

        if command == 'drive':
            goal = msg_cls.Goal()
            # target is a relative point in the robot's own base frame — x is
            # straight ahead (negative = backward), y/z stay 0 for a pure
            # straight-line drive. Field names verified against the real
            # DriveOnHeading.action definition.
            goal.target.x = float(params.get('distance_m', 0.0))
            goal.target.y = 0.0
            goal.target.z = 0.0
            goal.speed = float(params.get('speed_mps', 0.15))
            self._apply_shared_goal_fields(goal, command, time_allowance_s, disable_collision_checks)
            return goal

        raise ValueError(f'no goal builder for command {command}')

    def _check_battery_and_dock_preconditions(self) -> str | None:
        """Returns a precondition_failed:* reason string if it isn't safe to
        move right now, else None. Each check is independently opt-in via its
        own toggle (battery_check_enabled / dock_check_enabled) — a robot
        that hasn't turned one on just doesn't get gated by it at all. Once
        enabled, this is universal and unconditional: it applies to every
        command in COMMAND_DEFS the same way, with no way for a caller to
        pass a flag to skip it (unlike disable_collision_checks, which is a
        deliberate per-call opt-out of a different check).

        Fails closed on missing/stale data, not just on an actual bad
        reading: no cached value at all, or one older than
        STATUS_STALE_AFTER_S, is treated the same as an unsafe reading
        ('_unknown' reasons below) rather than silently letting the command
        through. This covers both "never received a message on this topic"
        (e.g. a latched publisher whose replay our subscription's QoS
        doesn't match — see the config doc for _dock_status_sub) and "the
        publisher died" with the same mechanism.
        """
        now = time.time()

        if self._battery_check_enabled:
            if self._last_battery_state is None or self._last_battery_state_time is None:
                return 'precondition_failed:battery_unknown'
            if now - self._last_battery_state_time > STATUS_STALE_AFTER_S:
                return 'precondition_failed:battery_unknown'
            percent = self._last_battery_state.get('percent')
            if percent is None:
                return 'precondition_failed:battery_unknown'
            if percent < self._battery_min_percent:
                return 'precondition_failed:battery_low'

        if self._dock_check_enabled:
            if self._last_dock_status is None or self._last_dock_status_time is None:
                return 'precondition_failed:dock_status_unknown'
            if now - self._last_dock_status_time > STATUS_STALE_AFTER_S:
                return 'precondition_failed:dock_status_unknown'
            if self._last_dock_status.get('is_docked'):
                return 'precondition_failed:docked'

        return None

    def _handle_send_command_request(self, data: dict):
        request_id = data.get('request_id', '')
        command = data.get('command')
        params = data.get('params') or {}

        command_def = COMMAND_DEFS.get(command)
        if not command_def:
            self._send_command_rejected(request_id, 'unknown_command')
            return

        if not self._goals_enabled:
            self._send_command_rejected(request_id, 'goals_disabled')
            return

        # Only one AI/user-commanded goal in flight at a time — claim the
        # slot immediately so two overlapping requests can't both pass this
        # check before either resolves. Cleared on reject, on accept-failure,
        # and once the accepted goal reaches a terminal state (_on_goal_result).
        with self._active_command_lock:
            if self._active_command_pending or self._active_command_goal_handle is not None:
                self._send_command_rejected(request_id, 'command_already_in_progress')
                return
            self._active_command_pending = True

        def _release_pending():
            with self._active_command_lock:
                self._active_command_pending = False

        client = self._get_command_action_client(command)
        if client is None:
            _release_pending()
            self._send_command_rejected(request_id, 'action_type_unavailable')
            return

        # Non-blocking — deliberately not wait_for_server(), which blocks the
        # asyncio loop thread this runs on for up to its whole timeout and
        # would freeze all other WS traffic while waiting. If the server
        # isn't known-ready right now, fail fast rather than stall.
        if not client.server_is_ready():
            _release_pending()
            self._send_command_rejected(request_id, 'action_server_not_available')
            return

        # Precondition: the action server existing isn't enough on its own for
        # commands that depend on more than that (none currently do — both
        # spin and drive only need their own server up, no lifecycle-managed
        # dependency — but this stays in place for whatever needs it next).
        # Reuses _lifecycle_state_cache, which the graph watcher already keeps
        # live via transition-event subscriptions (see _on_lifecycle_transition),
        # not a one-off snapshot.
        for node_fqn in command_def['precondition_nodes']:
            state = self._lifecycle_state_cache.get(node_fqn)
            if state != 'active':
                _release_pending()
                self._send_command_rejected(request_id, f'precondition_failed:{node_fqn}_not_active')
                return

        # Battery/dock safety check — universal across every command in
        # COMMAND_DEFS, not per-command opt-in like precondition_nodes above,
        # and not something the caller can pass a flag to skip. See
        # _check_battery_and_dock_preconditions for what each check requires.
        safety_reason = self._check_battery_and_dock_preconditions()
        if safety_reason:
            _release_pending()
            self._send_command_rejected(request_id, safety_reason)
            return

        try:
            module = __import__(command_def['module'], fromlist=[command_def['cls_name']])
            msg_cls = getattr(module, command_def['cls_name'])
            goal_msg = self._build_command_goal(command, params, msg_cls)
        except Exception as e:
            _release_pending()
            self._send_command_rejected(request_id, f'invalid_params:{e}')
            return

        send_future = client.send_goal_async(goal_msg)
        send_future.add_done_callback(
            lambda fut, rid=request_id, cmd=command: self._on_command_send_response(fut, rid, cmd)
        )

    def _on_command_send_response(self, future, request_id: str, command: str):
        try:
            goal_handle = future.result()
        except Exception as e:
            with self._active_command_lock:
                self._active_command_pending = False
            self._send_command_rejected(request_id, f'send_failed:{e}')
            return

        if not goal_handle.accepted:
            with self._active_command_lock:
                self._active_command_pending = False
            self._send_command_rejected(request_id, 'goal_rejected_by_server')
            return

        with self._active_command_lock:
            self._active_command_pending = False
            self._active_command_goal_handle = goal_handle
            timer = threading.Timer(
                ACTIVE_COMMAND_TIMEOUT_S,
                lambda gh=goal_handle: self._force_release_active_command(gh),
            )
            timer.daemon = True
            self._active_command_timeout_timer = timer
            timer.start()

        self._enqueue({
            'type': 'command_accepted',
            'request_id': request_id,
            'command': command,
            'goal_id': bytes(goal_handle.goal_id.uuid).hex(),
            'timestamp': time.time(),
        })

        # Progress/outcome is already reported via the existing generic
        # goal_event pipeline (_on_action_status monitors every action in the
        # graph, not just ones this code sent). This callback exists only to
        # release the "one command at a time" slot once this goal is done —
        # the normal path. _force_release_active_command (via the timer
        # started above) is the fallback for a goal that never gets here at
        # all, e.g. a behavior that never reaches a terminal state on its own
        # and doesn't cleanly terminate after a cancel either.
        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(lambda fut, gh=goal_handle: self._on_command_goal_result(gh))

    def _on_command_goal_result(self, goal_handle):
        with self._active_command_lock:
            if self._active_command_goal_handle is goal_handle:
                self._active_command_goal_handle = None
                if self._active_command_timeout_timer is not None:
                    self._active_command_timeout_timer.cancel()
                    self._active_command_timeout_timer = None

    def _force_release_active_command(self, goal_handle):
        """Fires from the timeout timer, on whatever thread threading.Timer
        uses — not the ROS executor thread, same as the other threading-based
        timers in this file (_trigger_graph_poll). Only acts if this is still
        the SAME goal that's still stuck; a goal that finished normally
        already cleared both the handle and this timer together in
        _on_command_goal_result, so a late/stale timer firing is a no-op."""
        with self._active_command_lock:
            if self._active_command_goal_handle is not goal_handle:
                return
            self._active_command_goal_handle = None
            self._active_command_timeout_timer = None
        self.get_logger().warning(
            f'[command] goal {bytes(goal_handle.goal_id.uuid).hex()} never reached a terminal '
            f'state within {ACTIVE_COMMAND_TIMEOUT_S}s — force-releasing the command slot so '
            f'future commands aren\'t blocked. The underlying ROS goal may still be running; '
            f'this only unblocks send_command_request, it does not cancel anything.'
        )

    def _send_command_rejected(self, request_id: str, reason: str):
        self._enqueue({
            'type': 'command_rejected',
            'request_id': request_id,
            'reason': reason,
            'timestamp': time.time(),
        })

    def _handle_cancel_command_request(self, data: dict):
        """Manual stop — bypasses the AI entirely by design. Cancels whatever
        command is currently active/in-flight, regardless of what requested
        it or what conversation (if any) triggered it."""
        request_id = data.get('request_id', '')
        with self._active_command_lock:
            goal_handle = self._active_command_goal_handle
        if goal_handle is None:
            self._enqueue({
                'type': 'cancel_failed',
                'request_id': request_id,
                'reason': 'no_active_command',
                'timestamp': time.time(),
            })
            return

        cancel_future = goal_handle.cancel_goal_async()

        def _on_cancel_done(fut, rid=request_id):
            self._enqueue({
                'type': 'command_cancelled',
                'request_id': rid,
                'timestamp': time.time(),
            })

        cancel_future.add_done_callback(_on_cancel_done)

    # ──────────────────────────────────────────────
    # C++ graph watcher integration
    # ──────────────────────────────────────────────

    def _on_graph_changed(self, _msg: EmptyMsg):
        """Debounced callback fired by the C++ osiris_graph_watcher node."""
        self.get_logger().debug("[graph] event received")
        self._trigger_graph_poll()

    def _trigger_graph_poll(self):
        """Single debounced entry point for all graph poll triggers.

        Resets a one-shot timer on every call so rapid bursts coalesce
        into a single poll.
        """
        if self._graph_debounce_timer is not None:
            self._graph_debounce_timer.cancel()
        _interval = self.get_parameter('graph_debounce_interval').get_parameter_value().double_value
        self._graph_debounce_timer = threading.Timer(_interval, self._debounce_fire)
        self._graph_debounce_timer.daemon = True
        self._graph_debounce_timer.start()

    def _debounce_fire(self):
        """Called from threading.Timer — run the graph poll directly."""
        self.get_logger().debug("[graph] watcher triggered poll")
        self._check_graph_changes()

    # ──────────────────────────────────────────────
    # Parameters (async, lazy-loaded)
    # ──────────────────────────────────────────────

    def _do_startup_check(self):
        """One-shot timer: run the initial graph scan then cancel itself."""
        self._startup_check_timer.cancel()
        if self._watcher_proc is not None:
            rc = self._watcher_proc.poll()
            if rc is not None:
                self.get_logger().error(
                    f"[graph] graph_watcher exited unexpectedly (rc={rc}) — "
                    "no graph events will be received"
                )
            else:
                self.get_logger().info(
                    f"[graph] graph_watcher healthy (pid={self._watcher_proc.pid})"
                )
        if not self._first_graph_check_done:
            self._check_graph_changes()

    def _cancel_param_fetch_timer(self):
        """Cancel the one-shot delayed param-fetch timer after it fires."""
        t = self._param_fetch_timer
        if t is not None:
            t.cancel()
            self._param_fetch_timer = None

    def _on_parameter_event(self, msg: ParameterEvent):
        """React to parameter changes published by any node on /parameter_events."""
        fqn = msg.node
        if fqn not in self._active_nodes:
            return
        cache = dict(self._node_parameter_cache.get(fqn) or {})
        for param in list(msg.new_parameters) + list(msg.changed_parameters):
            try:
                cache[param.name] = parameter_value_to_python(param.value)
            except Exception:
                pass
        for param in msg.deleted_parameters:
            cache.pop(param.name, None)
        if cache != self._node_parameter_cache.get(fqn):
            self._node_parameter_cache[fqn] = cache
            self.get_logger().debug(f'[params] updated {len(cache)} params for {fqn} via /parameter_events')
            self._trigger_graph_poll()

    def _fetch_action_types(self, action_name: str, topic_type_map: dict) -> bool:
        """Resolve and cache goal/result/feedback types for an action server.

        Looks up the [action]/_action/feedback topic type, strips the
        _FeedbackMessage suffix to derive the base action type, then imports
        the _Goal / _Result / _Feedback message classes to introspect fields.

        Returns True if types were resolved and enqueued, False if not yet
        available (e.g. the feedback topic hasn't appeared in DDS yet).
        """
        if action_name in self._action_type_cache:
            return self._action_type_cache[action_name] is not None

        feedback_topic = f'{action_name}/_action/feedback'
        types_list = topic_type_map.get(feedback_topic)
        if not types_list:
            return False

        feedback_msg_type = types_list[0]
        if not feedback_msg_type.endswith('_FeedbackMessage'):
            return False

        base_type = feedback_msg_type[:-len('_FeedbackMessage')]

        try:
            # base_type is e.g. 'nav2_msgs/action/NavigateToPose'
            # Action sub-types (Goal/Result/Feedback) are nested on the action
            # class itself — get_message() only handles message types, not actions.
            import importlib
            pkg, _, action_name_part = base_type.split('/', 2)
            # action_name_part may be 'action/NavigateToPose' — take just the class name
            class_name = action_name_part.split('/')[-1]
            action_mod = importlib.import_module(f'{pkg}.action')
            action_cls = getattr(action_mod, class_name)
            # Use get_message() for the _FeedbackMessage type — it IS a standalone message
            # type (unlike _Goal/_Result/_Feedback which are nested). The type string
            # comes directly from the topic registry so it's always correct.
            feedback_msg_cls = get_message(feedback_msg_type)

            goal_cls     = action_cls.Goal
            result_cls   = action_cls.Result
            feedback_cls = action_cls.Feedback

            def _fields(cls):
                try:
                    return dict(cls.get_fields_and_field_types())
                except Exception:
                    return {}

            type_info = {
                'goal_type':       f'{base_type}_Goal',
                'result_type':     f'{base_type}_Result',
                'feedback_type':   f'{base_type}_Feedback',
                'goal_fields':     _fields(goal_cls),
                'result_fields':   _fields(result_cls),
                'feedback_fields': _fields(feedback_cls),
                # Wire message type (not the same string as feedback_type
                # above — that's the nested class name, this is the
                # standalone _FeedbackMessage actually subscribed to) kept
                # so a later goals_enabled toggle-on can catch up the
                # feedback subscription without redoing type resolution —
                # see _catch_up_action_feedback.
                '_feedback_msg_type': feedback_msg_type,
            }
            self._action_type_cache[action_name] = type_info
            self.get_logger().info(f'[actions] resolved types for {action_name}: {base_type}')
            self._graph_dirty = True
            if self._goals_enabled:
                self._subscribe_action_feedback(action_name, feedback_msg_cls)
            return True
        except Exception as e:
            self.get_logger().warning(f'[actions] failed to resolve types for {action_name}: {e}')
            self._action_type_cache[action_name] = None  # mark failed — avoid retry spam
            return False

    def _fetch_node_parameters_async(self, fqn: str):
        """Fetch parameters for *fqn* without blocking the executor.

        Creates service clients, fires async calls, and stores results in
        _node_parameter_cache when callbacks fire.  Safe to call from any
        timer or graph-change callback.
        """
        if fqn in self._pending_param_fetches:
            return

        list_client = self.create_client(ListParameters, f"{fqn}/list_parameters")
        if not list_client.service_is_ready():
            self.destroy_client(list_client)
            self._nodes_no_param_service.add(fqn)
            return

        self._pending_param_fetches.add(fqn)
        req = ListParameters.Request()
        req.depth = 10
        future = list_client.call_async(req)

        def _on_list(fut):
            self.destroy_client(list_client)
            response = fut.result()
            if response is None or not response.result.names:
                self._pending_param_fetches.discard(fqn)
                self._node_parameter_cache[fqn] = {}  # fetched but empty — stop retrying
                return
            param_names = list(response.result.names)
            get_client = self.create_client(GetParameters, f"{fqn}/get_parameters")
            get_req = GetParameters.Request()
            get_req.names = param_names
            get_future = get_client.call_async(get_req)

            def _on_get(gfut):
                self.destroy_client(get_client)
                self._pending_param_fetches.discard(fqn)
                get_resp = gfut.result()
                if get_resp is None:
                    return
                params = {}
                for name, value in zip(param_names, get_resp.values):
                    try:
                        params[name] = parameter_value_to_python(value)
                    except Exception:
                        pass
                self._node_parameter_cache[fqn] = params
                self._graph_dirty = True
                self.get_logger().debug(f"[params] cached {len(params)} params for {fqn}")

            get_future.add_done_callback(_on_get)

        future.add_done_callback(_on_list)

    # ──────────────────────────────────────────────
    # Telemetry
    # ──────────────────────────────────────────────

    def _apply_agent_config_fallback(self):
        self._agent_config_fallback_timer.cancel()
        if self._agent_config_received:
            return
        self.get_logger().warning('No agent_config received within timeout — applying local param defaults')
        self._apply_agent_config({})

    def _resolve_config_value(self, name, config, local_default, cast=None):
        """All-or-nothing per agent run, not a per-field override: passing
        --params-file at all is a deliberate choice to run off that yaml
        file, so every field resolves from it (yaml value if the file sets
        this one, else its hardcoded declare_parameter default) and the
        cloud agent_config is ignored entirely for the whole run — not just
        for the specific fields the yaml file happens to set. Without a
        params file, every field resolves from the cloud config if present,
        else the hardcoded default. self._param_overrides being non-empty at
        all (regardless of which names it contains) is what decides which
        source every field uses — the two sources never mix within a run.
        """
        if self._param_overrides:
            return local_default
        if name in config:
            return cast(config[name]) if cast else config[name]
        return local_default

    def _apply_agent_config(self, config: dict) -> None:
        """Applies the gateway-pushed feature-toggle config (sent right after
        auth_success on every connect, and by _apply_agent_config_fallback if
        that never arrives). Per-field precedence: local yaml/CLI override >
        cloud agent_config > hardcoded default — see _resolve_config_value.
        A robot with nothing set in agent_config yet, and no yaml override
        either, behaves exactly as it did before this existed. Single
        dispatch point so each new toggle (Graph, Params, Nav2, Goals, BT,
        TF Tree) has one place to land rather than scattering config reads
        across the file.
        """
        self._agent_config_received = True

        self._telemetry_enabled = self._resolve_config_value('telemetry_enabled', config, self._telemetry_enabled_default, bool)

        # TF tree: resolve the final answer, then construct/destroy the
        # collector at most once to reach it — never both in the same pass.
        tf_tree_enabled = self._resolve_config_value('tf_tree_enabled', config, self._tf_tree_enabled_default, bool)
        if tf_tree_enabled and self._tf_tree is None:
            self._tf_tree = TfTreeCollector(
                node=self,
                event_callback=self._on_tf_tree_event,
                logger=self.get_logger(),
            )
            self.get_logger().info('TF tree monitoring started')
        elif not tf_tree_enabled and self._tf_tree is not None:
            self._tf_tree.destroy()
            self._tf_tree = None
            self.get_logger().info('TF tree monitoring stopped')

        # TF tree poll timer: independent of the collector above — _poll_tf_tree
        # is a no-op whenever self._tf_tree is None, so the timer's period is
        # resolved and (re)created here regardless of tf_tree_enabled, exactly
        # like telemetry's own timer runs unconditionally.
        tf_tree_poll_interval = self._resolve_config_value('tf_tree_poll_interval', config, self._tf_tree_poll_interval_default, float)
        if self._tf_tree_poll_timer is None or self._tf_tree_poll_interval != tf_tree_poll_interval:
            if self._tf_tree_poll_timer is not None:
                self._tf_tree_poll_timer.cancel()
            self._tf_tree_poll_timer = self.create_timer(tf_tree_poll_interval, self._poll_tf_tree)
            self._tf_tree_poll_interval = tf_tree_poll_interval

        # ros2_control: same construct/destroy-at-most-once pattern as TF
        # tree, plus the same reconnect-if-changed handling as bt_conn below
        # — poll_interval is a constructor arg baked into the collector
        # (rate-limit check in Ros2ControlCollector.poll()), not re-read
        # live, so a changed interval while already enabled needs an actual
        # destroy+reconstruct to ever take effect.
        ros2_control_enabled = self._resolve_config_value('ros2_control_enabled', config, self._ros2_control_enabled_default, bool)
        ros2_control_poll_interval = self._resolve_config_value('ros2_control_poll_interval', config, self._ros2_control_poll_interval_default, float)
        if ros2_control_enabled and (self._ros2_control is None or self._ros2_control_poll_interval != ros2_control_poll_interval):
            if self._ros2_control is not None:
                self._ros2_control.destroy()
            self._ros2_control = Ros2ControlCollector(
                node=self,
                event_callback=self._on_ros2_control_event,
                logger=self.get_logger(),
                poll_interval=ros2_control_poll_interval,
            )
            self._ros2_control_poll_interval = ros2_control_poll_interval
            self.get_logger().info('ros2_control monitoring started')
        elif not ros2_control_enabled and self._ros2_control is not None:
            self._ros2_control.destroy()
            self._ros2_control = None
            self.get_logger().info('ros2_control monitoring stopped')

        # Battery topic: the subscription's topic name is a constructor arg,
        # so it's (re)created here whenever the resolved topic changes.
        battery_topic = self._resolve_config_value('battery_topic', config, self._battery_topic_default)
        if self._battery_sub is None or self._battery_topic != battery_topic:
            if self._battery_sub is not None:
                self.destroy_subscription(self._battery_sub)
                self._battery_sub = None
            try:
                from sensor_msgs.msg import BatteryState as BatteryStateMsg
                self._battery_sub = self.create_subscription(
                    BatteryStateMsg, battery_topic,
                    self._on_battery_state, 10,
                )
                self.get_logger().info(f'Battery state subscription active on {battery_topic}')
            except Exception as e:
                self.get_logger().warning(f'Battery state monitoring unavailable: {e}')
            self._battery_topic = battery_topic

        self._battery_check_enabled = self._resolve_config_value('battery_check_enabled', config, self._battery_check_enabled_default, bool)
        self._battery_min_percent = self._resolve_config_value('battery_min_percent', config, self._battery_min_percent_default, float)

        # Dock status topic — same (re)subscribe-on-change pattern as battery
        # above, and subscribed whenever a topic is configured regardless of
        # dock_check_enabled (same as battery_sub existing independently of
        # battery_check_enabled) — so the cache is already warm by the time
        # the operator flips the check on, instead of guaranteeing an initial
        # dock_status_unknown rejection while waiting for the first message.
        # lazily imported since opennav_docking_msgs is Nav2-docking-specific,
        # not guaranteed installed on every robot (same reasoning as
        # nav2_msgs elsewhere).
        self._dock_check_enabled = self._resolve_config_value('dock_check_enabled', config, self._dock_check_enabled_default, bool)
        dock_status_topic = self._resolve_config_value('dock_status_topic', config, self._dock_status_topic_default)
        if self._dock_status_sub is None or self._dock_status_topic != dock_status_topic:
            if self._dock_status_sub is not None:
                self.destroy_subscription(self._dock_status_sub)
                self._dock_status_sub = None
                self._last_dock_status = None
                self._last_dock_status_time = None
            if dock_status_topic:
                try:
                    from opennav_docking_msgs.msg import DockStatus
                    self._dock_status_sub = self.create_subscription(
                        DockStatus, dock_status_topic,
                        self._on_dock_status, 10,
                    )
                    self.get_logger().info(f'Dock status subscription active on {dock_status_topic}')
                except Exception as e:
                    self.get_logger().warning(f'Dock status monitoring unavailable: {e}')
            self._dock_status_topic = dock_status_topic

        # Goals: no single collector object to construct/destroy — just a set
        # of per-action subscriptions. Turning on catches up on every action
        # already known (self._active_actions, populated by the graph scan
        # regardless of this flag); turning off tears all of them down.
        goals_enabled = self._resolve_config_value('goals_enabled', config, self._goals_enabled_default, bool)
        if goals_enabled and not self._goals_enabled:
            for a in self._active_actions:
                self._subscribe_action_status(a)
                self._catch_up_action_feedback(a)
            self.get_logger().info(f'Goal tracking started ({len(self._active_actions)} action(s))')
        elif not goals_enabled and self._goals_enabled:
            for a in list(set(self._action_status_subs) | set(self._action_feedback_subs)):
                self._unsubscribe_action_monitoring(a)
            self.get_logger().info('Goal tracking stopped')
        self._goals_enabled = goals_enabled

        # Params: no collector/subscription to construct/destroy either — just
        # a cache. Turning on catches up on every node already known
        # (self._active_nodes, populated by the graph scan regardless of this
        # flag); turning off clears the cache so stale values don't keep
        # showing in the Params pane after the user asked this to stop.
        params_enabled = self._resolve_config_value('params_enabled', config, self._params_enabled_default, bool)
        if params_enabled and not self._params_enabled:
            for fqn in self._active_nodes:
                self._fetch_node_parameters_async(fqn)
            self.get_logger().info(f'Param fetching started ({len(self._active_nodes)} node(s))')
        elif not params_enabled and self._params_enabled:
            self._node_parameter_cache.clear()
            self._pending_param_fetches.clear()
            self._graph_dirty = True
            self.get_logger().info('Param fetching stopped')
        self._params_enabled = params_enabled

        # BT: mutually exclusive by construction — Nav2 BT and BT.CPP share
        # the same event pipeline (_on_bt_event/_cached_bt_tree_event, no
        # source tagging), so only one may ever be active. Tear down
        # whichever isn't the resolved mode before starting the other, so a
        # switch never briefly has both running. BT.CPP additionally
        # reconnects if its host/port changed while already in btcpp mode —
        # a running BTCollector is a live ZMQ connection bound to whatever
        # host/port it was constructed with.
        bt_mode = self._resolve_config_value('bt_mode', config, self._bt_mode_default)
        bt_host = self._resolve_config_value('bt_host', config, self._bt_host_default)
        bt_server_port = self._resolve_config_value('bt_server_port', config, self._bt_server_port_default, int)
        bt_publisher_port = self._resolve_config_value('bt_publisher_port', config, self._bt_publisher_port_default, int)
        bt_conn = (bt_host, bt_server_port, bt_publisher_port)

        if bt_mode != 'nav2' and self._nav2_bt_monitor_initialized:
            self._teardown_nav2_bt_monitor()
        if bt_mode != 'btcpp' and self._bt_collector is not None:
            self._bt_collector.stop()
            self._bt_collector = None
            self._bt_collector_conn = None

        if bt_mode == 'nav2' and not self._nav2_bt_monitor_initialized:
            self._init_nav2_bt_monitor()
        elif bt_mode == 'btcpp' and (self._bt_collector is None or self._bt_collector_conn != bt_conn):
            if self._bt_collector is not None:
                self._bt_collector.stop()
            self._bt_collector = BTCollector(
                event_callback=self._on_bt_event,
                host=bt_host,
                server_port=bt_server_port,
                publisher_port=bt_publisher_port,
                logger=self.get_logger(),
            )
            self._bt_collector.start()
            self._bt_collector_conn = bt_conn
            self.get_logger().info(f'BT.CPP monitoring started ({bt_host}:{bt_server_port}/{bt_publisher_port})')

        self._bt_mode = bt_mode

        # bag_output_dir / graph_debounce_interval / topic_data_rate_hz /
        # topic_data_max_bytes_per_sec / global_topic_data_max_bytes_per_sec /
        # global_topic_data_max_msgs_per_sec / image_jpeg_quality /
        # image_max_dimension: every consumer already calls
        # self.get_parameter(...) fresh at time of use (a plain path string
        # re-read on each bag list/download/record; a plain float re-read
        # into a brand-new threading.Timer on every debounce trigger, not a
        # fixed recurring ROS timer; a plain float/int re-read on every topic
        # message in _on_topic_msg/_topic_data_over_budget) — so there's
        # nothing to construct or defer here, just update the underlying ROS
        # param when overridden. Same all-or-nothing rule as
        # _resolve_config_value: a yaml params file being present at all
        # (regardless of whether it sets these specific fields) means the
        # cloud config is skipped for all eight — the ROS param already holds
        # the yaml-or-hardcoded value and is simply left untouched.
        if not self._param_overrides and 'bag_output_dir' in config:
            self.set_parameters([Parameter('bag_output_dir', Parameter.Type.STRING, str(config['bag_output_dir']))])
        if not self._param_overrides and 'graph_debounce_interval' in config:
            self.set_parameters([Parameter('graph_debounce_interval', Parameter.Type.DOUBLE, float(config['graph_debounce_interval']))])
        if not self._param_overrides and 'topic_data_rate_hz' in config:
            self.set_parameters([Parameter('topic_data_rate_hz', Parameter.Type.DOUBLE, float(config['topic_data_rate_hz']))])
        if not self._param_overrides and 'topic_data_max_bytes_per_sec' in config:
            self.set_parameters([Parameter('topic_data_max_bytes_per_sec', Parameter.Type.DOUBLE, float(config['topic_data_max_bytes_per_sec']))])
        if not self._param_overrides and 'global_topic_data_max_bytes_per_sec' in config:
            self.set_parameters([Parameter('global_topic_data_max_bytes_per_sec', Parameter.Type.DOUBLE, float(config['global_topic_data_max_bytes_per_sec']))])
        if not self._param_overrides and 'global_topic_data_max_msgs_per_sec' in config:
            self.set_parameters([Parameter('global_topic_data_max_msgs_per_sec', Parameter.Type.DOUBLE, float(config['global_topic_data_max_msgs_per_sec']))])
        if not self._param_overrides and 'image_jpeg_quality' in config:
            self.set_parameters([Parameter('image_jpeg_quality', Parameter.Type.INTEGER, int(config['image_jpeg_quality']))])
        if not self._param_overrides and 'image_max_dimension' in config:
            self.set_parameters([Parameter('image_max_dimension', Parameter.Type.INTEGER, int(config['image_max_dimension']))])

        # topic_limits: per-topic {rate_hz, max_bytes_per_sec} overrides of
        # the two topic_data_* defaults above (see _topic_limit_overrides,
        # _on_topic_msg, _topic_data_over_budget). Not a set_parameters call
        # like the six scalars above — ROS2 declared parameters are a flat,
        # known-in-advance namespace, a poor fit for a dynamic topic-name-
        # keyed map — so this is plain instance state instead, consulted
        # fresh on every topic message exactly like the scalar params are.
        # Same all-or-nothing yaml-override rule: a params-file run clears
        # it rather than leaving stale overrides from a previous cloud push.
        with self._topic_limit_lock:
            if self._param_overrides:
                self._topic_limit_overrides = {}
            elif 'topic_limits' in config:
                self._topic_limit_overrides = dict(config['topic_limits'] or {})
            # Snapshot for the resolved_agent_config ground-truth send below —
            # taken under the same lock rather than re-read there unguarded.
            _topic_limits_snapshot = dict(self._topic_limit_overrides)

        # Deliberately includes every topic_data/image field, not just the
        # five original toggles above — this line predated the bandwidth-cap
        # and JPEG re-encode work and was never extended, so a live push that
        # only touched e.g. image_jpeg_quality logged nothing to distinguish
        # it from a no-op call. Same silent-config smell _rate_throttle_drop_logged
        # / _budget_drop_logged were added for; read fresh off the ROS params
        # (not local vars) so this is accurate regardless of which fields this
        # particular call actually touched.
        self.get_logger().info(
            f'Applied agent_config: telemetry_enabled={self._telemetry_enabled}, '
            f'tf_tree_enabled={tf_tree_enabled}, goals_enabled={goals_enabled}, '
            f'params_enabled={params_enabled}, bt_mode={bt_mode}, '
            f'topic_data_rate_hz={self.get_parameter("topic_data_rate_hz").get_parameter_value().double_value:.1f}, '
            f'topic_data_max_bytes_per_sec={self.get_parameter("topic_data_max_bytes_per_sec").get_parameter_value().double_value:.0f}, '
            f'global_topic_data_max_bytes_per_sec={self.get_parameter("global_topic_data_max_bytes_per_sec").get_parameter_value().double_value:.0f}, '
            f'global_topic_data_max_msgs_per_sec={self.get_parameter("global_topic_data_max_msgs_per_sec").get_parameter_value().double_value:.0f}, '
            f'image_jpeg_quality={self.get_parameter("image_jpeg_quality").get_parameter_value().integer_value}, '
            f'image_max_dimension={self.get_parameter("image_max_dimension").get_parameter_value().integer_value}, '
            f'topic_limits={_topic_limits_snapshot}'
        )

        # Ground truth for the client: what this agent is ACTUALLY running
        # with right now, for every field — as opposed to the cloud
        # agent_config, which can be completely irrelevant (yaml-file runs
        # ignore it outright) or simply stale until this agent reconnects.
        # Sent on every _apply_agent_config call (every connect/reconnect,
        # and the 5s fallback), so the client always has a current answer,
        # never a guess based on pane data being empty. bag_output_dir and
        # graph_debounce_interval are read fresh here rather than reusing a
        # local var — they're the two fields actually pushed through
        # set_parameters() above rather than tracked as plain instance
        # state, so this is the one place their resolved value lives.
        #
        # yaml_override is metadata about the SOURCE these values were
        # resolved from, not a config value itself — a yaml/CLI params file
        # being passed at all means the client shouldn't claim a restart
        # will sync Cloud Config (it never will, as long as that file keeps
        # getting passed); it should instead tell the operator how to
        # actually switch back to cloud-driven config.
        self._enqueue({
            'type': 'resolved_agent_config',
            'yaml_override': bool(self._param_overrides),
            'config': {
                'telemetry_enabled': self._telemetry_enabled,
                'goals_enabled': goals_enabled,
                'params_enabled': params_enabled,
                'tf_tree_enabled': tf_tree_enabled,
                'tf_tree_poll_interval': tf_tree_poll_interval,
                'ros2_control_enabled': ros2_control_enabled,
                'ros2_control_poll_interval': ros2_control_poll_interval,
                'battery_topic': battery_topic,
                'battery_check_enabled': self._battery_check_enabled,
                'battery_min_percent': self._battery_min_percent,
                'dock_check_enabled': self._dock_check_enabled,
                'dock_status_topic': dock_status_topic,
                'bt_mode': bt_mode,
                'bt_host': bt_host,
                'bt_server_port': bt_server_port,
                'bt_publisher_port': bt_publisher_port,
                'bag_output_dir': self.get_parameter('bag_output_dir').get_parameter_value().string_value,
                'graph_debounce_interval': self.get_parameter('graph_debounce_interval').get_parameter_value().double_value,
                'topic_data_rate_hz': self.get_parameter('topic_data_rate_hz').get_parameter_value().double_value,
                'topic_data_max_bytes_per_sec': self.get_parameter('topic_data_max_bytes_per_sec').get_parameter_value().double_value,
                'global_topic_data_max_bytes_per_sec': self.get_parameter('global_topic_data_max_bytes_per_sec').get_parameter_value().double_value,
                'global_topic_data_max_msgs_per_sec': self.get_parameter('global_topic_data_max_msgs_per_sec').get_parameter_value().double_value,
                'image_jpeg_quality': self.get_parameter('image_jpeg_quality').get_parameter_value().integer_value,
                'image_max_dimension': self.get_parameter('image_max_dimension').get_parameter_value().integer_value,
                'topic_limits': _topic_limits_snapshot,
            },
            'timestamp': time.time(),
        })

    def _collect_telemetry(self):
        if not self.ws or not self.loop:
            return
        if not self._telemetry_enabled:
            return
        self._telemetry_tick += 1
        include_processes = (self._telemetry_tick % TELEMETRY_PROCESS_EVERY_N_TICKS == 0)
        self._enqueue({
            'type': 'telemetry',
            'data': self._get_telemetry_snapshot(include_processes=include_processes),
            'timestamp': time.time(),
        })

    def _on_battery_state(self, msg) -> None:
        """Cache the latest BatteryState message for inclusion in telemetry snapshots
        and for the send_command battery-level precondition check."""
        try:
            self._last_battery_state = {
                'percent':  round(float(msg.percentage) * 100.0, 1) if msg.percentage == msg.percentage else None,  # NaN guard
                'voltage':  round(float(msg.voltage), 3)  if msg.voltage  == msg.voltage  else None,
                'current':  round(float(msg.current), 3)  if msg.current  == msg.current  else None,
                'status':   int(msg.power_supply_status),
                'present':  bool(msg.present),
            }
            self._last_battery_state_time = time.time()
        except Exception:
            pass

    def _on_dock_status(self, msg) -> None:
        """Cache the latest opennav_docking_msgs/DockStatus message for the
        send_command docked-status precondition check."""
        try:
            self._last_dock_status = {
                'is_docked':   bool(msg.is_docked),
                'is_charging': bool(msg.is_charging),
            }
            self._last_dock_status_time = time.time()
        except Exception:
            pass

    def _get_telemetry_snapshot(self, include_processes: bool = True) -> dict:
        cpu_now = round(psutil.cpu_percent(interval=None), 1)

        vm = psutil.virtual_memory()
        ram_percent = vm.percent

        now = time.time()
        disk_usage      = psutil.disk_usage('/')
        disk_read_mbps  = 0.0
        disk_write_mbps = 0.0
        try:
            disk_io = psutil.disk_io_counters()
            if self._last_disk_io is not None and self._last_io_time is not None:
                dt = now - self._last_io_time
                if dt > 0:
                    disk_read_mbps  = round(max(0.0, (disk_io.read_bytes  - self._last_disk_io.read_bytes)  / dt / (1024 * 1024)), 2)
                    disk_write_mbps = round(max(0.0, (disk_io.write_bytes - self._last_disk_io.write_bytes) / dt / (1024 * 1024)), 2)
            self._last_disk_io = disk_io
        except Exception:
            pass

        net_tx_mbps = 0.0
        net_rx_mbps = 0.0
        try:
            net_io = psutil.net_io_counters()
            if self._last_net_io is not None and self._last_io_time is not None:
                dt = now - self._last_io_time
                if dt > 0:
                    net_tx_mbps = round(max(0.0, (net_io.bytes_sent - self._last_net_io.bytes_sent) / dt / (1024 * 1024)), 2)
                    net_rx_mbps = round(max(0.0, (net_io.bytes_recv - self._last_net_io.bytes_recv) / dt / (1024 * 1024)), 2)
            self._last_net_io = net_io
        except Exception:
            pass

        self._last_io_time = now

        cpu_c = None
        try:
            temps = psutil.sensors_temperatures()
            for key in ('coretemp', 'cpu-thermal', 'acpitz', 'k10temp', 'cpu_thermal'):
                entries = temps.get(key)
                if entries:
                    cpu_c = round(entries[0].current, 1)
                    break
        except Exception:
            pass

        # CPU frequency (GHz)
        cpu_freq = None
        try:
            freq = psutil.cpu_freq()
            if freq and freq.current:
                cpu_freq = round(freq.current / 1000.0, 2)
        except Exception:
            pass

        # Load averages (1, 5, 15 min)
        cpu_load = None
        try:
            load1, load5, load15 = os.getloadavg()
            cpu_load = {
                'load1':  round(load1, 1),
                'load5':  round(load5, 1),
                'load15': round(load15, 1),
            }
        except Exception:
            pass

        # Process list (top processes by CPU usage).
        # Two-phase fetch: cheap fields for all processes first, then only
        # pull the more expensive fields (cmdline, memory_info, username) for
        # the top N CPU consumers, to avoid per-second syscalls against every
        # process on the host and to avoid leaking the full host process list
        # (cmdline can contain secrets) over the wire.
        #
        # psutil.Process.cpu_percent() is normalized to a SINGLE core by
        # default (100% = one core fully saturated, so a process pegging 2 of
        # 4 cores reports ~200%) — a different convention than cpu_now above,
        # which psutil already normalizes to the whole system (0-100%). Divide
        # by logical core count here so both numbers in this same pane mean
        # the same thing: percent of total system CPU capacity.
        try:
            logical_cpus = psutil.cpu_count(logical=True) or 1
        except Exception:
            logical_cpus = 1

        # Skipped entirely (not just omitted from the result) on ticks that
        # don't need it — this is the expensive part TELEMETRY_PROCESS_EVERY_N_
        # TICKS exists to avoid paying every second, not just a smaller payload.
        processes = []
        if include_processes:
            try:
                candidates = []
                for proc in psutil.process_iter(['pid', 'name', 'cpu_percent']):
                    try:
                        info = proc.info
                        candidates.append((round((info['cpu_percent'] or 0.0) / logical_cpus, 1), proc))
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                candidates.sort(key=lambda c: c[0], reverse=True)

                for cpu_percent, proc in candidates[:MAX_TELEMETRY_PROCESSES]:
                    try:
                        with proc.oneshot():
                            cmdline = proc.cmdline()
                            mem_info = proc.memory_info()
                            processes.append({
                                'pid':          proc.pid,
                                'name':         proc.name(),
                                'cmdline':      ' '.join(cmdline)[:256] if cmdline else '',
                                'num_threads':  proc.num_threads(),
                                'username':     proc.username(),
                                'memory_mb':    round(mem_info.rss / (1024 * 1024), 1) if mem_info else 0,
                                'cpu_percent':  cpu_percent,
                            })
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            except Exception:
                pass

        snapshot = {
            'cpu': {
                'now':        cpu_now,
                'throttling': None,
                'temp':       cpu_c,
                'freq':       cpu_freq,
                'load':       cpu_load,
            },
            'ram': {
                'percent':  round(ram_percent, 1),
                'used_mb':  round(vm.used  / (1024 * 1024), 1),
                'total_mb': round(vm.total / (1024 * 1024), 1),
            },
            'disk': {
                'percent':    round(disk_usage.percent, 1),
                'used_gb':    round(disk_usage.used  / (1024 ** 3), 2),
                'total_gb':   round(disk_usage.total / (1024 ** 3), 2),
                'read_mbps':  disk_read_mbps,
                'write_mbps': disk_write_mbps,
            },
            'net': {
                'tx_mbps': net_tx_mbps,
                'rx_mbps': net_rx_mbps,
            },
            'battery':   self._last_battery_state,
        }
        # Present only on ticks that actually computed it — absent, not an
        # empty list, so the client can tell "no update this tick" apart from
        # "genuinely no processes" and hold onto whatever it already has
        # (see stores/robot.js's telemetry handler) instead of blanking the
        # UI for the 4 out of 5 ticks this is skipped.
        if include_processes:
            snapshot['processes'] = processes
        return snapshot

    def _get_cpu_model(self) -> str | None:
        try:
            if os.path.exists('/proc/cpuinfo'):
                with open('/proc/cpuinfo') as f:
                    for line in f:
                        if line.lower().startswith(('model name', 'hardware', 'processor')):
                            _, value = line.split(':', 1)
                            value = value.strip()
                            if value:
                                return value
        except Exception:
            pass

        cpu_model = platform.processor() or platform.machine()
        return cpu_model or None

    def _get_robot_model(self) -> str | None:
        for env_name in ('OSIRIS_ROBOT_MODEL', 'ROBOT_MODEL'):
            value = os.environ.get(env_name)
            if value:
                return value

        for path in ('/proc/device-tree/model', '/sys/firmware/devicetree/base/model'):
            try:
                if os.path.exists(path):
                    with open(path, 'rb') as f:
                        value = f.read().decode(errors='ignore').strip('\x00\n ')
                        if value:
                            return value
            except Exception:
                pass
        return None

    def _get_initial_state_meta(self, telemetry: dict | None = None) -> dict:
        ram_total_mb = None
        try:
            ram_total_mb = telemetry.get('ram', {}).get('total_mb') if telemetry else None
            if ram_total_mb is None:
                ram_total_mb = round(psutil.virtual_memory().total / (1024 * 1024), 1)
        except Exception:
            pass

        return {
            'agentVersion': AGENT_VERSION,
            'ros_distro': os.environ.get('ROS_DISTRO'),
            'cpu_model': self._get_cpu_model(),
            'cpu_cores': psutil.cpu_count(logical=False),
            'cpu_threads': psutil.cpu_count(logical=True),
            'ram_total_mb': ram_total_mb,
            'arch': platform.machine() or None,
            'robot_model': self._get_robot_model(),
        }

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    @staticmethod
    def _node_full_name(name: str, namespace: str) -> str:
        ns = namespace if namespace.endswith('/') else namespace + '/'
        return ns + name

    @staticmethod
    def _qos_to_dict(qos) -> dict | None:
        if not qos:
            return None
        return {
            'reliability': qos.reliability.name if hasattr(qos.reliability, 'name') else str(qos.reliability),
            'durability':  qos.durability.name  if hasattr(qos.durability,  'name') else str(qos.durability),
            'history':     qos.history.name     if hasattr(qos.history,     'name') else str(qos.history),
            'depth':       qos.depth,
            'liveliness':  qos.liveliness.name  if hasattr(qos.liveliness,  'name') else str(qos.liveliness),
        }

    def _enqueue(self, payload: dict):
        """Thread-safe enqueue to the asyncio send queue."""
        if self.ws and self.loop:
            asyncio.run_coroutine_threadsafe(
                self._send_queue.put(json.dumps(payload)),
                self.loop,
            )

    async def _put_pair(self, header: str, payload: bytes):
        # Two sequential puts on the *same* coroutine, not two separate
        # _enqueue calls. _send_queue is unbounded, so asyncio.Queue.put()
        # never actually suspends (see its source: the full() wait loop is
        # skipped entirely) — these two puts land back-to-back with no
        # window for another thread's run_coroutine_threadsafe call to slip
        # a different message in between. That matters here specifically:
        # the gateway pairs this header with whichever binary frame arrives
        # right after it, so the two must never be split by an unrelated
        # topic's message landing between them.
        await self._send_queue.put(header)
        await self._send_queue.put(payload)

    def _enqueue_binary_topic_data(self, header: dict, payload: bytes):
        """Like _enqueue, but for a topic_data message whose byte-array field
        was pulled out into a raw binary WS frame (see _on_topic_msg) — sends
        the JSON header and that frame as one atomic pair. See _put_pair."""
        if self.ws and self.loop:
            asyncio.run_coroutine_threadsafe(
                self._put_pair(json.dumps(header), payload),
                self.loop,
            )

    # ──────────────────────────────────────────────
    # Collector event handlers
    # ──────────────────────────────────────────────

    def _poll_tf_tree(self):
        """Periodic 1 Hz timer callback to keep tf_tree updates flowing."""
        if self._tf_tree is not None:
            self._tf_tree.poll()

    def _on_ros2_control_event(self, event: dict):
        self._enqueue(event)

    def _on_tf_tree_event(self, event: dict):
        self._enqueue(event)

    def _on_bt_event(self, event: dict):
        if event.get('type') == 'bt_tree':
            self._cached_bt_tree_event = event if event.get('tree_id') else None
        self._enqueue(event)

    # ──────────────────────────────────────────────
    # Nav2 BT monitoring
    # ──────────────────────────────────────────────

    def _init_nav2_bt_monitor(self):
        try:
            from nav2_msgs.msg import BehaviorTreeLog
            from action_msgs.msg import GoalStatusArray
            self._nav2_bt_statuses:           dict[str, str] = {}
            self._nav2_bt_last_sent_statuses:  dict[str, str] = {}  # what client currently has
            self._nav2_bt_session_active   = False
            self._nav2_bt_publisher_active = False
            self._nav2_bt_tree_id          = None
            self._nav2_bt_tree_structure   = None
            self._nav2_bt_nodes_list:      list = []
            self._nav2_bt_name_to_uid:     dict = {}
            self._nav2_bt_log_sub = self.create_subscription(
                BehaviorTreeLog, '/behavior_tree_log', self._on_nav2_bt_log, 10
            )
            self._nav2_bt_goal_status_sub = self.create_subscription(
                GoalStatusArray,
                '/navigate_to_pose/_action/status',
                self._on_nav2_goal_status,
                10,
            )
            self._nav2_bt_monitor_initialized = True
            # If bt_navigator is already publishing, pre-parse the XML so
            # the startup bt_state event includes the tree structure.
            if self.count_publishers('/behavior_tree_log') > 0:
                self._nav2_bt_publisher_active = True
                self._load_and_parse_bt_xml()
        except Exception as e:
            self.get_logger().debug(f"Nav2 BT monitoring unavailable: {e}")

    def _teardown_nav2_bt_monitor(self):
        if not self._nav2_bt_monitor_initialized:
            return
        self._on_nav2_bt_gone()  # notifies client the tree is gone, resets tree-state fields
        self.destroy_subscription(self._nav2_bt_log_sub)
        self.destroy_subscription(self._nav2_bt_goal_status_sub)
        self._nav2_bt_log_sub = None
        self._nav2_bt_goal_status_sub = None
        self._nav2_bt_monitor_initialized = False
        self.get_logger().info('Nav2 BT monitoring stopped')

    def _load_and_parse_bt_xml(self) -> bool:
        if self._nav2_bt_tree_id is not None:
            return True
        import hashlib
        import xml.etree.ElementTree as ET

        xml_path = self._node_parameter_cache.get('/bt_navigator', {}).get(
            'default_nav_to_pose_bt_xml', ''
        )
        if not xml_path:
            try:
                from ament_index_python.packages import get_package_share_directory
                nav2_share = get_package_share_directory('nav2_bt_navigator')
                xml_path = os.path.join(
                    nav2_share, 'behavior_trees',
                    'navigate_to_pose_w_replanning_and_recovery.xml',
                )
            except Exception:
                return False

        try:
            with open(xml_path) as f:
                xml_content = f.read()
        except Exception as e:
            self.get_logger().error(f"Cannot read BT XML '{xml_path}': {e}")
            return False

        try:
            root_elem = ET.fromstring(xml_content)
            bt_elem = root_elem.find('.//BehaviorTree')
            if bt_elem is None:
                return False

            nodes_list: list = []
            name_to_uid: dict = {}
            uid_counter = [1]

            def elem_to_node(elem):
                name = elem.attrib.get('name', elem.attrib.get('ID', elem.tag))
                uid = uid_counter[0]; uid_counter[0] += 1
                name_to_uid[name] = uid
                nodes_list.append({'uid': uid, 'name': name, 'tag': elem.tag})
                node = {
                    'tag': elem.tag, 'name': name, 'uid': uid,
                    'attributes': dict(elem.attrib),
                }
                kids = [elem_to_node(c) for c in elem]
                if kids:
                    node['children'] = kids
                return node

            bt_children = list(bt_elem)
            tree_structure = elem_to_node(bt_children[0]) if bt_children else {}
            self._nav2_bt_tree_structure = tree_structure
            self._nav2_bt_nodes_list     = nodes_list
            self._nav2_bt_name_to_uid    = name_to_uid
            self._nav2_bt_tree_id = hashlib.sha1(xml_content.encode()).hexdigest()[:16]
            return True
        except Exception as e:
            self.get_logger().error(f"Failed to parse BT XML: {e}")
            return False

    def _on_nav2_bt_log(self, msg):
        if not self._nav2_bt_publisher_active:
            return
        if not self._load_and_parse_bt_xml():
            return

        # Collapse all transitions in this log tick to the final status per node.
        # The event_log can contain multiple entries for the same node (e.g.
        # RUNNING → FAILURE → IDLE) — only the last one matters to the client.
        has_running = False
        final_per_node: dict[str, str] = {}  # node_name → final status this tick
        for change in msg.event_log:
            self._nav2_bt_statuses[change.node_name] = change.current_status
            final_per_node[change.node_name] = change.current_status
            if change.current_status == 'RUNNING':
                has_running = True

        if has_running and not self._nav2_bt_session_active:
            self.get_logger().info("[bt] navigation session started")
            self._nav2_bt_session_active = True
            # Full tree send — sync last-sent cache
            self._nav2_bt_last_sent_statuses = {
                nd['name']: self._nav2_bt_statuses.get(nd['name'], 'IDLE')
                for nd in self._nav2_bt_nodes_list
            }
            self._on_bt_event({
                'type': 'bt_tree', 'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'tree': self._nav2_bt_tree_structure,
                'nodes': [
                    {**nd, 'status': self._nav2_bt_last_sent_statuses.get(nd['name'], 'IDLE')}
                    for nd in self._nav2_bt_nodes_list
                ],
            })
            return  # full tree already sent; skip bt_status this tick

        # Only send nodes whose final status this tick differs from what client has
        changes = []
        for node_name, status in final_per_node.items():
            if self._nav2_bt_last_sent_statuses.get(node_name) == status:
                continue
            uid = self._nav2_bt_name_to_uid.get(node_name)
            if uid is not None:
                changes.append({'uid': uid, 'name': node_name, 'tag': '', 'status': status})
                self._nav2_bt_last_sent_statuses[node_name] = status

        if changes:
            self._on_bt_event({
                'type': 'bt_status', 'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'changes': changes,
            })

    def _on_nav2_goal_status(self, msg):
        has_active = any(s.status in (1, 2, 3) for s in msg.status_list)
        if self._nav2_bt_session_active and not has_active:
            self.get_logger().info("[bt] navigation session ended")
            self._nav2_bt_session_active = False
            self._nav2_bt_statuses.clear()
            if self._nav2_bt_tree_id:
                self._on_bt_event({
                    'type': 'bt_tree', 'timestamp': time.time(),
                    'tree_id': self._nav2_bt_tree_id,
                    'tree': self._nav2_bt_tree_structure,
                    'nodes': [{**nd, 'status': 'IDLE'} for nd in self._nav2_bt_nodes_list],
                })

    def _on_nav2_bt_gone(self):
        if self._nav2_bt_tree_id is None and not self._nav2_bt_publisher_active:
            return
        self.get_logger().info("Nav2 BT gone — clearing BT state")
        self._nav2_bt_publisher_active = False
        self._nav2_bt_session_active   = False
        self._nav2_bt_statuses.clear()
        self._nav2_bt_last_sent_statuses.clear()
        self._nav2_bt_tree_id          = None
        self._nav2_bt_tree_structure   = None
        self._nav2_bt_nodes_list       = []
        self._nav2_bt_name_to_uid      = {}
        self._on_bt_event({
            'type': 'bt_tree', 'timestamp': time.time(),
            'tree_id': None, 'tree': None, 'nodes': [],
        })

    def _build_startup_bt_state_event(self) -> dict:
        src = self._cached_bt_tree_event
        if src:
            return {
                'type': 'bt_state', 'timestamp': src.get('timestamp', time.time()),
                'tree_id': src.get('tree_id'), 'tree': src.get('tree'),
                'nodes': src.get('nodes', []),
                'blackboard': src.get('blackboard'),
            }
        if (
            self._nav2_bt_monitor_initialized
            and self._nav2_bt_tree_id
            and self._nav2_bt_tree_structure
        ):
            return {
                'type': 'bt_state', 'timestamp': time.time(),
                'tree_id': self._nav2_bt_tree_id,
                'tree': self._nav2_bt_tree_structure,
                'nodes': [
                    {**nd, 'status': self._nav2_bt_statuses.get(nd['name'], 'IDLE')}
                    for nd in self._nav2_bt_nodes_list
                ],
                'blackboard': None,
            }
        return {
            'type': 'bt_state', 'timestamp': time.time(),
            'tree_id': None, 'tree': None, 'nodes': [], 'blackboard': None,
        }

    def _bt_snapshot_from_state_event(self, src: dict) -> dict:
        return {
            'timestamp': src.get('timestamp', time.time()),
            'tree_id': src.get('tree_id'),
            'tree': src.get('tree'),
            'nodes': src.get('nodes', []),
            'blackboard': src.get('blackboard'),
        }

    # ──────────────────────────────────────────────
    # Cleanup
    # ──────────────────────────────────────────────

    def destroy_node(self):
        # Cancel pending debounce timer so it doesn't fire after shutdown.
        if self._graph_debounce_timer is not None:
            try:
                self._graph_debounce_timer.cancel()
            except Exception:
                pass
            self._graph_debounce_timer = None
        if self._tf_tree is not None:
            self._tf_tree.destroy()
        if self._bt_collector:
            self._bt_collector.stop()
        with self._bag_lock:
            bag_proc = self._bag_proc
            self._bag_proc = None
            self._bag_output_path = None
        if bag_proc is not None and bag_proc.poll() is None:
            try:
                bag_proc.send_signal(signal.SIGINT)
                bag_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                bag_proc.kill()
                try:
                    bag_proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
        super().destroy_node()


def main(args=None):
    import shutil
    import stat
    import subprocess
    import time
    import importlib.resources

    def _is_elf(path):
        # Reject non-ELF binaries (e.g. a macOS Mach-O that accidentally ended
        # up in the PyPI wheel) before trying to run them.
        try:
            with open(path, 'rb') as f:
                return f.read(4) == b'\x7fELF'
        except Exception:
            return False

    def _try_launch(path):
        """Spawn a candidate binary and verify it doesn't die immediately.
        An ABI/symbol mismatch against whatever rclcpp is actually loaded
        (e.g. `symbol lookup error: undefined symbol: ...`) surfaces as an
        exit within milliseconds, so a short grace period is enough to tell
        a working binary from a broken one. Returns the live Popen on
        success, or None (having logged why) on early exit."""
        try:
            os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            proc = subprocess.Popen([path], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except OSError as e:
            print(f'[osiris] graph_watcher candidate {path} could not be started: {e}', flush=True)
            return None
        time.sleep(0.5)
        rc = proc.poll()
        if rc is not None:
            try:
                err = proc.stderr.read().decode(errors='replace').strip()
            except Exception:
                err = ''
            print(
                f'[osiris] graph_watcher candidate {path} exited immediately (rc={rc})'
                + (f': {err}' if err else ''),
                flush=True,
            )
            return None
        return proc

    # Locate the graph_watcher binary:
    # 1. Prefer PATH (colcon dev workspace with source install/setup.bash)
    # 2. Otherwise try bundled binaries in preference order, actually
    #    launching each and keeping the first one that survives the startup
    #    grace period above:
    #      a. distro+arch-specific   bin/graph_watcher_{arch}_{distro}
    #         (e.g. graph_watcher_aarch64_lyrical, graph_watcher_x86_64_jazzy)
    #      b. any other distro-tagged binary for this arch — covers the case
    #         where $ROS_DISTRO wasn't set/exported, or names a distro we
    #         haven't built for, but another build happens to be ABI-compatible
    #      c. arch-only              bin/graph_watcher_{arch}
    #      d. bin/graph_watcher      (legacy / colcon-installed generic name)
    _watcher_proc = None
    _watcher_bin = shutil.which('graph_watcher')

    if _watcher_bin is not None:
        if sys.platform != 'linux':
            import logging
            logging.getLogger(__name__).warning(
                f"graph_watcher is a Linux binary and cannot run on {sys.platform} — "
                "graph events will not be available."
            )
            _watcher_bin = None
        elif not _is_elf(_watcher_bin):
            import logging
            logging.getLogger(__name__).error(
                f"osiris_graph_watcher binary at '{_watcher_bin}' is not a Linux ELF "
                "(wrong platform — was a macOS binary published by mistake?). "
                "Graph events will not be available."
            )
            _watcher_bin = None
        else:
            _watcher_proc = _try_launch(_watcher_bin)
            if _watcher_proc is None:
                _watcher_bin = None

    if _watcher_bin is None and sys.platform == 'linux':
        try:
            _arch = platform.machine()  # 'x86_64' or 'aarch64'
            _distro = os.environ.get('ROS_DISTRO', '')  # e.g. 'humble', 'jazzy', 'lyrical'
            _bin_dir = importlib.resources.files('osiris_agent').joinpath('bin')

            _candidates = []
            if _distro:
                _candidates.append(f'graph_watcher_{_arch}_{_distro}')
            try:
                for _entry in sorted(_bin_dir.iterdir(), key=lambda p: p.name):  # type: ignore[attr-defined]
                    if _entry.name.startswith(f'graph_watcher_{_arch}_') and _entry.name not in _candidates:
                        _candidates.append(_entry.name)
            except Exception:
                pass
            _candidates += [f'graph_watcher_{_arch}', 'graph_watcher']

            for _name in _candidates:
                _candidate = _bin_dir.joinpath(_name)
                if not _candidate.is_file():  # type: ignore[attr-defined]
                    continue
                _candidate = str(_candidate)
                if not _is_elf(_candidate):
                    continue
                _proc = _try_launch(_candidate)
                if _proc is not None:
                    _watcher_bin = _candidate
                    _watcher_proc = _proc
                    break
        except Exception:
            _watcher_bin = None

    if _watcher_proc is not None:
        print(f'[osiris] graph_watcher started: {_watcher_bin} (pid={_watcher_proc.pid})', flush=True)
    else:
        import logging
        logging.getLogger(__name__).warning(
            "osiris_graph_watcher not found or failed to start — graph events will not be available."
        )

    rclpy.init(args=args)
    node = WebBridge(watcher_proc=_watcher_proc)

    # Forward graph_watcher stderr to the ROS logger so crashes are visible.
    # Suppress rclcpp signal-handler lines (pure noise) and fall back to plain
    # stderr once rclpy is shutting down to avoid publishing on an invalid context.
    if _watcher_proc and _watcher_proc.stderr:
        def _forward_watcher_stderr():
            ros_log = node.get_logger()
            for line in _watcher_proc.stderr:
                decoded = line.decode().rstrip()
                if not decoded:
                    continue
                # rclcpp prints these on SIGINT/SIGTERM — informational noise.
                if 'signal_handler(' in decoded:
                    continue
                if rclpy.ok():
                    try:
                        ros_log.info(f'[gw] {decoded}')
                        continue
                    except Exception:
                        pass
                print(f'[gw] {decoded}', file=sys.stderr, flush=True)
        threading.Thread(target=_forward_watcher_stderr, daemon=True).start()

    try:
        rclpy.spin(node)
    except (KeyboardInterrupt, rclpy.executors.ExternalShutdownException):
        pass
    finally:
        # Stop the C++ watcher first so its stderr pipe closes and the
        # forwarder thread exits before we tear down the rclpy context.
        if _watcher_proc is not None:
            try:
                _watcher_proc.terminate()
                _watcher_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                _watcher_proc.kill()
                try:
                    _watcher_proc.wait(timeout=2)
                except Exception:
                    pass
            except Exception:
                pass
        try:
            node.destroy_node()
        except Exception:
            pass
        if rclpy.ok():
            try:
                rclpy.shutdown()
            except Exception:
                pass


if __name__ == '__main__':
    main()
    