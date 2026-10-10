"""
===============================================================================
  SMART CITY RTOS - REAL-TIME MISSION CONTROL DASHBOARD
  QNX Neutrino RTOS Telemetry & IPC Analytics Monitor
  Built with CustomTkinter & Matplotlib
===============================================================================
"""

import json
import math
import random
import socket
import sys
import threading
import time
from collections import defaultdict, deque

# pyrefly: ignore [missing-import]
import customtkinter as ctk
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ----------------- Matplotlib Dark Theme Setup -----------------
matplotlib.use("TkAgg")
plt.style.use("dark_background")

# ----------------- Configuration & Defaults -----------------
DEFAULT_PORT = 5005
HISTORY_LEN = 90             # Rolling sample history for time-series charts
NODE_TIMEOUT = 3.0           # Seconds without telemetry packet -> Node marked DOWN
CHART_UPDATE_INTERVAL_MS = 600  # GUI chart refresh rate (ms)

# Sci-fi / Cyberpunk HUD Color Palette
NODE_PALETTE = [
    "#00F0FF",  # Neon Cyan (Node 0)
    "#A855F7",  # Electric Purple (Node 1)
    "#FF007A",  # Neon Pink / Magenta (Node 2)
    "#00FF88",  # Cyber Green (Node 3)
    "#FFB800",  # Amber Gold (Node 4)
    "#38BDF8",  # Sky Blue (Node 5)
]

NODE_ROLES = {
    0: "Traffic & V2X Gateway",
    1: "Power Grid & Sensors",
    2: "Emergency Services & Transit",
    3: "Water & Environmental Telemetry",
}

STATUS_COLORS = {
    "HEALTHY": "#00FF88",   # Neon Green
    "DEGRADED": "#FFB800",  # Neon Amber
    "CRITICAL": "#FF0055",  # Neon Red
    "DOWN": "#64748B",      # Muted Slate
}

# ----------------- Global Shared State & Synchronization -----------------
data_lock = threading.Lock()
history = defaultdict(lambda: {
    "t": deque(maxlen=HISTORY_LEN),
    "cpu": deque(maxlen=HISTORY_LEN),
    "cpu_avg": deque(maxlen=HISTORY_LEN),
    "ipc": deque(maxlen=HISTORY_LEN),
    "ipc_avg": deque(maxlen=HISTORY_LEN),
    "health": deque(maxlen=HISTORY_LEN),
})
latest_node_data = {}          # node_id -> latest telemetry payload
last_seen_time = {}            # node_id -> local timestamp (time.time())
last_fault_state = defaultdict(lambda: "NONE")
event_log = deque(maxlen=200)  # High-capacity system audit log
packet_count = 0
packet_rate = 0.0
last_rate_calc_time = time.time()
rate_packet_counter = 0

sim_active = False
sim_thread = None
receiver_running = True
receiver_socket = None


def get_node_color(node_id):
    try:
        idx = int(node_id)
    except (ValueError, TypeError):
        idx = sum(ord(c) for c in str(node_id))
    return NODE_PALETTE[idx % len(NODE_PALETTE)]


def get_health_state(node_id, now):
    if node_id not in last_seen_time or (now - last_seen_time[node_id] > NODE_TIMEOUT):
        return "DOWN"
    m = latest_node_data.get(node_id, {})
    fault = m.get("fault", "NONE")
    health = m.get("health", 100.0)
    task = m.get("task", "RUNNING")
    
    if fault != "NONE" or health < 40.0 or task == "FAILED":
        return "CRITICAL"
    if health < 75.0 or task == "BLOCKED":
        return "DEGRADED"
    return "HEALTHY"


# ----------------- Telemetry Data Processor -----------------
def process_telemetry(payload):
    global packet_count, rate_packet_counter
    now = time.time()
    
    if not isinstance(payload, dict) or "node" not in payload:
        return

    n = payload["node"]
    cpu = float(payload.get("cpu", 0.0))
    cpu_avg = float(payload.get("cpu_avg", cpu))
    ipc_us = float(payload.get("ipc_us", 0.0))
    ipc_avg = float(payload.get("ipc_avg", ipc_us))
    health = float(payload.get("health", 100.0))
    fault = str(payload.get("fault", "NONE"))
    task = str(payload.get("task", "RUNNING"))
    hb = int(payload.get("hb", 0))
    hb_age_ms = float(payload.get("hb_age_ms", 0.0))

    with data_lock:
        packet_count += 1
        rate_packet_counter += 1
        
        h = history[n]
        h["t"].append(now)
        h["cpu"].append(cpu)
        h["cpu_avg"].append(cpu_avg)
        h["ipc"].append(ipc_us)
        h["ipc_avg"].append(ipc_avg)
        h["health"].append(health)

        latest_node_data[n] = {
            "node": n,
            "ts": payload.get("ts", int(now * 1000)),
            "cpu": cpu,
            "cpu_avg": cpu_avg,
            "ipc_us": ipc_us,
            "ipc_avg": ipc_avg,
            "task": task,
            "hb": hb,
            "hb_age_ms": hb_age_ms,
            "fault": fault,
            "health": health,
        }
        last_seen_time[n] = now

        # Evaluate fault state transitions for logging
        old_f = last_fault_state[n]
        if fault != old_f:
            stamp = time.strftime("%H:%M:%S")
            role = NODE_ROLES.get(n, f"Subsystem {n}")
            if fault != "NONE":
                event_log.appendleft({
                    "time": stamp,
                    "node": n,
                    "type": "FAULT",
                    "text": f"[{stamp}] NODE {n} ({role}) ALARM: {fault} [Task: {task}, CPU: {cpu:.1f}%, IPC: {ipc_us:.0f}µs]",
                    "level": "CRITICAL"
                })
            else:
                event_log.appendleft({
                    "time": stamp,
                    "node": n,
                    "type": "RECOVERY",
                    "text": f"[{stamp}] NODE {n} ({role}) RECOVERED -> Normal Operations Resumed",
                    "level": "HEALTHY"
                })
            last_fault_state[n] = fault


# ----------------- UDP Receiver Thread -----------------
def udp_receiver_thread(port):
    global receiver_socket, receiver_running
    try:
        receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Allow immediate reuse of address
        receiver_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        receiver_socket.bind(("0.0.0.0", port))
        receiver_socket.settimeout(1.0)
        print(f"[NET] Telemetry UDP receiver bound successfully to 0.0.0.0:{port}")
        
        event_log.appendleft({
            "time": time.strftime("%H:%M:%S"),
            "node": -1,
            "type": "SYSTEM",
            "text": f"[{time.strftime('%H:%M:%S')}] UDP Telemetry listener active on 0.0.0.0:{port}",
            "level": "INFO"
        })
    except Exception as e:
        print(f"[ERROR] Failed to bind UDP port {port}: {e}")
        event_log.appendleft({
            "time": time.strftime("%H:%M:%S"),
            "node": -1,
            "type": "ERROR",
            "text": f"[{time.strftime('%H:%M:%S')}] UDP Bind Error on Port {port}: {e}",
            "level": "CRITICAL"
        })
        return

    while receiver_running:
        try:
            data, _ = receiver_socket.recvfrom(4096)
            msg_str = data.decode("utf-8", errors="replace").strip()
            # Handle potential multiple JSON lines per UDP frame
            for line in msg_str.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    process_telemetry(payload)
                except json.JSONDecodeError:
                    pass
        except socket.timeout:
            continue
        except Exception as e:
            if receiver_running:
                time.sleep(0.1)


# ----------------- Embedded QNX RTOS Simulator Mode -----------------
def run_simulation():
    """Generates synthetic QNX RTOS telemetry matching monitor.c behavior."""
    global sim_active
    nodes_state = {}
    for i in range(4):
        nodes_state[i] = {
            "base_cpu": 18.0 + i * 12.0,
            "cpu": 20.0 + i * 10.0,
            "cpu_avg": 20.0 + i * 10.0,
            "ipc_us": 320.0 + i * 150.0,
            "ipc_avg": 320.0 + i * 150.0,
            "hb": 100,
            "last_hb_ms": int(time.time() * 1000),
            "health": 100.0,
        }

    while sim_active:
        t_now = time.time()
        now_ms = int(t_now * 1000)
        sec = int(t_now)

        for i in range(4):
            st = nodes_state[i]
            # Random walk CPU
            st["base_cpu"] += random.uniform(-2.5, 2.5)
            st["base_cpu"] = max(5.0, min(55.0, st["base_cpu"]))
            
            shown_cpu = st["base_cpu"]
            task = "RUNNING"
            send_hb = True
            ipc = max(180.0, st["ipc_us"] + random.uniform(-40, 40))

            # Realistic fault injection schedule matching node_sim.c
            if i == 1 and (sec % 35) < 7:
                send_hb = False      # Heartbeat timeout
            if i == 2 and (sec % 45) < 6:
                shown_cpu = 95.5     # CPU overload
            if i == 0 and (sec % 60) < 5:
                task = "FAILED"      # Task failure
            elif i == 0 and 5 <= (sec % 60) < 8:
                task = "BLOCKED"     # Task blocked
            if i == 3 and (sec % 50) >= 30 and (sec % 50) < 36:
                ipc = 2450.0         # IPC latency spike

            if send_hb:
                st["hb"] += 1
                st["last_hb_ms"] = now_ms

            # Analytics simulation
            st["cpu"] = shown_cpu
            st["cpu_avg"] = 0.2 * shown_cpu + 0.8 * st["cpu_avg"]
            st["ipc_us"] = ipc
            st["ipc_avg"] = 0.2 * ipc + 0.8 * st["ipc_avg"]

            hb_age = now_ms - st["last_hb_ms"]
            fault = "NONE"
            if hb_age > 3000:
                fault = "HEARTBEAT_LOST"
            elif task == "FAILED":
                fault = "TASK_FAILED"
            elif shown_cpu > 90.0:
                fault = "CPU_OVERLOAD"
            elif ipc > 2000.0:
                fault = "IPC_LATENCY"

            # Health calculation
            h = 100.0
            if st["cpu_avg"] > 50.0:
                h -= (st["cpu_avg"] - 50.0) * 0.8
            if st["ipc_avg"] > 500.0:
                h -= (st["ipc_avg"] - 500.0) / 50.0
            if task == "BLOCKED":
                h -= 15.0
            if fault != "NONE":
                h -= 40.0
            st["health"] = max(0.0, min(100.0, h))

            payload = {
                "node": i,
                "ts": now_ms,
                "cpu": round(shown_cpu, 1),
                "cpu_avg": round(st["cpu_avg"], 1),
                "ipc_us": round(ipc, 1),
                "ipc_avg": round(st["ipc_avg"], 1),
                "task": task,
                "hb": st["hb"],
                "hb_age_ms": hb_age,
                "fault": fault,
                "health": round(st["health"], 1),
            }
            process_telemetry(payload)

        time.sleep(1.0)


# ----------------- Main GUI Application Class -----------------
class SmartCityDashboard(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("SMART CITY RTOS // MISSION CONTROL & TELEMETRY CENTER")
        self.geometry("1500x950")
        self.minsize(1200, 800)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.configure(fg_color="#07090E")

        self.node_cards = {}
        self.selected_node_filter = "ALL"
        self.log_filter = "ALL"
        self.last_rendered_event_count = -1

        self.setup_ui_layout()
        self.setup_header()
        self.setup_main_panels()
        self.setup_charts()
        self.setup_logs_panel()

        # Start periodic GUI refresh loops
        self.after(300, self.update_dashboard)
        self.after(1000, self.calc_network_rates)

    def setup_ui_layout(self):
        self.grid_columnconfigure(0, weight=3)  # Left panel (Node telemetry cards & controls)
        self.grid_columnconfigure(1, weight=7)  # Right panel (Real-time telemetry plots)
        self.grid_rowconfigure(0, weight=0)     # Header banner & KPI counters
        self.grid_rowconfigure(1, weight=6)     # Main dashboard area
        self.grid_rowconfigure(2, weight=3)     # Bottom System Audit Logs

    # ----------------- Top Header & System KPI Bar -----------------
    def setup_header(self):
        self.header_frame = ctk.CTkFrame(self, fg_color="#0D111A", corner_radius=10, border_width=1, border_color="#1E293B")
        self.header_frame.grid(row=0, column=0, columnspan=2, sticky="ew", padx=12, pady=(10, 6))

        # Brand Title
        title_box = ctk.CTkFrame(self.header_frame, fg_color="transparent")
        title_box.pack(side="left", padx=16, pady=8)

        lbl_logo = ctk.CTkLabel(
            title_box, 
            text="⚡ SMART CITY RTOS", 
            font=ctk.CTkFont(family="Segoe UI", size=19, weight="bold"), 
            text_color="#00F0FF"
        )
        lbl_logo.pack(anchor="w")

        lbl_sub = ctk.CTkLabel(
            title_box, 
            text="QNX Neutrino 7.1 • Fault Tolerant Distributed Node Analytics", 
            font=ctk.CTkFont(family="Segoe UI", size=11), 
            text_color="#64748B"
        )
        lbl_sub.pack(anchor="w")

        # Right Controls: Mode Toggle & Status Badges
        ctrl_box = ctk.CTkFrame(self.header_frame, fg_color="transparent")
        ctrl_box.pack(side="right", padx=16, pady=8)

        self.lbl_stream_status = ctk.CTkLabel(
            ctrl_box,
            text="● LISTENING [UDP: 5005]",
            font=ctk.CTkFont(family="Segoe UI", size=12, weight="bold"),
            text_color="#00FF88",
            fg_color="#06281E",
            corner_radius=6,
            padx=10,
            pady=4
        )
        self.lbl_stream_status.pack(side="right", padx=6)

        self.btn_sim_toggle = ctk.CTkButton(
            ctrl_box,
            text="START DEMO SIMULATOR",
            command=self.toggle_simulator,
            fg_color="#1E293B",
            hover_color="#334155",
            text_color="#00F0FF",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
            border_width=1,
            border_color="#00F0FF",
            height=30
        )
        self.btn_sim_toggle.pack(side="right", padx=6)

        # Quick KPIs: Active Nodes, CPU Avg, System Health
        kpi_box = ctk.CTkFrame(self.header_frame, fg_color="transparent")
        kpi_box.pack(side="right", padx=20, pady=8)

        self.kpi_nodes = self.create_mini_kpi(kpi_box, "ONLINE NODES", "0 / 0", "#00F0FF")
        self.kpi_health = self.create_mini_kpi(kpi_box, "SYSTEM HEALTH", "100%", "#00FF88")
        self.kpi_traffic = self.create_mini_kpi(kpi_box, "THROUGHPUT", "0 pkts/s", "#A855F7")

    def create_mini_kpi(self, parent, label, init_val, color):
        frame = ctk.CTkFrame(parent, fg_color="#131B2A", corner_radius=6, border_width=1, border_color="#1E293B")
        frame.pack(side="left", padx=5)
        lbl_title = ctk.CTkLabel(frame, text=label, font=ctk.CTkFont(family="Segoe UI", size=9, weight="bold"), text_color="#64748B")
        lbl_title.pack(padx=8, pady=(4, 0))
        lbl_val = ctk.CTkLabel(frame, text=init_val, font=ctk.CTkFont(family="Segoe UI", size=13, weight="bold"), text_color=color)
        lbl_val.pack(padx=8, pady=(0, 4))
        return lbl_val

    # ----------------- Main Layout Panels -----------------
    def setup_main_panels(self):
        # Left Panel (Nodes List)
        self.left_panel = ctk.CTkFrame(self, fg_color="#0D111A", corner_radius=10, border_width=1, border_color="#1E293B")
        self.left_panel.grid(row=1, column=0, sticky="nsew", padx=(12, 6), pady=6)
        self.left_panel.grid_rowconfigure(1, weight=1)
        self.left_panel.grid_columnconfigure(0, weight=1)

        # Header for Left Panel
        left_header = ctk.CTkFrame(self.left_panel, fg_color="transparent")
        left_header.grid(row=0, column=0, sticky="ew", padx=12, pady=(10, 6))

        lbl_nodes_title = ctk.CTkLabel(
            left_header, 
            text="SUBSYSTEM TELEMETRY NODES", 
            font=ctk.CTkFont(family="Segoe UI", size=14, weight="bold"), 
            text_color="#F8FAFC"
        )
        lbl_nodes_title.pack(side="left")

        # Scrollable Nodes Container
        self.nodes_scroll = ctk.CTkScrollableFrame(self.left_panel, fg_color="#090C13", corner_radius=8)
        self.nodes_scroll.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))

        # Right Panel (Charts)
        self.right_panel = ctk.CTkFrame(self, fg_color="#0D111A", corner_radius=10, border_width=1, border_color="#1E293B")
        self.right_panel.grid(row=1, column=1, sticky="nsew", padx=(6, 12), pady=6)
        self.right_panel.grid_rowconfigure(0, weight=1)
        self.right_panel.grid_columnconfigure(0, weight=1)

    # ----------------- Real-time Matplotlib Charts -----------------
    def setup_charts(self):
        # Figure with 2 subplots: CPU usage on top, IPC Latency below
        self.fig, (self.ax_cpu, self.ax_ipc) = plt.subplots(
            2, 1, 
            figsize=(9, 6.5), 
            facecolor="#0D111A"
        )
        self.fig.subplots_adjust(left=0.08, right=0.97, top=0.93, bottom=0.09, hspace=0.35)

        self.canvas = FigureCanvasTkAgg(self.fig, master=self.right_panel)
        self.canvas.get_tk_widget().pack(fill=ctk.BOTH, expand=True, padx=8, pady=8)

        # Custom canvas background
        self.canvas.get_tk_widget().configure(bg="#0D111A")

    # ----------------- System Event Audit Logs Panel -----------------
    def setup_logs_panel(self):
        self.bottom_panel = ctk.CTkFrame(self, fg_color="#0D111A", corner_radius=10, border_width=1, border_color="#1E293B")
        self.bottom_panel.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=12, pady=(6, 10))
        self.bottom_panel.grid_rowconfigure(1, weight=1)
        self.bottom_panel.grid_columnconfigure(0, weight=1)

        # Header for Logs
        logs_header = ctk.CTkFrame(self.bottom_panel, fg_color="transparent")
        logs_header.grid(row=0, column=0, sticky="ew", padx=12, pady=(8, 4))

        lbl_logs = ctk.CTkLabel(
            logs_header, 
            text="SYSTEM AUDIT & FAULT EVENT LOGS", 
            font=ctk.CTkFont(family="Segoe UI", size=13, weight="bold"), 
            text_color="#F8FAFC"
        )
        lbl_logs.pack(side="left")

        btn_clear = ctk.CTkButton(
            logs_header,
            text="CLEAR LOGS",
            command=self.clear_logs,
            width=80,
            height=24,
            fg_color="#1E293B",
            hover_color="#334155",
            font=ctk.CTkFont(family="Segoe UI", size=10, weight="bold")
        )
        btn_clear.pack(side="right", padx=4)

        # Logs Text Display with syntax tags
        self.log_textbox = ctk.CTkTextbox(
            self.bottom_panel,
            fg_color="#090C13",
            text_color="#E2E8F0",
            font=ctk.CTkFont(family="Consolas", size=11),
            corner_radius=8,
            border_width=1,
            border_color="#1E293B"
        )
        self.log_textbox.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 8))
        self.log_textbox.configure(state="disabled")

    # ----------------- Interactive Node Cards -----------------
    def create_or_update_node_card(self, node_id, data, state, now):
        color = get_node_color(node_id)
        role = NODE_ROLES.get(node_id, f"RTOS Subsystem {node_id}")

        if node_id not in self.node_cards:
            card = ctk.CTkFrame(self.nodes_scroll, fg_color="#131B2A", corner_radius=8, border_width=1, border_color="#1E293B")
            card.pack(fill="x", padx=4, pady=5)

            # Top Header Line: Node ID + Role + State Badge
            header_f = ctk.CTkFrame(card, fg_color="transparent")
            header_f.pack(fill="x", padx=10, pady=(8, 2))

            lbl_id = ctk.CTkLabel(
                header_f, 
                text=f"NODE #{node_id}", 
                font=ctk.CTkFont(family="Segoe UI", size=14, weight="bold"), 
                text_color=color
            )
            lbl_id.pack(side="left")

            lbl_role = ctk.CTkLabel(
                header_f,
                text=f" • {role}",
                font=ctk.CTkFont(family="Segoe UI", size=11),
                text_color="#94A3B8"
            )
            lbl_role.pack(side="left", padx=4)

            badge_state = ctk.CTkLabel(
                header_f,
                text=state,
                font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
                text_color="#000000",
                fg_color=STATUS_COLORS.get(state, "#64748B"),
                corner_radius=4,
                padx=8,
                pady=2
            )
            badge_state.pack(side="right")

            # Health Meter Row
            health_f = ctk.CTkFrame(card, fg_color="transparent")
            health_f.pack(fill="x", padx=10, pady=2)
            lbl_h_title = ctk.CTkLabel(health_f, text="Health Score:", font=ctk.CTkFont(size=11), text_color="#94A3B8")
            lbl_h_title.pack(side="left")
            
            lbl_h_val = ctk.CTkLabel(health_f, text="100%", font=ctk.CTkFont(size=11, weight="bold"), text_color="#00FF88")
            lbl_h_val.pack(side="right")

            pbar_health = ctk.CTkProgressBar(card, height=6, fg_color="#1E293B", progress_color="#00FF88")
            pbar_health.pack(fill="x", padx=10, pady=(0, 6))
            pbar_health.set(1.0)

            # Metrics Grid (CPU, IPC, Task, Heartbeat)
            grid_f = ctk.CTkFrame(card, fg_color="#0D111A", corner_radius=6)
            grid_f.pack(fill="x", padx=8, pady=(0, 6))
            grid_f.grid_columnconfigure((0, 1), weight=1)

            lbl_cpu = ctk.CTkLabel(grid_f, text="CPU: --", font=ctk.CTkFont(family="Consolas", size=11), text_color="#E2E8F0")
            lbl_cpu.grid(row=0, column=0, padx=8, pady=3, sticky="w")

            lbl_ipc = ctk.CTkLabel(grid_f, text="IPC: --", font=ctk.CTkFont(family="Consolas", size=11), text_color="#E2E8F0")
            lbl_ipc.grid(row=0, column=1, padx=8, pady=3, sticky="w")

            lbl_task = ctk.CTkLabel(grid_f, text="Task: --", font=ctk.CTkFont(family="Consolas", size=11), text_color="#E2E8F0")
            lbl_task.grid(row=1, column=0, padx=8, pady=3, sticky="w")

            lbl_hb = ctk.CTkLabel(grid_f, text="HB: --", font=ctk.CTkFont(family="Consolas", size=11), text_color="#E2E8F0")
            lbl_hb.grid(row=1, column=1, padx=8, pady=3, sticky="w")

            # Fault Alert Indicator
            lbl_fault = ctk.CTkLabel(
                card, 
                text="FAULT: NONE", 
                font=ctk.CTkFont(family="Segoe UI", size=11, weight="bold"),
                text_color="#00F0FF"
            )
            lbl_fault.pack(anchor="w", padx=10, pady=(2, 6))

            self.node_cards[node_id] = {
                "card": card,
                "badge_state": badge_state,
                "lbl_h_val": lbl_h_val,
                "pbar_health": pbar_health,
                "lbl_cpu": lbl_cpu,
                "lbl_ipc": lbl_ipc,
                "lbl_task": lbl_task,
                "lbl_hb": lbl_hb,
                "lbl_fault": lbl_fault,
            }

        # Update card UI components
        c = self.node_cards[node_id]
        state_color = STATUS_COLORS.get(state, "#64748B")
        c["badge_state"].configure(
            text=state, 
            fg_color=state_color,
            text_color="#000000" if state != "DOWN" else "#FFFFFF"
        )
        c["card"].configure(border_color=color if state != "DOWN" else "#334155")

        if state == "DOWN":
            c["lbl_h_val"].configure(text="OFFLINE", text_color="#64748B")
            c["pbar_health"].set(0.0)
            c["pbar_health"].configure(progress_color="#64748B")
            c["lbl_fault"].configure(text="STATUS: NODE UNREACHABLE (TIMEOUT)", text_color="#64748B")
        else:
            cpu = data.get("cpu", 0.0)
            cpu_avg = data.get("cpu_avg", cpu)
            ipc = data.get("ipc_us", 0.0)
            ipc_avg = data.get("ipc_avg", ipc)
            task = data.get("task", "RUNNING")
            hb = data.get("hb", 0)
            health = data.get("health", 100.0)
            fault = data.get("fault", "NONE")

            # Health
            c["lbl_h_val"].configure(text=f"{health:.1f}%", text_color=state_color)
            c["pbar_health"].set(max(0.0, min(1.0, health / 100.0)))
            c["pbar_health"].configure(progress_color=state_color)

            # Task State Color
            task_col = "#00FF88" if task == "RUNNING" else ("#FFB800" if task == "BLOCKED" else "#FF0055")
            c["lbl_task"].configure(text=f"Task: {task}", text_color=task_col)

            # CPU & IPC
            cpu_col = "#FF0055" if cpu > 90.0 else "#E2E8F0"
            c["lbl_cpu"].configure(text=f"CPU: {cpu:.1f}% (μ:{cpu_avg:.1f}%)", text_color=cpu_col)

            ipc_col = "#FF0055" if ipc > 2000.0 else "#E2E8F0"
            c["lbl_ipc"].configure(text=f"IPC: {ipc:.0f}µs (μ:{ipc_avg:.0f})", text_color=ipc_col)

            c["lbl_hb"].configure(text=f"HB: #{hb}")

            # Fault
            if fault != "NONE":
                c["lbl_fault"].configure(text=f"ALARM: {fault}", text_color="#FF0055")
            else:
                c["lbl_fault"].configure(text="FAULT: NONE (OK)", text_color="#00FF88")

    # ----------------- Rate & Performance Counters -----------------
    def calc_network_rates(self):
        global packet_rate, rate_packet_counter, last_rate_calc_time
        now = time.time()
        dt = now - last_rate_calc_time
        if dt >= 1.0:
            packet_rate = rate_packet_counter / dt
            rate_packet_counter = 0
            last_rate_calc_time = now
            self.kpi_traffic.configure(text=f"{packet_rate:.1f} pkts/s")
        self.after(1000, self.calc_network_rates)

    # ----------------- Toggle Simulator Button -----------------
    def toggle_simulator(self):
        global sim_active, sim_thread
        if not sim_active:
            sim_active = True
            sim_thread = threading.Thread(target=run_simulation, daemon=True)
            sim_thread.start()
            self.btn_sim_toggle.configure(text="STOP SIMULATOR", fg_color="#FF0055", text_color="#FFFFFF", border_color="#FF0055")
            self.lbl_stream_status.configure(text="● SIMULATOR ACTIVE", text_color="#00F0FF", fg_color="#082A3A")
        else:
            sim_active = False
            self.btn_sim_toggle.configure(text="START DEMO SIMULATOR", fg_color="#1E293B", text_color="#00F0FF", border_color="#00F0FF")
            self.lbl_stream_status.configure(text="● LISTENING [UDP: 5005]", text_color="#00FF88", fg_color="#06281E")

    def clear_logs(self):
        with data_lock:
            event_log.clear()
            self.last_rendered_event_count = -1
        self.log_textbox.configure(state="normal")
        self.log_textbox.delete("1.0", "end")
        self.log_textbox.configure(state="disabled")

    # ----------------- Main GUI Refresh Loop -----------------
    def update_dashboard(self):
        now = time.time()

        with data_lock:
            active_nodes = list(latest_node_data.keys())
            
            # 1. Update Node Cards & Compute Global Health
            online_count = 0
            total_health_sum = 0.0
            
            for n in sorted(active_nodes):
                data = latest_node_data[n]
                state = get_health_state(n, now)
                self.create_or_update_node_card(n, data, state, now)
                
                if state != "DOWN":
                    online_count += 1
                    total_health_sum += data.get("health", 100.0)

            # Update Header KPIs
            total_nodes_seen = max(len(active_nodes), 1)
            self.kpi_nodes.configure(text=f"{online_count} / {len(active_nodes)}")
            
            if online_count > 0:
                sys_health = total_health_sum / online_count
                h_col = "#00FF88" if sys_health >= 75 else ("#FFB800" if sys_health >= 40 else "#FF0055")
                self.kpi_health.configure(text=f"{sys_health:.1f}%", text_color=h_col)
            else:
                self.kpi_health.configure(text="NO DATA", text_color="#64748B")

            # 2. Update System Audit Logs Box (only on change)
            if len(event_log) != self.last_rendered_event_count:
                self.log_textbox.configure(state="normal")
                self.log_textbox.delete("1.0", "end")
                for entry in list(event_log)[:80]:
                    self.log_textbox.insert("end", entry["text"] + "\n")
                self.log_textbox.configure(state="disabled")
                self.last_rendered_event_count = len(event_log)

            # 3. Update Real-time Time Series Charts
            self.render_charts(now)

        self.after(CHART_UPDATE_INTERVAL_MS, self.update_dashboard)

    # ----------------- Render Matplotlib Telemetry Plots -----------------
    def render_charts(self, now):
        self.ax_cpu.clear()
        self.ax_ipc.clear()

        self.ax_cpu.set_facecolor("#090C13")
        self.ax_ipc.set_facecolor("#090C13")

        has_data = False
        nodes_plotted = 0

        for n in sorted(history.keys()):
            h = history[n]
            if not h["t"] or len(h["t"]) < 2:
                continue

            has_data = True
            nodes_plotted += 1
            color = get_node_color(n)
            xs = [t - now for t in h["t"]]

            # Plot CPU Load Line & Underglow Fill
            cpu_vals = list(h["cpu"])
            self.ax_cpu.plot(
                xs, cpu_vals, 
                color=color, 
                linewidth=2.0, 
                label=f"Node {n} ({NODE_ROLES.get(n, f'Sub {n}')})",
                alpha=0.9
            )
            self.ax_cpu.fill_between(xs, 0, cpu_vals, color=color, alpha=0.08)

            # Plot IPC Latency Line & Underglow Fill
            ipc_vals = list(h["ipc"])
            self.ax_ipc.plot(
                xs, ipc_vals, 
                color=color, 
                linewidth=2.0, 
                label=f"Node {n}",
                alpha=0.9
            )
            self.ax_ipc.fill_between(xs, 0, ipc_vals, color=color, alpha=0.08)

        # Style CPU Chart
        self.ax_cpu.axhline(90, color="#FF0055", linestyle="--", linewidth=1.2, alpha=0.75, label="Alarm Limit (90%)")
        self.ax_cpu.set_title("REAL-TIME CPU UTILIZATION (%)", fontsize=11, fontweight="bold", color="#00F0FF", loc="left", pad=8)
        self.ax_cpu.set_ylabel("CPU %", fontsize=9, color="#94A3B8")
        self.ax_cpu.set_ylim(0, 105)
        self.ax_cpu.grid(True, linestyle=":", alpha=0.15, color="#38BDF8")

        # Style IPC Chart
        self.ax_ipc.axhline(2000, color="#FF0055", linestyle="--", linewidth=1.2, alpha=0.75, label="Alarm Limit (2000µs)")
        self.ax_ipc.set_title("QNX KERNEL IPC ROUND-TRIP LATENCY (MICROSECONDS µS)", fontsize=11, fontweight="bold", color="#00F0FF", loc="left", pad=8)
        self.ax_ipc.set_xlabel("TIMELINE (SECONDS AGO)", fontsize=9, color="#94A3B8", labelpad=6)
        self.ax_ipc.set_ylabel("Latency (µs)", fontsize=9, color="#94A3B8")
        self.ax_ipc.grid(True, linestyle=":", alpha=0.15, color="#38BDF8")

        # Border & Spine Styling
        for ax in (self.ax_cpu, self.ax_ipc):
            for spine in ax.spines.values():
                spine.set_color("#1E293B")
            ax.tick_params(colors="#94A3B8", labelsize=8)
            if has_data:
                ax.legend(loc="upper left", fontsize=8, framealpha=0.2, facecolor="#090C13", edgecolor="#1E293B", labelcolor="linecolor")

        self.canvas.draw_idle()


# ----------------- Application Entry Point -----------------
if __name__ == "__main__":
    # Start UDP receiver background thread with port fallback
    receiver_th = threading.Thread(target=udp_receiver_thread, args=(DEFAULT_PORT,), daemon=True)
    receiver_th.start()

    # Launch GUI Application
    app = SmartCityDashboard()
    try:
        app.mainloop()
    finally:
        receiver_running = False
        sim_active = False
        if receiver_socket:
            try:
                receiver_socket.close()
            except Exception:
                pass
