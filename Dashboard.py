import json
import socket
import threading
import time
from collections import deque, defaultdict

# pyrefly: ignore [missing-import]
import customtkinter as ctk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

plt.style.use('dark_background')

PORT = 5005
HISTORY = 120          # samples kept per node
NODE_TIMEOUT = 3.0     # seconds without packets -> node DOWN

# Modern sci-fi HUD color palette
COLORS = ['#0FF0FC', '#FF003C', '#B026FF', '#00FF7F', '#FFD700', '#FF8C00', '#FF1493', '#1E90FF']

def get_node_color(n):
    try:
        idx = int(n)
    except ValueError:
        idx = sum(ord(c) for c in str(n))
    return COLORS[idx % len(COLORS)]

def get_health_color(state):
    if state == "HEALTHY": return "#00FF7F" # Neon Green
    if state == "DEGRADED": return "#FFD700" # Neon Yellow
    if state == "CRITICAL": return "#FF003C" # Neon Red
    return "#555555" # DOWN / Dark Grey

lock = threading.Lock()
history = defaultdict(lambda: {"t": deque(maxlen=HISTORY),
                               "cpu": deque(maxlen=HISTORY),
                               "ipc": deque(maxlen=HISTORY)})
latest = {}                         # node_id -> last message
last_seen = {}                      # node_id -> local time
last_fault = defaultdict(lambda: "NONE")
events = deque(maxlen=50)           # extended fault log for GUI


# ---------------- Data Receiver ----------------
def receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", PORT))
    print(f"Listening on UDP {PORT} ...")
    while True:
        data, addr = sock.recvfrom(2048)
        try:
            process(json.loads(data.decode()))
        except (ValueError, KeyError) as e:
            print("Bad packet:", e)


# ---------------- Data Processing ----------------
def process(m):
    now = time.time()
    with lock:
        n = m["node"]
        h = history[n]
        h["t"].append(now)
        h["cpu"].append(m["cpu"])
        h["ipc"].append(m["ipc_us"])
        latest[n] = m
        last_seen[n] = now
        fault_analysis(n, m)


# ---------------- Fault Analysis ----------------
def fault_analysis(n, m):
    f = m["fault"]
    if f != last_fault[n]:
        stamp = time.strftime("%H:%M:%S")
        if f != "NONE":
            events.appendleft(f"[{stamp}] Node{n}: FAULT {f}")
        else:
            events.appendleft(f"[{stamp}] Node{n}: recovered")
        last_fault[n] = f


def node_health_label(n, now):
    if n not in last_seen or now - last_seen[n] > NODE_TIMEOUT:
        return "DOWN"
    m = latest[n]
    if m["fault"] != "NONE" or m["health"] < 40:
        return "CRITICAL"
    if m["health"] < 75:
        return "DEGRADED"
    return "HEALTHY"


# ---------------- Desktop GUI App ----------------
class DashboardApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        self.title("QNX Real-time Monitor (Cyberpunk GUI)")
        self.geometry("1400x900")
        
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        
        # Grid layout (1x2)
        self.grid_columnconfigure(0, weight=1) # Left Sidebar (Nodes)
        self.grid_columnconfigure(1, weight=3) # Right Main (Charts)
        self.grid_rowconfigure(0, weight=1)
        
        # --- Left Panel: Nodes Status & Logs ---
        self.left_panel = ctk.CTkFrame(self, corner_radius=0, fg_color="#0B0C10")
        self.left_panel.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        self.left_panel.grid_rowconfigure(1, weight=3) # Nodes
        self.left_panel.grid_rowconfigure(3, weight=1) # Logs
        
        self.title_lbl = ctk.CTkLabel(self.left_panel, text="NODE STATUS", font=ctk.CTkFont(size=20, weight="bold"), text_color="#0FF0FC")
        self.title_lbl.grid(row=0, column=0, padx=10, pady=10, sticky="w")
        
        self.nodes_frame = ctk.CTkScrollableFrame(self.left_panel, fg_color="#11151C")
        self.nodes_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        
        self.logs_lbl = ctk.CTkLabel(self.left_panel, text="SYSTEM LOGS", font=ctk.CTkFont(size=20, weight="bold"), text_color="#B026FF")
        self.logs_lbl.grid(row=2, column=0, padx=10, pady=10, sticky="w")
        
        self.logs_frame = ctk.CTkScrollableFrame(self.left_panel, fg_color="#11151C")
        self.logs_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=5)
        self.last_event_count = -1
        self.last_top_event = None
        
        # --- Right Panel: Matplotlib Charts ---
        self.right_panel = ctk.CTkFrame(self, corner_radius=0, fg_color="#0B0C10")
        self.right_panel.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
        
        self.fig, (self.ax_cpu, self.ax_ipc) = plt.subplots(2, 1, figsize=(8, 8), facecolor='#0B0C10')
        self.fig.subplots_adjust(left=0.06, right=0.97, top=0.95, bottom=0.08, hspace=0.3)
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.right_panel)
        self.canvas.get_tk_widget().pack(fill=ctk.BOTH, expand=True)
        
        self.node_widgets = {}
        
        # Start the GUI update loop
        self.update_gui()

    def create_node_card(self, n):
        frame = ctk.CTkFrame(self.nodes_frame, fg_color="#1A202C", corner_radius=8, border_width=1, border_color=get_node_color(n))
        frame.pack(fill="x", padx=5, pady=5)
        
        lbl_title = ctk.CTkLabel(frame, text=f"NODE {n}", font=ctk.CTkFont(size=16, weight="bold"), text_color=get_node_color(n))
        lbl_title.grid(row=0, column=0, padx=10, pady=5, sticky="w")
        
        lbl_state = ctk.CTkLabel(frame, text="UNKNOWN", font=ctk.CTkFont(size=14, weight="bold"))
        lbl_state.grid(row=0, column=1, padx=10, pady=5, sticky="e")
        
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_columnconfigure(1, weight=1)
        
        info_frame = ctk.CTkFrame(frame, fg_color="transparent")
        info_frame.grid(row=1, column=0, columnspan=2, sticky="ew", padx=10, pady=5)
        info_frame.grid_columnconfigure((0,1), weight=1)
        
        lbl_task = ctk.CTkLabel(info_frame, text="Task: -", font=ctk.CTkFont(size=12))
        lbl_task.grid(row=0, column=0, sticky="w")
        lbl_cpu = ctk.CTkLabel(info_frame, text="CPU: -", font=ctk.CTkFont(size=12))
        lbl_cpu.grid(row=0, column=1, sticky="w")
        lbl_ipc = ctk.CTkLabel(info_frame, text="IPC: -", font=ctk.CTkFont(size=12))
        lbl_ipc.grid(row=1, column=0, sticky="w")
        lbl_health = ctk.CTkLabel(info_frame, text="Health: -", font=ctk.CTkFont(size=12))
        lbl_health.grid(row=1, column=1, sticky="w")
        lbl_fault = ctk.CTkLabel(info_frame, text="Fault: NONE", font=ctk.CTkFont(size=12, weight="bold"))
        lbl_fault.grid(row=2, column=0, columnspan=2, sticky="w")
        
        self.node_widgets[n] = {
            'frame': frame,
            'state': lbl_state,
            'task': lbl_task,
            'cpu': lbl_cpu,
            'ipc': lbl_ipc,
            'health': lbl_health,
            'fault': lbl_fault
        }

    def update_gui(self):
        now = time.time()
        
        with lock:
            # 1. Update Node Cards
            for n in list(latest.keys()):
                if n not in self.node_widgets:
                    self.create_node_card(n)
                
                state = node_health_label(n, now)
                m = latest[n]
                w = self.node_widgets[n]
                
                h_color = get_health_color(state)
                w['state'].configure(text=state, text_color=h_color)
                w['task'].configure(text=f"Task: {m['task']}")
                w['cpu'].configure(text=f"CPU: {m['cpu']:.1f}%")
                w['ipc'].configure(text=f"IPC: {m['ipc_us']:.0f}us")
                w['health'].configure(text=f"Health: {m['health']:.0f}%")
                
                fault_color = "#FF003C" if m['fault'] != "NONE" else "#0FF0FC"
                w['fault'].configure(text=f"Fault: {m['fault']}", text_color=fault_color)
            
            # Update DOWN nodes
            for n, w in self.node_widgets.items():
                if node_health_label(n, now) == "DOWN":
                    w['state'].configure(text="DOWN", text_color="#555555")

            # 2. Update Logs (only if changed)
            if len(events) != self.last_event_count or (events and self.last_top_event != events[0]):
                for widget in self.logs_frame.winfo_children():
                    widget.destroy()
                for ev in events:
                    ev_color = "#FF003C" if "FAULT" in ev and "recovered" not in ev else "#00FF7F"
                    lbl = ctk.CTkLabel(self.logs_frame, text=ev, text_color=ev_color, 
                                       font=ctk.CTkFont(family="monospace", size=12), anchor="w", justify="left")
                    lbl.pack(fill="x", padx=5, pady=2)
                self.last_event_count = len(events)
                if events:
                    self.last_top_event = events[0]
                
            # 3. Update Charts
            self.ax_cpu.clear()
            self.ax_ipc.clear()
            
            self.ax_cpu.set_facecolor('#11151C')
            self.ax_ipc.set_facecolor('#11151C')
            
            has_data = False
            for n in sorted(history):
                h = history[n]
                if not h["t"]: continue
                has_data = True
                xs = [t - now for t in h["t"]]
                color = get_node_color(n)
                
                # Subtle glow effect and line for CPU
                self.ax_cpu.plot(xs, list(h["cpu"]), color=color, linewidth=4, alpha=0.15)
                self.ax_cpu.plot(xs, list(h["cpu"]), label=f"Node {n}", color=color, linewidth=1.5, 
                            marker='o', markersize=3, markerfacecolor='white', markeredgewidth=0)
                for alpha_val in [0.05, 0.1]:
                    self.ax_cpu.fill_between(xs, 0, list(h["cpu"]), color=color, alpha=alpha_val)
                
                # Subtle glow effect and line for IPC
                self.ax_ipc.plot(xs, list(h["ipc"]), color=color, linewidth=4, alpha=0.15)
                self.ax_ipc.plot(xs, list(h["ipc"]), label=f"Node {n}", color=color, linewidth=1.5,
                            marker='o', markersize=3, markerfacecolor='white', markeredgewidth=0)
                for alpha_val in [0.05, 0.1]:
                    self.ax_ipc.fill_between(xs, 0, list(h["ipc"]), color=color, alpha=alpha_val)

            # Style CPU Plot
            self.ax_cpu.axhline(90, color="#FF003C", ls="--", lw=1.5, alpha=0.8)
            self.ax_cpu.set_title("CPU USAGE (%)", fontsize=11, fontweight='bold', color="#0FF0FC", loc='left', pad=10)
            self.ax_cpu.margins(y=0.15)
            self.ax_cpu.grid(True, linestyle='-', alpha=0.1, color='#0FF0FC')
            
            # Style IPC Plot
            self.ax_ipc.axhline(2000, color="#FF003C", ls="--", lw=1.5, alpha=0.8)
            self.ax_ipc.set_title("IPC LATENCY (US)", fontsize=11, fontweight='bold', color="#0FF0FC", loc='left', pad=10)
            self.ax_ipc.set_xlabel("SECONDS AGO", fontsize=9, color="#0FF0FC", labelpad=10)
            self.ax_ipc.margins(y=0.15)
            self.ax_ipc.grid(True, linestyle='-', alpha=0.1, color='#0FF0FC')
            
            # Clean up spines (borders)
            for ax in [self.ax_cpu, self.ax_ipc]:
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['bottom'].set_color('#0FF0FC')
                ax.spines['left'].set_color('#0FF0FC')
                ax.tick_params(colors='#0FF0FC', labelsize=9)
                if has_data:
                    ax.legend(loc="upper left", fontsize=9, framealpha=0.0, labelcolor='linecolor')

            self.canvas.draw_idle()

        # Schedule next update (1000ms = 1s)
        self.after(1000, self.update_gui)


if __name__ == "__main__":
    # Start UDP receiver in background
    threading.Thread(target=receiver, daemon=True).start()
    
    # Start CustomTkinter app in main thread
    app = DashboardApp()
    app.mainloop()
