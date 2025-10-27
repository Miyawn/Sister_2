import asyncio
from aiohttp import web

class FailureDetector:
    def __init__(self, app):
        self.app = app
        self.config = app['config']
        self.http_client = app['http_client']
        
        # --- PERUBAHAN DI SINI ---
        self.alive_nodes = set() # Mulai dari kosong
        self.dead_nodes = set()  # Mulai dari kosong
        self.unseen_nodes = set(self.config.PEERS.keys()) # Lacak node baru
        # --- AKHIR PERUBAHAN ---
        
        self.ping_interval = 5.0 # Detik
        self.ping_timeout = 2.0 # Detik

    async def run_ping_loop(self):
        """Background task untuk mem-ping semua peers."""
        await asyncio.sleep(7) 
        print(f"[{self.config.NODE_ID}] Memulai Failure Detector Ping Loop...")
        
        while True:
            # Ping dulu
            tasks = []
            for node_id, url in self.config.PEERS.items():
                tasks.append(self.ping_node(node_id, f"{url}/health"))
            
            await asyncio.gather(*tasks)
            
            # Baru tidur
            await asyncio.sleep(self.ping_interval)

    async def ping_node(self, node_id, url):
        """Mencoba menghubungi satu node."""
        try:
            async with self.http_client.get(url, timeout=self.ping_timeout) as response:
                if response.status == 200:
                    self.mark_node_up(node_id)
                else:
                    self.mark_node_down(node_id)
        except Exception:
            self.mark_node_down(node_id)

    def mark_node_up(self, node_id):
        # --- LOGIKA BARU ---
        if node_id in self.dead_nodes:
            # Node yang mati telah kembali
            self.dead_nodes.remove(node_id)
            self.alive_nodes.add(node_id)
            print(f"✅ DETECTOR [{self.config.NODE_ID}]: Node {node_id} KEMBALI UP.")
        elif node_id in self.unseen_nodes:
            # Node baru terdeteksi untuk pertama kali
            self.unseen_nodes.remove(node_id)
            self.alive_nodes.add(node_id)
            print(f"✅ DETECTOR [{self.config.NODE_ID}]: Node {node_id} terdeteksi UP (Pertama kali).")
        # --- AKHIR LOGIKA BARU ---
            
    def mark_node_down(self, node_id):
        # --- LOGIKA BARU ---
        if node_id in self.alive_nodes:
            # Node yang hidup baru saja mati
            self.alive_nodes.remove(node_id)
            self.dead_nodes.add(node_id)
            print(f"❌ DETECTOR [{self.config.NODE_ID}]: Node {node_id} terdeteksi DOWN.")
        elif node_id in self.unseen_nodes:
            # Node baru gagal di-ping pertama kali
            self.unseen_nodes.remove(node_id)
            self.dead_nodes.add(node_id)
            print(f"❌ DETECTOR [{self.config.NODE_ID}]: Node {node_id} terdeteksi DOWN (Pertama kali).")
        # --- AKHIR LOGIKA BARU ---

    async def handle_health_check(self, request: web.Request):
        """Endpoint yang dipanggil oleh node lain."""
        return web.json_response({"status": "ok", "node_id": self.config.NODE_ID})