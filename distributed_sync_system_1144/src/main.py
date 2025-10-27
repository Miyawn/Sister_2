import asyncio
import aiohttp
from aiohttp import web
from dotenv import load_dotenv

# Import utilitas dan konfigurasi
# PERBAIKAN: Impor dari direktori yang sama ('.')
from .config import Config 
# from .metrics import start_prometheus_server # Akan digunakan nanti

# Import modul komunikasi
# PERBAIKAN: Naik satu level ('..') untuk menemukan 'communication'
from ..communication import message_passing 
from ..communication.failure_detector import FailureDetector

# --- Placeholder untuk modul lain (belum diimplementasikan) ---
# PERBAIKAN: Naik satu level ('..') untuk menemukan 'nodes'
from ..nodes.queue_node import QueueNode 

class MockModule:
    def __init__(self, *args, **kwargs): pass

LockManager = CacheNode = RaftNode = PbftNode = MockModule
# --- Akhir Placeholder ---

async def main():
    """Fungsi utama untuk inisialisasi dan menjalankan server node."""
    load_dotenv()
    
    config = Config()
    app = web.Application()
    
    app['config'] = config
    app['http_client'] = aiohttp.ClientSession()
    
    print(f"🚀 Memulai node: {config.NODE_ID} di region {config.REGION} (Port: {config.NODE_PORT})")
    if config.IS_BYZANTINE:
        print(f"     -- WARNING: Node ini berjalan dalam mode BYZANTINE! --")
        
    message_passing.init_region_latencies(config.NODE_REGION, config.PEERS_REGIONS)
    app['failure_detector'] = FailureDetector(app)

    # Inisialisasi modul
    app['raft'] = RaftNode(app)
    app['pbft'] = PbftNode(app)
    app['lock_manager'] = LockManager(app)
    app['queue_node'] = QueueNode(app) # <-- Ini sekarang menggunakan kode asli
    app['cache_node'] = CacheNode(app)

    # Daftarkan Rute API (Endpoints)
    routes = [
        web.get('/health', app['failure_detector'].handle_health_check),
        
        # --- TAMBAHKAN RUTE QUEUE ---
        web.post('/queue/enqueue', app['queue_node'].handle_enqueue),
        web.get('/queue/dequeue', app['queue_node'].handle_dequeue),
        web.post('/queue/ack', app['queue_node'].handle_ack),
        # -----------------------------
    ]
    app.add_routes(routes)

    # Mulai Background Tasks
    app['failure_detector_ping_task'] = asyncio.create_task(app['failure_detector'].run_ping_loop())
    
    # Jalankan Server Web
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', config.NODE_PORT)
    await site.start()
    
    print(f"✅ Node {config.NODE_ID} berjalan di http://0.0.0.0:{config.NODE_PORT}")

    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Menghentikan node...")