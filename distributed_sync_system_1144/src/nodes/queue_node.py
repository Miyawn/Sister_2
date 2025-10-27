import aioredis
from aiohttp import web
from consistent_hashing.ring import ConsistentHashRing

# Impor relatif
from ..communication import message_passing

class QueueNode:
    """
    Mengelola distributed queue menggunakan consistent hashing.
    Menggunakan Redis Streams untuk persistensi dan at-least-once delivery.
    """
    def __init__(self, app):
        self.app = app
        self.config = app['config']
        self.http_client = app['http_client']
        self.node_id = self.config.NODE_ID
        
        # Inisialisasi Consistent Hashing Ring
        # Ambil semua node (termasuk diri sendiri) dari config
        all_nodes = sorted(list(self.config.PEERS.keys()) + [self.node_id])
        self.ring = ConsistentHashRing(nodes=all_nodes)
        print(f"[{self.node_id}] QueueNode: Hash ring diinisialisasi dengan nodes: {all_nodes}")
        
        self.redis = None # Koneksi Redis akan dibuat saat dibutuhkan

    async def _connect_redis(self):
        """Membuka koneksi ke Redis."""
        if self.redis is None or not self.redis.is_connected:
            try:
                self.redis = await aioredis.from_url(
                    f"redis://{self.config.REDIS_HOST}:{self.config.REDIS_PORT}",
                    decode_responses=True
                )
                await self.redis.ping()
                print(f"[{self.node_id}] QueueNode: Terhubung ke Redis di {self.config.REDIS_HOST}")
            except Exception as e:
                print(f"[{self.node_id}] QueueNode: GAGAL terhubung ke Redis: {e}")
                self.redis = None
        return self.redis

    def _get_target_node(self, queue_name: str) -> str:
        """Tentukan node yang bertanggung jawab untuk queue ini."""
        return self.ring.get_node(queue_name)

    # --- API Handlers ---

    async def handle_enqueue(self, request: web.Request):
        """Menangani penambahan pesan ke queue."""
        try:
            data = await request.json()
            queue_name = data.get('queue_name')
            message = data.get('message') # message harus berupa dict
            
            if not queue_name or not message or not isinstance(message, dict):
                return web.json_response({"error": "queue_name dan message (dict) diperlukan"}, status=400)

            target_node_id = self._get_target_node(queue_name)
            
            if target_node_id == self.node_id:
                # === Node ini yang bertanggung jawab ===
                redis = await self._connect_redis()
                if not redis:
                    return web.json_response({"error": "Layanan tidak tersedia (Redis down)"}, status=503)
                
                # Gunakan Redis Streams untuk persistensi
                message_id = await redis.xadd(queue_name, message)
                # print(f"[{self.node_id}] ENQUEUE: Pesan {message_id} ditambahkan ke {queue_name}")
                return web.json_response({"status": "enqueued", "node": self.node_id, "message_id": message_id})
            
            else:
                # === Forward request ke node yang benar ===
                # print(f"[{self.node_id}] FORWARD: Meneruskan enqueue {queue_name} ke {target_node_id}")
                url = f"{self.config.PEERS[target_node_id]}/queue/enqueue"
                result = await message_passing.send_message(self.http_client, url, data, target_node_id)
                
                if result:
                    return web.json_response(result)
                else:
                    return web.json_response({"error": f"Gagal meneruskan ke {target_node_id}"}, status=502)
        
        except Exception as e:
            return web.json_response({"error": f"Internal server error: {e}"}, status=500)

    async def handle_dequeue(self, request: web.Request):
        """Menangani pengambilan pesan (at-least-once)."""
        queue_name = request.query.get('queue_name')
        consumer_group = request.query.get('group', 'default_group')
        consumer_id = request.query.get('consumer', 'default_consumer')

        if not queue_name:
            return web.json_response({"error": "queue_name diperlukan"}, status=400)

        target_node_id = self._get_target_node(queue_name)
        
        if target_node_id == self.node_id:
            # === Node ini yang bertanggung jawab ===
            redis = await self._connect_redis()
            if not redis:
                return web.json_response({"error": "Layanan tidak tersedia (Redis down)"}, status=503)

            try:
                # Buat consumer group jika belum ada
                try:
                    await redis.xgroup_create(queue_name, consumer_group, id='$', mkstream=True)
                except aioredis.ResponseError as e:
                    if "BUSYGROUP" not in str(e): # Abaikan error jika grup sudah ada
                        raise e
                
                # Baca 1 pesan baru ('>') untuk consumer ini, tunggu hingga 5 detik (block=5000)
                messages = await redis.xreadgroup(
                    consumer_group,
                    consumer_id,
                    {queue_name: '>'}, 
                    count=1,
                    block=5000 
                )
                
                if not messages:
                    return web.json_response({"status": "no_message"})
                    
                # Format: [ [stream_name, [ (message_id, {field: val}) ]] ]
                msg_id = messages[0][1][0][0]
                msg_data = messages[0][1][0][1]
                
                # print(f"[{self.node_id}] DEQUEUE: Pesan {msg_id} dari {queue_name} dikirim ke {consumer_id}")
                return web.json_response({"status": "dequeued", "message_id": msg_id, "message": msg_data})
            
            except Exception as e:
                return web.json_response({"error": f"Internal server error: {e}"}, status=500)
        else:
            # === Forward request ke node yang benar ===
            # print(f"[{self.node_id}] FORWARD: Meneruskan dequeue {queue_name} ke {target_node_id}")
            url = f"{self.config.PEERS[target_node_id]}/queue/dequeue?{request.query_string}"
            
            # send_message hanya untuk POST, kita perlu GET
            try:
                async with self.http_client.get(url, timeout=7.0) as response:
                    return web.json_response(await response.json(), status=response.status)
            except Exception:
                return web.json_response({"error": f"Gagal meneruskan ke {target_node_id}"}, status=502)

    async def handle_ack(self, request: web.Request):
        """Menangani konfirmasi (ACK) dari consumer."""
        try:
            data = await request.json()
            queue_name = data.get('queue_name')
            consumer_group = data.get('group')
            message_id = data.get('message_id')

            if not all([queue_name, consumer_group, message_id]):
                return web.json_response({"error": "queue_name, group, dan message_id diperlukan"}, status=400)

            target_node_id = self._get_target_node(queue_name)
            
            if target_node_id == self.node_id:
                # === Node ini yang bertanggung jawab ===
                redis = await self._connect_redis()
                if not redis:
                    return web.json_response({"error": "Layanan tidak tersedia (Redis down)"}, status=503)
                
                # ACK pesan, menghapusnya dari Pending Entries List (PEL)
                await redis.xack(queue_name, consumer_group, message_id)
                # print(f"[{self.node_id}] ACK: Pesan {message_id} dari {queue_name} di-ACK")
                return web.json_response({"status": "acked"})
            
            else:
                # === Forward request ke node yang benar ===
                url = f"{self.config.PEERS[target_node_id]}/queue/ack"
                result = await message_passing.send_message(self.http_client, url, data, target_node_id)
                if result:
                    return web.json_response(result)
                else:
                    return web.json_response({"error": f"Gagal meneruskan ke {target_node_id}"}, status=502)
        
        except Exception as e:
            return web.json_response({"error": f"Internal server error: {e}"}, status=500)