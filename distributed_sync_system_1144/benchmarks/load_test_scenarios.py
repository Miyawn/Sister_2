from locust import HttpUser, task, between

class DistributedSystemUser(HttpUser):
    # Tunggu antara 1 sampai 5 detik antar tugas
    wait_time = between(1, 5)
    
    # client.base_url akan diatur ke salah satu node, misal http://VM_KALI_IP:8001

    @task(3)
    def test_lock_manager(self):
        """Uji alur acquire-release lock."""
        lock_name = "test_lock_1"
        client_id = self.environment.runner.greenlet.name # ID unik untuk tiap user
        
        # 1. Acquire lock
        response = self.client.post("/lock/acquire", json={
            "lock_name": lock_name,
            "type": "exclusive",
            "client_id": client_id
        })
        
        if response.status_code == 200:
            # 2. Release lock
            self.client.post("/lock/release", json={
                "lock_name": lock_name,
                "client_id": client_id
            })

    @task(2)
    def test_queue_system(self):
        """Uji alur enqueue-dequeue."""
        queue_name = "test_queue"
        
        # 1. Enqueue
        self.client.post("/queue/enqueue", json={
            "queue_name": queue_name,
            "message": {"user": self.environment.runner.greenlet.name, "data": "hello"}
        })
        
        # 2. Dequeue
        self.client.get(f"/queue/dequeue?queue_name={queue_name}&group=locust_group&consumer={self.environment.runner.greenlet.name}")
        # TODO: Implementasi ACK setelah dequeue berhasil

    @task(1)
    def test_cache(self):
        """Uji baca dan tulis cache."""
        key = "cache_key_1"
        
        # Tulis ke cache
        self.client.post("/cache/write", json={"key": key, "value": "locust_data"})
        
        # Baca dari cache
        self.client.get(f"/cache/read?key={key}")