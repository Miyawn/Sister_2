import asyncio
import aiohttp
from typing import Dict, Any

# Matriks latensi simulasi (dalam detik)
REGION_LATENCIES = {
    ('us-east-1', 'eu-west-1'): 0.07,  # 70ms
    ('us-east-1', 'ap-south-1'): 0.15, # 150ms
    ('eu-west-1', 'ap-south-1'): 0.12, # 120ms
}
CURRENT_REGION = "default"
PEER_REGIONS = {}

def init_region_latencies(current_region: str, peer_regions: dict):
    """Inisialisasi info region dari config untuk simulasi latensi."""
    global CURRENT_REGION, PEER_REGIONS
    CURRENT_REGION = current_region
    PEER_REGIONS = peer_regions

async def get_simulated_latency(target_node_id: str) -> float:
    """Mendapatkan latensi simulasi berdasarkan region."""
    target_region = PEER_REGIONS.get(target_node_id)
    if not target_region or target_region == CURRENT_REGION:
        return 0.0
        
    regions = tuple(sorted((CURRENT_REGION, target_region)))
    return REGION_LATENCIES.get(regions, 0.05)

async def send_message(session: aiohttp.ClientSession, url: str, data: Dict[str, Any], target_node_id: str) -> Dict[str, Any] | None:
    """Mengirim satu pesan POST async ke node lain dengan simulasi latensi."""
    latency = await get_simulated_latency(target_node_id)
    if latency > 0:
        await asyncio.sleep(latency)
            
    try:
        async with session.post(url, json=data, timeout=2.0) as response:
            if response.status >= 200 and response.status < 300:
                return await response.json()
            else:
                # Jangan print error di sini, biarkan failure detector yang menangani
                return None
    except Exception:
        # Juga jangan print error, biarkan failure detector
        return None

async def broadcast_message(session: aiohttp.ClientSession, peers: Dict[str, str], endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Mengirim pesan ke *semua* peers secara paralel."""
    tasks = []
    for node_id, base_url in peers.items():
        url = f"{base_url}{endpoint}"
        tasks.append(send_message(session, url, data, target_node_id=node_id))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return dict(zip(peers.keys(), results))