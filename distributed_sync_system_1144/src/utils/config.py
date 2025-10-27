import os

class Config:
    def __init__(self):
        self.NODE_ID = os.getenv('NODE_ID', 'default_node')
        self.NODE_PORT = int(os.getenv('NODE_PORT', 8000))
        self.NODE_REGION = os.getenv('NODE_REGION', 'default_region')
        self.IS_BYZANTINE = os.getenv('IS_BYZANTINE', 'False').lower() == 'true'
        
        self.REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
        self.REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
        
        node_list_str = os.getenv('NODE_LIST', '')
        
        # Informasi region statis
        ALL_NODES_INFO = {
            'node1': 'us-east-1',
            'node2': 'eu-west-1',
            'node3': 'ap-south-1',
            'node4_byz': 'us-east-1'
        }
        
        self.PEERS = {} # Format: { 'node_id': 'http://hostname:port' }
        self.PEERS_REGIONS = {} # Format: { 'node_id': 'region_name' }
        
        if node_list_str:
            for node_addr in node_list_str.split(','):
                parts = node_addr.split(':')
                node_id = parts[0]
                
                if node_id != self.NODE_ID:
                    hostname = "localhost" # Default hostname
                    port = "8000"       # Default port
                    
                    if len(parts) == 3: # Format: node_id:hostname:port (untuk local testing)
                        hostname = parts[1]
                        port = parts[2]
                    elif len(parts) == 2: # Format: node_id:port (untuk Docker)
                        hostname = node_id # Di Docker, hostname = node_id
                        port = parts[1]
                    
                    self.PEERS[node_id] = f"http://{hostname}:{port}"
                    self.PEERS_REGIONS[node_id] = ALL_NODES_INFO.get(node_id, 'default_region')

        print(f"[{self.NODE_ID}] Config loaded. Region: {self.NODE_REGION}")
        print(f"[{self.NODE_ID}] Peers: {list(self.PEERS.keys())}")
        print(f"[{self.NODE_ID}] Peer URLs: {list(self.PEERS.values())}")