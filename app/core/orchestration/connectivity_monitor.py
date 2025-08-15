"""
Orchestrator Connectivity Monitor
Provides proof-of-connection verification for Livepeer orchestrators
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import logging

from ...dependencies import get_redis
from ...errors import OrchestrationError

logger = logging.getLogger(__name__)


@dataclass
class ConnectionProof:
    """Proof of orchestrator connectivity"""
    orchestrator_id: str
    timestamp: str
    connection_id: str
    manager_challenge: str
    orchestrator_response: str
    latency_ms: int
    uptime_seconds: int
    processed_calls: int
    capabilities: List[str]
    
    def is_valid(self, secret_key: str) -> bool:
        """Validate the proof response"""
        expected = hashlib.sha256(
            f"{self.manager_challenge}:{self.orchestrator_id}:{secret_key}".encode()
        ).hexdigest()
        return self.orchestrator_response == expected


@dataclass 
class OrchestratorConnection:
    """Represents an orchestrator's connection state"""
    orchestrator_id: str
    endpoint: str
    status: str  # connected, disconnected, unhealthy
    last_heartbeat: Optional[str] = None
    last_proof: Optional[ConnectionProof] = None
    connection_started: Optional[str] = None
    total_calls_processed: int = 0
    average_latency_ms: float = 0.0
    capabilities: List[str] = None
    

class ConnectivityMonitor:
    """Monitors orchestrator connections and validates connectivity proofs"""
    
    REDIS_PREFIX = "orchestrator:connection:"
    CHALLENGE_PREFIX = "orchestrator:challenge:"
    HEARTBEAT_TIMEOUT = 30  # seconds
    PROOF_INTERVAL = 60  # seconds
    
    def __init__(self):
        self._connections: Dict[str, OrchestratorConnection] = {}
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
        self._secret_key = secrets.token_hex(32)
        
    async def register_orchestrator(
        self, 
        orchestrator_id: str, 
        endpoint: str,
        capabilities: List[str] = None
    ) -> str:
        """Register a new orchestrator connection"""
        connection = OrchestratorConnection(
            orchestrator_id=orchestrator_id,
            endpoint=endpoint,
            status="connected",
            connection_started=datetime.utcnow().isoformat(),
            capabilities=capabilities or []
        )
        
        self._connections[orchestrator_id] = connection
        
        # Store in Redis
        redis = await get_redis()
        key = f"{self.REDIS_PREFIX}{orchestrator_id}"
        await redis.set(key, asdict(connection))
        
        # Start monitoring
        if orchestrator_id in self._monitoring_tasks:
            self._monitoring_tasks[orchestrator_id].cancel()
        self._monitoring_tasks[orchestrator_id] = asyncio.create_task(
            self._monitor_connection(orchestrator_id)
        )
        
        logger.info(f"[connectivity] Registered orchestrator {orchestrator_id}")
        return self._generate_challenge(orchestrator_id)
    
    async def submit_proof(
        self,
        orchestrator_id: str,
        proof_data: Dict[str, Any]
    ) -> bool:
        """Submit and validate a connectivity proof"""
        if orchestrator_id not in self._connections:
            raise OrchestrationError(f"Unknown orchestrator: {orchestrator_id}")
            
        proof = ConnectionProof(
            orchestrator_id=orchestrator_id,
            timestamp=proof_data["timestamp"],
            connection_id=proof_data["connection_id"],
            manager_challenge=proof_data["manager_challenge"],
            orchestrator_response=proof_data["orchestrator_response"],
            latency_ms=proof_data["metrics"]["latency_ms"],
            uptime_seconds=proof_data["metrics"]["uptime_seconds"],
            processed_calls=proof_data["metrics"]["processed_calls"],
            capabilities=proof_data.get("capabilities", [])
        )
        
        # Validate proof
        if not proof.is_valid(self._secret_key):
            logger.warning(f"[connectivity] Invalid proof from {orchestrator_id}")
            return False
            
        # Update connection state
        connection = self._connections[orchestrator_id]
        connection.last_proof = proof
        connection.last_heartbeat = datetime.utcnow().isoformat()
        connection.status = "connected"
        connection.total_calls_processed = proof.processed_calls
        connection.average_latency_ms = proof.latency_ms
        
        # Update Redis
        redis = await get_redis()
        key = f"{self.REDIS_PREFIX}{orchestrator_id}"
        await redis.set(key, asdict(connection))
        
        logger.info(
            f"[connectivity] Valid proof from {orchestrator_id} "
            f"(latency: {proof.latency_ms}ms, calls: {proof.processed_calls})"
        )
        return True
    
    async def get_connection_status(
        self, 
        orchestrator_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get current connection status for an orchestrator"""
        if orchestrator_id not in self._connections:
            # Try to load from Redis
            redis = await get_redis()
            key = f"{self.REDIS_PREFIX}{orchestrator_id}"
            data = await redis.get(key)
            if data:
                return data
            return None
            
        connection = self._connections[orchestrator_id]
        return {
            "orchestrator_id": connection.orchestrator_id,
            "status": connection.status,
            "endpoint": connection.endpoint,
            "last_heartbeat": connection.last_heartbeat,
            "uptime": self._calculate_uptime(connection),
            "metrics": {
                "total_calls": connection.total_calls_processed,
                "avg_latency_ms": connection.average_latency_ms
            },
            "capabilities": connection.capabilities
        }
    
    async def list_connections(self) -> List[Dict[str, Any]]:
        """List all orchestrator connections"""
        redis = await get_redis()
        keys = await redis.keys(f"{self.REDIS_PREFIX}*")
        
        connections = []
        for key in keys:
            data = await redis.get(key)
            if data:
                connections.append(data)
                
        return connections
    
    async def disconnect_orchestrator(self, orchestrator_id: str) -> bool:
        """Mark an orchestrator as disconnected"""
        if orchestrator_id not in self._connections:
            return False
            
        connection = self._connections[orchestrator_id]
        connection.status = "disconnected"
        
        # Cancel monitoring
        if orchestrator_id in self._monitoring_tasks:
            self._monitoring_tasks[orchestrator_id].cancel()
            del self._monitoring_tasks[orchestrator_id]
            
        # Update Redis
        redis = await get_redis()
        key = f"{self.REDIS_PREFIX}{orchestrator_id}"
        await redis.set(key, asdict(connection))
        
        logger.info(f"[connectivity] Disconnected orchestrator {orchestrator_id}")
        return True
    
    def _generate_challenge(self, orchestrator_id: str) -> str:
        """Generate a challenge for proof-of-connection"""
        challenge = secrets.token_hex(16)
        asyncio.create_task(self._store_challenge(orchestrator_id, challenge))
        return challenge
    
    async def _store_challenge(self, orchestrator_id: str, challenge: str):
        """Store challenge in Redis with expiry"""
        redis = await get_redis()
        key = f"{self.CHALLENGE_PREFIX}{orchestrator_id}"
        await redis.set(key, challenge, ex=300)  # 5 minute expiry
    
    async def _monitor_connection(self, orchestrator_id: str):
        """Monitor an orchestrator connection"""
        while orchestrator_id in self._connections:
            try:
                connection = self._connections[orchestrator_id]
                
                # Check heartbeat timeout
                if connection.last_heartbeat:
                    last_hb = datetime.fromisoformat(connection.last_heartbeat)
                    if datetime.utcnow() - last_hb > timedelta(seconds=self.HEARTBEAT_TIMEOUT):
                        connection.status = "unhealthy"
                        logger.warning(f"[connectivity] Orchestrator {orchestrator_id} is unhealthy")
                
                # Request new proof periodically
                if connection.status == "connected":
                    await self._request_proof(orchestrator_id)
                    
                await asyncio.sleep(self.PROOF_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[connectivity] Monitor error for {orchestrator_id}: {e}")
                await asyncio.sleep(10)
    
    async def _request_proof(self, orchestrator_id: str):
        """Request connectivity proof from orchestrator"""
        # This would typically send a request to the orchestrator
        # For now, we'll just generate a new challenge
        challenge = self._generate_challenge(orchestrator_id)
        logger.debug(f"[connectivity] Requested proof from {orchestrator_id} with challenge {challenge}")
    
    def _calculate_uptime(self, connection: OrchestratorConnection) -> int:
        """Calculate uptime in seconds"""
        if not connection.connection_started:
            return 0
        start = datetime.fromisoformat(connection.connection_started)
        return int((datetime.utcnow() - start).total_seconds())


# Global instance
connectivity_monitor = ConnectivityMonitor()