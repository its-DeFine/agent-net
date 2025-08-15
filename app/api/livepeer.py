"""
Livepeer Orchestrator Pipeline API
Handles connectivity monitoring and call distribution for Livepeer orchestrators
"""

from __future__ import annotations

from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field

from ..dependencies import get_current_user
from ..core.orchestration.connectivity_monitor import connectivity_monitor

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/livepeer", tags=["livepeer"])


class OrchestratorRegistration(BaseModel):
    """Orchestrator registration request"""
    orchestrator_id: str = Field(..., description="Unique orchestrator identifier")
    endpoint: str = Field(..., description="Orchestrator API endpoint")
    capabilities: List[str] = Field(default_factory=list, description="Supported capabilities")
    eth_address: Optional[str] = Field(None, description="Ethereum address for payments")


class ConnectivityProof(BaseModel):
    """Connectivity proof submission"""
    orchestrator_id: str
    timestamp: str
    connection_id: str
    manager_challenge: str
    orchestrator_response: str
    metrics: Dict[str, Any]
    capabilities: List[str] = []


class PaymentCall(BaseModel):
    """Payment processing call request"""
    call_type: str = Field(..., description="Type of payment call")
    amount: float = Field(..., description="Payment amount")
    recipient: str = Field(..., description="Recipient address")
    metadata: Dict[str, Any] = {}


class CallDistribution(BaseModel):
    """Call distribution request across orchestrators"""
    call_type: str
    orchestrator_ids: Optional[List[str]] = None  # None means all connected
    strategy: str = Field("round_robin", description="Distribution strategy")
    payload: Dict[str, Any]


@router.post("/orchestrators/register")
async def register_orchestrator(
    req: OrchestratorRegistration,
    user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Register a new Livepeer orchestrator"""
    try:
        challenge = await connectivity_monitor.register_orchestrator(
            orchestrator_id=req.orchestrator_id,
            endpoint=req.endpoint,
            capabilities=req.capabilities
        )
        
        logger.info(f"[livepeer] Registered orchestrator {req.orchestrator_id}")
        
        return {
            "success": True,
            "orchestrator_id": req.orchestrator_id,
            "challenge": challenge,
            "message": "Orchestrator registered. Submit connectivity proof with the challenge."
        }
    except Exception as e:
        logger.error(f"[livepeer] Registration failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/orchestrators/{orchestrator_id}/proof")
async def submit_connectivity_proof(
    orchestrator_id: str,
    proof: ConnectivityProof,
    user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Submit connectivity proof from orchestrator"""
    if proof.orchestrator_id != orchestrator_id:
        raise HTTPException(status_code=400, detail="Orchestrator ID mismatch")
    
    try:
        valid = await connectivity_monitor.submit_proof(
            orchestrator_id=orchestrator_id,
            proof_data=proof.dict()
        )
        
        if not valid:
            raise HTTPException(status_code=401, detail="Invalid connectivity proof")
        
        return {
            "success": True,
            "message": "Connectivity proof accepted",
            "next_proof_required": 60  # seconds
        }
    except Exception as e:
        logger.error(f"[livepeer] Proof submission failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/orchestrators/{orchestrator_id}/status")
async def get_orchestrator_status(
    orchestrator_id: str,
    user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get connectivity status for a specific orchestrator"""
    status = await connectivity_monitor.get_connection_status(orchestrator_id)
    
    if not status:
        raise HTTPException(status_code=404, detail="Orchestrator not found")
    
    return status


@router.get("/orchestrators")
async def list_orchestrators(
    user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """List all registered orchestrators with connectivity status"""
    connections = await connectivity_monitor.list_connections()
    
    # Categorize by status
    connected = [c for c in connections if c.get("status") == "connected"]
    disconnected = [c for c in connections if c.get("status") == "disconnected"]
    unhealthy = [c for c in connections if c.get("status") == "unhealthy"]
    
    return {
        "total": len(connections),
        "connected": len(connected),
        "disconnected": len(disconnected),
        "unhealthy": len(unhealthy),
        "orchestrators": connections,
        "summary": {
            "total_calls_processed": sum(c.get("total_calls_processed", 0) for c in connections),
            "average_latency_ms": sum(c.get("average_latency_ms", 0) for c in connected) / max(len(connected), 1)
        }
    }


@router.post("/orchestrators/{orchestrator_id}/disconnect")
async def disconnect_orchestrator(
    orchestrator_id: str,
    user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Manually disconnect an orchestrator"""
    success = await connectivity_monitor.disconnect_orchestrator(orchestrator_id)
    
    if not success:
        raise HTTPException(status_code=404, detail="Orchestrator not found")
    
    return {
        "success": True,
        "message": f"Orchestrator {orchestrator_id} disconnected"
    }


@router.post("/calls/distribute")
async def distribute_calls(
    req: CallDistribution,
    background_tasks: BackgroundTasks,
    user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Distribute calls across connected orchestrators"""
    # Get connected orchestrators
    connections = await connectivity_monitor.list_connections()
    connected = [c for c in connections if c.get("status") == "connected"]
    
    if not connected:
        raise HTTPException(status_code=503, detail="No connected orchestrators available")
    
    # Filter by requested IDs if specified
    if req.orchestrator_ids:
        connected = [c for c in connected if c.get("orchestrator_id") in req.orchestrator_ids]
        if not connected:
            raise HTTPException(status_code=404, detail="None of the specified orchestrators are connected")
    
    # Distribute based on strategy
    distribution = []
    if req.strategy == "round_robin":
        for i, orchestrator in enumerate(connected):
            distribution.append({
                "orchestrator_id": orchestrator["orchestrator_id"],
                "endpoint": orchestrator["endpoint"],
                "assigned": True
            })
    elif req.strategy == "least_loaded":
        # Sort by processed calls (ascending)
        connected.sort(key=lambda x: x.get("total_calls_processed", 0))
        for orchestrator in connected[:3]:  # Top 3 least loaded
            distribution.append({
                "orchestrator_id": orchestrator["orchestrator_id"],
                "endpoint": orchestrator["endpoint"],
                "assigned": True
            })
    elif req.strategy == "capabilities":
        # Filter by required capabilities in payload
        required_caps = req.payload.get("required_capabilities", [])
        for orchestrator in connected:
            caps = orchestrator.get("capabilities", [])
            if all(cap in caps for cap in required_caps):
                distribution.append({
                    "orchestrator_id": orchestrator["orchestrator_id"],
                    "endpoint": orchestrator["endpoint"],
                    "assigned": True
                })
    
    if not distribution:
        raise HTTPException(status_code=503, detail="No suitable orchestrators for distribution strategy")
    
    # Queue the actual distribution in background
    background_tasks.add_task(_execute_distribution, req.call_type, distribution, req.payload)
    
    return {
        "success": True,
        "strategy": req.strategy,
        "orchestrators_assigned": len(distribution),
        "distribution": distribution,
        "status": "queued"
    }


@router.post("/payments/process")
async def process_payment(
    payment: PaymentCall,
    user: dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Process a payment through connected orchestrators"""
    # This would integrate with actual payment processing
    # For now, we'll simulate the flow
    
    connections = await connectivity_monitor.list_connections()
    connected = [c for c in connections if c.get("status") == "connected"]
    
    if not connected:
        raise HTTPException(status_code=503, detail="No orchestrators available for payment processing")
    
    # Select orchestrator with payment capability
    payment_capable = [
        c for c in connected 
        if "payment_processing" in c.get("capabilities", [])
    ]
    
    if not payment_capable:
        raise HTTPException(status_code=503, detail="No payment-capable orchestrators available")
    
    # Use the one with lowest latency
    orchestrator = min(payment_capable, key=lambda x: x.get("average_latency_ms", float('inf')))
    
    return {
        "success": True,
        "payment_id": f"pay_{datetime.utcnow().timestamp()}",
        "orchestrator_assigned": orchestrator["orchestrator_id"],
        "estimated_latency_ms": orchestrator.get("average_latency_ms", 0),
        "status": "processing"
    }


async def _execute_distribution(call_type: str, distribution: List[Dict], payload: Dict):
    """Execute the actual call distribution (background task)"""
    import httpx
    
    results = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        for target in distribution:
            try:
                response = await client.post(
                    f"{target['endpoint']}/execute",
                    json={
                        "call_type": call_type,
                        "payload": payload
                    }
                )
                results.append({
                    "orchestrator_id": target["orchestrator_id"],
                    "status": "success" if response.status_code == 200 else "failed",
                    "response_code": response.status_code
                })
            except Exception as e:
                results.append({
                    "orchestrator_id": target["orchestrator_id"],
                    "status": "error",
                    "error": str(e)
                })
    
    logger.info(f"[livepeer] Distribution completed: {len(results)} orchestrators processed")
    return results