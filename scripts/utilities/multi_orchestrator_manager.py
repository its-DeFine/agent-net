#!/usr/bin/env python3
"""
Multi-Orchestrator Manager for Livepeer Pipeline
Handles call distribution and payment processing across multiple orchestrators
"""

import asyncio
import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
import httpx
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MultiOrchestratorManager:
    """Manages multiple orchestrators for call distribution"""
    
    def __init__(self, manager_url: str = "http://localhost:8010", api_key: Optional[str] = None):
        self.manager_url = manager_url
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.orchestrators: List[Dict] = []
        self.call_history: List[Dict] = []
        
    async def fetch_orchestrators(self) -> List[Dict]:
        """Fetch list of connected orchestrators from manager"""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.manager_url}/api/v1/livepeer/orchestrators",
                    headers=self.headers
                )
                if response.status_code == 200:
                    data = response.json()
                    self.orchestrators = data.get("orchestrators", [])
                    logger.info(f"Fetched {len(self.orchestrators)} orchestrators")
                    return self.orchestrators
                else:
                    logger.error(f"Failed to fetch orchestrators: {response.status_code}")
                    return []
            except Exception as e:
                logger.error(f"Error fetching orchestrators: {e}")
                return []
    
    async def distribute_payment_calls(
        self,
        amount: float,
        recipients: List[str],
        strategy: str = "round_robin"
    ) -> Dict[str, Any]:
        """Distribute payment calls across orchestrators"""
        if not self.orchestrators:
            await self.fetch_orchestrators()
        
        connected = [o for o in self.orchestrators if o.get("status") == "connected"]
        if not connected:
            logger.error("No connected orchestrators available")
            return {"success": False, "error": "No connected orchestrators"}
        
        logger.info(f"Distributing {len(recipients)} payment calls using {strategy} strategy")
        
        results = {
            "total_calls": len(recipients),
            "successful": 0,
            "failed": 0,
            "orchestrators_used": set(),
            "details": []
        }
        
        # Distribute recipients across orchestrators
        assignments = self._assign_calls(recipients, connected, strategy)
        
        # Execute payments in parallel
        tasks = []
        for orchestrator_id, assigned_recipients in assignments.items():
            orchestrator = next(o for o in connected if o["orchestrator_id"] == orchestrator_id)
            for recipient in assigned_recipients:
                task = self._process_payment(
                    orchestrator,
                    amount,
                    recipient
                )
                tasks.append(task)
        
        # Wait for all payments to complete
        payment_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in payment_results:
            if isinstance(result, Exception):
                results["failed"] += 1
                results["details"].append({"status": "error", "error": str(result)})
            elif result.get("success"):
                results["successful"] += 1
                results["orchestrators_used"].add(result.get("orchestrator_id"))
                results["details"].append(result)
            else:
                results["failed"] += 1
                results["details"].append(result)
        
        results["orchestrators_used"] = list(results["orchestrators_used"])
        
        # Store in history
        self.call_history.append({
            "timestamp": datetime.utcnow().isoformat(),
            "type": "payment_distribution",
            "results": results
        })
        
        return results
    
    async def send_transcoding_calls(
        self,
        video_urls: List[str],
        profiles: List[str],
        strategy: str = "least_loaded"
    ) -> Dict[str, Any]:
        """Distribute video transcoding calls"""
        if not self.orchestrators:
            await self.fetch_orchestrators()
        
        # Filter orchestrators with transcoding capability
        capable = [
            o for o in self.orchestrators 
            if o.get("status") == "connected" and "video_transcoding" in o.get("capabilities", [])
        ]
        
        if not capable:
            logger.error("No transcoding-capable orchestrators available")
            return {"success": False, "error": "No capable orchestrators"}
        
        logger.info(f"Distributing {len(video_urls)} transcoding jobs using {strategy} strategy")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.manager_url}/api/v1/livepeer/calls/distribute",
                headers=self.headers,
                json={
                    "call_type": "transcode",
                    "strategy": strategy,
                    "payload": {
                        "video_urls": video_urls,
                        "profiles": profiles,
                        "required_capabilities": ["video_transcoding"]
                    }
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"success": False, "error": f"API error: {response.status_code}"}
    
    async def monitor_orchestrator_health(self, interval: int = 30):
        """Continuously monitor orchestrator health"""
        logger.info(f"Starting health monitoring (interval: {interval}s)")
        
        while True:
            await self.fetch_orchestrators()
            
            connected = [o for o in self.orchestrators if o.get("status") == "connected"]
            disconnected = [o for o in self.orchestrators if o.get("status") == "disconnected"]
            unhealthy = [o for o in self.orchestrators if o.get("status") == "unhealthy"]
            
            logger.info(
                f"Health Status - Connected: {len(connected)}, "
                f"Disconnected: {len(disconnected)}, Unhealthy: {len(unhealthy)}"
            )
            
            # Alert on unhealthy orchestrators
            for orch in unhealthy:
                logger.warning(f"Unhealthy orchestrator: {orch['orchestrator_id']}")
            
            # Calculate aggregate metrics
            if connected:
                total_calls = sum(o.get("total_calls_processed", 0) for o in connected)
                avg_latency = sum(o.get("average_latency_ms", 0) for o in connected) / len(connected)
                logger.info(f"Aggregate Metrics - Total Calls: {total_calls}, Avg Latency: {avg_latency:.2f}ms")
            
            await asyncio.sleep(interval)
    
    async def execute_load_test(
        self,
        num_calls: int = 100,
        call_type: str = "payment",
        concurrent: int = 10
    ) -> Dict[str, Any]:
        """Execute a load test across orchestrators"""
        logger.info(f"Starting load test: {num_calls} {call_type} calls with {concurrent} concurrent")
        
        start_time = time.time()
        
        if call_type == "payment":
            # Generate test payment calls
            recipients = [f"0x{i:040x}" for i in range(num_calls)]
            results = await self.distribute_payment_calls(0.001, recipients, "round_robin")
        elif call_type == "transcode":
            # Generate test transcode calls
            videos = [f"video_{i}.mp4" for i in range(num_calls)]
            results = await self.send_transcoding_calls(videos, ["720p", "1080p"], "least_loaded")
        else:
            return {"success": False, "error": f"Unknown call type: {call_type}"}
        
        duration = time.time() - start_time
        
        return {
            "test_type": call_type,
            "total_calls": num_calls,
            "duration_seconds": duration,
            "calls_per_second": num_calls / duration if duration > 0 else 0,
            "results": results
        }
    
    def _assign_calls(
        self,
        items: List[Any],
        orchestrators: List[Dict],
        strategy: str
    ) -> Dict[str, List[Any]]:
        """Assign items to orchestrators based on strategy"""
        assignments = {o["orchestrator_id"]: [] for o in orchestrators}
        
        if strategy == "round_robin":
            for i, item in enumerate(items):
                orch = orchestrators[i % len(orchestrators)]
                assignments[orch["orchestrator_id"]].append(item)
                
        elif strategy == "least_loaded":
            # Sort by total calls processed (ascending)
            sorted_orchs = sorted(orchestrators, key=lambda x: x.get("total_calls_processed", 0))
            for i, item in enumerate(items):
                orch = sorted_orchs[i % min(3, len(sorted_orchs))]  # Use top 3 least loaded
                assignments[orch["orchestrator_id"]].append(item)
                
        elif strategy == "random":
            import random
            for item in items:
                orch = random.choice(orchestrators)
                assignments[orch["orchestrator_id"]].append(item)
        
        return {k: v for k, v in assignments.items() if v}  # Remove empty assignments
    
    async def _process_payment(
        self,
        orchestrator: Dict,
        amount: float,
        recipient: str
    ) -> Dict[str, Any]:
        """Process a single payment through an orchestrator"""
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                response = await client.post(
                    f"{self.manager_url}/api/v1/livepeer/payments/process",
                    headers=self.headers,
                    json={
                        "call_type": "payment",
                        "amount": amount,
                        "recipient": recipient,
                        "metadata": {
                            "orchestrator_id": orchestrator["orchestrator_id"]
                        }
                    }
                )
                
                if response.status_code == 200:
                    result = response.json()
                    result["orchestrator_id"] = orchestrator["orchestrator_id"]
                    return result
                else:
                    return {
                        "success": False,
                        "orchestrator_id": orchestrator["orchestrator_id"],
                        "error": f"HTTP {response.status_code}"
                    }
            except Exception as e:
                return {
                    "success": False,
                    "orchestrator_id": orchestrator["orchestrator_id"],
                    "error": str(e)
                }
    
    def print_summary(self):
        """Print summary of operations"""
        print("\n" + "="*60)
        print("MULTI-ORCHESTRATOR MANAGER SUMMARY")
        print("="*60)
        
        print(f"\nConnected Orchestrators: {len([o for o in self.orchestrators if o.get('status') == 'connected'])}")
        print(f"Total Orchestrators: {len(self.orchestrators)}")
        
        if self.call_history:
            print(f"\nCall History: {len(self.call_history)} operations")
            for entry in self.call_history[-5:]:  # Last 5 operations
                print(f"  - {entry['timestamp']}: {entry['type']} "
                      f"(Success: {entry['results'].get('successful', 0)}, "
                      f"Failed: {entry['results'].get('failed', 0)})")
        
        print("="*60)


async def main():
    parser = argparse.ArgumentParser(description="Multi-Orchestrator Manager for Livepeer")
    parser.add_argument("--manager-url", default="http://localhost:8010", help="Manager API URL")
    parser.add_argument("--api-key", help="API key for authentication")
    parser.add_argument("--action", choices=["monitor", "test", "payment", "transcode"], 
                       default="monitor", help="Action to perform")
    parser.add_argument("--num-calls", type=int, default=10, help="Number of calls for test/payment")
    parser.add_argument("--strategy", default="round_robin", 
                       choices=["round_robin", "least_loaded", "random"],
                       help="Distribution strategy")
    
    args = parser.parse_args()
    
    manager = MultiOrchestratorManager(args.manager_url, args.api_key)
    
    try:
        if args.action == "monitor":
            await manager.monitor_orchestrator_health()
            
        elif args.action == "test":
            results = await manager.execute_load_test(args.num_calls, "payment")
            print(json.dumps(results, indent=2))
            
        elif args.action == "payment":
            recipients = [f"0x{i:040x}" for i in range(args.num_calls)]
            results = await manager.distribute_payment_calls(0.001, recipients, args.strategy)
            print(json.dumps(results, indent=2))
            
        elif args.action == "transcode":
            videos = [f"video_{i}.mp4" for i in range(args.num_calls)]
            results = await manager.send_transcoding_calls(videos, ["720p", "1080p"], args.strategy)
            print(json.dumps(results, indent=2))
        
        manager.print_summary()
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        manager.print_summary()


if __name__ == "__main__":
    asyncio.run(main())