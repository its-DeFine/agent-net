"""
AutoGen Platform - Main Application (Fixed)

A production-ready platform for orchestrating Microsoft AutoGen AI agents
with focus on maintainability, observability, and scalability.
"""
import os
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn
import logging
import structlog

EMBODIMENT_ONLY_ENV = os.environ.get("EMBODIMENT_ONLY", "").lower() == "true"
if EMBODIMENT_ONLY_ENV:
    from .api import auth
    from .api import embodiment
    from .api import orchestrators
    from .api import livepeer
    from .api import dashboard_clean as dashboard
    # Set None for unused modules in embodiment-only mode
    agents = teams = tasks = gpu = management = master = audit = ollama = security = cluster = vtuber = None
    orchestrator = gpu_orchestrator = agent_manager = collective_intelligence = None
    websocket_manager = cross_instance_bridge = audit_logger = ollama_manager = None
else:
    from .api import auth, agents, teams, tasks, gpu, management, master, audit, ollama, security, cluster, vtuber
    from .api import embodiment
    from .api import orchestrators
    from .api import livepeer
    from .api import dashboard_clean as dashboard
    from .core.orchestration.orchestrator import orchestrator
    from .core.orchestration.gpu_orchestrator import gpu_orchestrator
    from .core.agents.agent_manager import agent_manager
    from .core.agents.collective_intelligence import collective_intelligence
    from .infrastructure.messaging.websocket_manager import websocket_manager
    from .infrastructure.messaging.cross_instance_bridge import cross_instance_bridge
    from .infrastructure.monitoring.audit_logger import audit_logger
    from .services.ollama_integration import ollama_manager

from .dependencies import get_redis, get_docker
from .config import settings, IS_PRODUCTION, IS_DEVELOPMENT
from .middleware import LoggingMiddleware, MetricsMiddleware, RateLimitMiddleware, SecurityHeadersMiddleware
from .errors import PlatformError, platform_exception_handler

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer() if settings.log_format == "json" else structlog.dev.ConsoleRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting AutoGen Platform...")
    
    # Initialize database
    from .db_models import init_db
    init_db()
    logger.info("Database initialized")
    
    redis = await get_redis()
    
    # Check Redis connection
    await redis.ping()
    logger.info("Redis connected")
    
    # Try Docker connection (optional)
    try:
        docker = get_docker()
        docker.ping()
        logger.info("Docker connected")
    except Exception as e:
        logger.warning(f"Docker not available: {e}")
        docker = None
    
    if not EMBODIMENT_ONLY_ENV:
        # Start orchestrator (enhanced with 24/7 capabilities)
        await orchestrator.start()
        
        # Initialize GPU orchestrator
        await gpu_orchestrator.initialize()
        await gpu_orchestrator.start_monitoring()
        
        
        # Initialize agent manager with inter-agent communication
        await agent_manager.start()
        
        # Initialize collective intelligence system - FIXED: Don't block on startup
        # Create a task to start it in the background
        async def start_collective_intelligence():
            try:
                logger.info("Starting collective intelligence in background...")
                await collective_intelligence.start()
            except Exception as e:
                logger.warning(f"Collective intelligence startup issue (non-fatal): {e}")
        
        asyncio.create_task(start_collective_intelligence())
        
        # Initialize WebSocket manager for real-time updates
        await websocket_manager.start()
        
        
        # Initialize audit logger
        await audit_logger.initialize()
    else:
        logger.info("Running in EMBODIMENT_ONLY mode - skipping agent/orchestrator initialization")
    
    # Initialize security manager (optional in debug)
    if os.environ.get("SKIP_PGP_INIT", "").lower() == "true":
        logger.warning("Skipping PGP initialization (SKIP_PGP_INIT=true)")
    else:
        try:
            from .infrastructure.security.key_manager import secure_key_manager
            await secure_key_manager.initialize()
            logger.info("Security manager initialized with PGP support")
        except Exception as e:
            logger.warning(f"Security manager initialization skipped due to error: {e}")
    
    # Initialize distributed container services
    from .core.orchestration.container_discovery import container_discovery_service
    from .core.orchestration.container_registry import container_registry
    from .core.agents.distributed_agent_manager import distributed_agent_manager
    from .infrastructure.messaging.container_hub import container_hub
    
    try:
        # Initialize container communication hub first
        await container_hub.initialize()
        logger.info("Container communication hub initialized")
        
        # Start container discovery
        await container_discovery_service.start()
        logger.info("Container discovery service started")
        
        # Start container registry
        await container_registry.start()
        logger.info("Container registry service started")
        
        # Start distributed agent manager
        await distributed_agent_manager.initialize()
        logger.info("Distributed agent manager started")
    except Exception as e:
        logger.warning(f"Distributed services initialization issue: {e}")
        # Continue without distributed services - fallback to monolithic mode
    
    # Initialize Ollama if available
    try:
        await ollama_manager.initialize()
        logger.info(f"Ollama integration initialized with {len(ollama_manager.available_models)} models")
    except Exception as e:
        logger.warning(f"Ollama not available: {e}")
        # Continue without Ollama - it's optional
    
    
    # Initialize cross-instance bridge if configured
    instance_id = os.environ.get("INSTANCE_ID")
    master_url = os.environ.get("MASTER_URL")
    instance_api_key = os.environ.get("INSTANCE_API_KEY")
    
    if instance_id and master_url and instance_api_key:
        await cross_instance_bridge.initialize(instance_id, master_url, instance_api_key)
        asyncio.create_task(cross_instance_bridge.start_listening())
        logger.info("Cross-instance bridge initialized")
    
    logger.info("🚀 VTuber Autonomy Platform started successfully!")
    logger.info("📊 Access API docs at http://localhost:8000/docs")
    yield
    
    # Shutdown in reverse order
    logger.info("Shutting down VTuber Autonomy Platform...")
    
    # Stop distributed services if running
    try:
        await distributed_agent_manager.stop()
        await container_registry.stop()
        await container_discovery_service.stop()
        await container_hub.shutdown()
        logger.info("Distributed services stopped")
    except:
        pass  # Services might not have been started
    
    if not EMBODIMENT_ONLY_ENV:
        await websocket_manager.stop()
        await collective_intelligence.stop()
        await agent_manager.stop()
        await gpu_orchestrator.stop_monitoring()
        await orchestrator.stop()
    await redis.close()
    if docker:
        docker.close()

# Create app with configuration
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    lifespan=lifespan,
    docs_url="/docs" if not IS_PRODUCTION else None,
    redoc_url="/redoc" if not IS_PRODUCTION else None
)

# Add middleware in correct order (last added = first executed)
app.add_middleware(MetricsMiddleware)
app.add_middleware(LoggingMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)  # Conservative rate limiting
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8080"] if IS_DEVELOPMENT else ["https://yourdomain.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# Add exception handlers
app.add_exception_handler(PlatformError, platform_exception_handler)

# Include routers (allow minimal boot for embodiment-only QA)
if EMBODIMENT_ONLY_ENV:
    app.include_router(auth.router)
    app.include_router(embodiment.router)
    app.include_router(orchestrators.router)  # Orchestrator management API
    app.include_router(dashboard.router)
else:
    app.include_router(auth.router)
    app.include_router(agents.router)
    app.include_router(teams.router)
    app.include_router(tasks.router)
    app.include_router(gpu.router)
    app.include_router(management.router)  # New management API
    app.include_router(master.router)  # Master management API
    app.include_router(audit.router)  # Audit logging API
    app.include_router(ollama.router)  # Ollama LLM API
    app.include_router(security.router)  # Security API
    app.include_router(cluster.router)  # Cluster management API
    app.include_router(vtuber.router)  # VTuber control API
    app.include_router(embodiment.router)  # Embodied agent orchestration API
    app.include_router(orchestrators.router)  # Orchestrator management API
    app.include_router(livepeer.router)  # Livepeer connectivity pipeline API
    app.include_router(dashboard.router)  # Built-in Dashboard

# Health check
@app.get("/health")
async def health():
    from ._version import get_version
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "version": get_version()
    }

# Version endpoint
@app.get("/api/v1/version")
async def get_version_info():
    from ._version import get_version_info as get_full_version
    return get_full_version()

# Enhanced WebSocket endpoints using WebSocket Manager
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for real-time updates"""
    client_id = await websocket_manager.connect_client(websocket)
    
    try:
        while True:
            data = await websocket.receive_json()
            await websocket_manager.handle_client_message(client_id, data)
    except WebSocketDisconnect:
        await websocket_manager.disconnect_client(client_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket_manager.disconnect_client(client_id)

# Authenticated WebSocket endpoint
@app.websocket("/ws/authenticated")
async def authenticated_websocket(websocket: WebSocket):
    """Authenticated WebSocket endpoint with user context"""
    # Get token from query params
    token = websocket.query_params.get("token")
    user_id = None
    
    # Validate token - PROPER AUTHENTICATION IMPLEMENTED
    if not token:
        await websocket.close(code=4001, reason="Authentication token required")
        return
    
    try:
        import jwt
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        user_id = payload.get("sub")
        
        if not user_id:
            await websocket.close(code=4001, reason="Invalid token payload")
            return
            
    except jwt.ExpiredSignatureError:
        await websocket.close(code=4001, reason="Token expired")
        return
    except jwt.InvalidTokenError:
        await websocket.close(code=4001, reason="Invalid token")
        return
    except Exception as e:
        logger.error(f"WebSocket authentication error: {e}")
        await websocket.close(code=4001, reason="Authentication failed")
        return
        
    client_id = await websocket_manager.connect_client(websocket, user_id)
    
    try:
        while True:
            data = await websocket.receive_json()
            await websocket_manager.handle_client_message(client_id, data)
    except WebSocketDisconnect:
        await websocket_manager.disconnect_client(client_id)
    except Exception as e:
        logger.error(f"Authenticated WebSocket error: {e}")
        await websocket_manager.disconnect_client(client_id)


# API-only mode - no frontend
# All control via Master Manager UI

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)