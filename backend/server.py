import asyncio
from fastapi import FastAPI, APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
import json
from pydantic import BaseModel, EmailStr
from typing import Any, Dict, List, Optional
import uuid
from datetime import datetime, timezone, timedelta
import jwt
import bcrypt
import time
from pymongo.errors import PyMongoError
from agent.graph import build_graph
from analysis_progress import complete_analysis_job, create_analysis_job, fail_analysis_job, get_analysis_job

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Database connection
USE_INMEMORY_DB = os.environ.get("USE_INMEMORY_DB", "0") == "1"
client = None
db = None

if not USE_INMEMORY_DB:
    mongo_url = os.environ['MONGO_URL']
    client = AsyncIOMotorClient(mongo_url)
    db = client[os.environ['DB_NAME']]

# In-memory fallback store for local development without MongoDB
MEMORY_USERS = {}
MEMORY_ANALYSES = {}
MEMORY_STORE_PATH = ROOT_DIR / ".run" / "memory_store.json"

# Used during backend startup to load local fallback users and analyses from disk.
def _load_memory_store() -> None:
    if not USE_INMEMORY_DB:
        return
    try:
        if MEMORY_STORE_PATH.exists():
            data = json.loads(MEMORY_STORE_PATH.read_text(encoding="utf-8"))
            users = data.get("users", {})
            analyses = data.get("analyses", {})
            if isinstance(users, dict):
                MEMORY_USERS.update(users)
            if isinstance(analyses, dict):
                MEMORY_ANALYSES.update(analyses)
    except Exception:
        # Keep startup resilient in local mode.
        pass

# Used by in-memory DB helpers to persist local users and analyses to disk.
def _save_memory_store() -> None:
    if not USE_INMEMORY_DB:
        return
    try:
        MEMORY_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
        MEMORY_STORE_PATH.write_text(
            json.dumps(
                {
                    "users": MEMORY_USERS,
                    "analyses": MEMORY_ANALYSES,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
    except Exception:
        # Non-fatal for local mode.
        pass


_load_memory_store()

# Used by auth routes to look up a user in MongoDB or the local memory store.
async def db_find_user_by_email(email: str):
    if USE_INMEMORY_DB:
        return MEMORY_USERS.get(email)
    return await db.users.find_one({"email": email})

# Used by auth token validation to look up a user by id.
async def db_find_user_by_id(user_id: str):
    if USE_INMEMORY_DB:
        for user in MEMORY_USERS.values():
            if user["id"] == user_id:
                return {k: v for k, v in user.items() if k != "password"}
        return None
    return await db.users.find_one({"id": user_id}, {"_id": 0, "password": 0})

# Used by `/auth/register` to insert a user into the active persistence layer.
async def db_insert_user(user_doc: dict):
    if USE_INMEMORY_DB:
        MEMORY_USERS[user_doc["email"]] = user_doc
        _save_memory_store()
        return
    await db.users.insert_one(user_doc)

# Used by `_run_analysis_job` to store completed analyses.
async def db_insert_analysis(analysis_doc: dict):
    if USE_INMEMORY_DB:
        MEMORY_ANALYSES[analysis_doc["id"]] = analysis_doc
        _save_memory_store()
        return
    await db.analyses.insert_one(analysis_doc)

# Used by `/analysis/{analysis_id}` to fetch one saved analysis.
async def db_get_analysis(analysis_id: str, user_id: str):
    if USE_INMEMORY_DB:
        analysis = MEMORY_ANALYSES.get(analysis_id)
        if not analysis or analysis["user_id"] != user_id:
            return None
        return analysis
    return await db.analyses.find_one(
        {"id": analysis_id, "user_id": user_id},
        {"_id": 0}
    )

# Used by `/analysis` to fetch a user's saved analysis history.
async def db_get_analyses(user_id: str):
    if USE_INMEMORY_DB:
        analyses = [a for a in MEMORY_ANALYSES.values() if a["user_id"] == user_id]
        return sorted(analyses, key=lambda a: a["created_at"], reverse=True)
    return await db.analyses.find(
        {"user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)

# JWT Settings
JWT_SECRET = os.environ.get('JWT_SECRET', 'crop-optimizer-secret-key-2024')
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24

# Create the main app
app = FastAPI(title="Crop Portfolio Optimizer API")

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Security
security = HTTPBearer()

# ============ Models ============

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    name: str

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: str
    email: str
    name: str
    created_at: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse

class FarmLocation(BaseModel):
    lat: float
    lng: float
    address: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None

class FarmProfile(BaseModel):
    location: FarmLocation
    acres: float
    has_irrigation: bool
    soil_type: str
    selected_crops: List[str] = []
    risk_preference: str  # conservative, moderate, aggressive
    goal: str  # maximize_profit, minimize_risk, balanced

class AnalysisCreate(BaseModel):
    farm_profile: FarmProfile

class CropResult(BaseModel):
    crop_name: str
    expected_profit: float
    profit_p10: float
    profit_p50: float
    profit_p90: float
    yield_forecast: float
    yield_unit: str = "units/acre"
    calc_yield_for_profit: float = 0.0
    calc_yield_unit: str = "units/acre"
    price_forecast: float
    price_unit: str = "$/unit"
    revenue_per_acre: float = 0.0
    profit_per_acre: float = 0.0
    soil_compatibility: float
    risk_score: float
    risk_level: str
    soil_explanation: str
    cost_per_acre: float = 0.0
    forecast_source: str = "deterministic_fallback"
    forecast_confidence: float = 0.0
    cost_source: str = "api_or_default"

class AnalysisResponse(BaseModel):
    id: str
    user_id: str
    farm_profile: FarmProfile
    results: List[CropResult]
    weather_summary: str
    market_outlook: str
    created_at: str
    status: str


class AnalysisJobResponse(BaseModel):
    job_id: str
    status: str
    stage_id: str
    stage_title: str
    progress_pct: int
    message: str
    logs: List[Dict[str, Any]] = []
    error: Optional[str] = None
    result: Optional[AnalysisResponse] = None
    created_at: str
    updated_at: str

# Runs the LangGraph workflow and stores both progress state and final analysis output.
async def _run_analysis_job(job_id: str, data: AnalysisCreate, current_user: dict):
    start_ts = datetime.now(timezone.utc)
    timer_start = time.perf_counter()
    logger.info(
        "analysis.create started user_id=%s job_id=%s start_ts=%s",
        current_user["id"],
        job_id,
        start_ts.isoformat(),
    )
    try:
        analysis_id = str(uuid.uuid4())
        created_at = datetime.now(timezone.utc).isoformat()
        graph = build_graph()
        agent_out = await asyncio.to_thread(
            graph.invoke,
            {"farm_profile": data.farm_profile.model_dump(), "progress_job_id": job_id},
        )

        results = [CropResult(**r) for r in agent_out.get("crop_results", [])]
        weather_summary = str(agent_out.get("weather_summary", ""))
        market_outlook = str(agent_out.get("market_outlook", ""))

        analysis_doc = {
            "id": analysis_id,
            "user_id": current_user["id"],
            "farm_profile": data.farm_profile.model_dump(),
            "results": [r.model_dump() for r in results],
            "weather_summary": weather_summary,
            "market_outlook": market_outlook,
            "created_at": created_at,
            "status": "completed"
        }
        await db_insert_analysis(analysis_doc)

        response_payload = AnalysisResponse(
            id=analysis_id,
            user_id=current_user["id"],
            farm_profile=data.farm_profile,
            results=results,
            weather_summary=weather_summary,
            market_outlook=market_outlook,
            created_at=created_at,
            status="completed"
        )
        complete_analysis_job(job_id, response_payload.model_dump())

        end_ts = datetime.now(timezone.utc)
        duration_sec = time.perf_counter() - timer_start
        logger.info(
            "analysis.create completed user_id=%s job_id=%s end_ts=%s crops_analyzed=%d duration_sec=%.3f",
            current_user["id"],
            job_id,
            end_ts.isoformat(),
            len(results),
            duration_sec,
        )
    except Exception as exc:
        fail_analysis_job(job_id, f"Analysis failed: {exc}")
        logger.exception("analysis.create failed user_id=%s job_id=%s error=%s", current_user["id"], job_id, exc)
# ============ Auth Helpers ============
# Used by `register` to hash plaintext passwords.
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Used by `login` to verify plaintext credentials.
def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

# Used by auth routes to create JWT access tokens.
def create_token(user_id: str, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS),
        "iat": datetime.now(timezone.utc)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
# Used by protected routes to resolve the authenticated user from the bearer token.
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = await db_find_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except PyMongoError:
        raise HTTPException(status_code=503, detail="Database unavailable")
# Health/info route used by local startup checks.
@api_router.get("/")
async def root():
    return {"message": "Crop Portfolio Optimizer API", "version": "1.0.0"}

# Auth Routes
# Register a new user and return a JWT.
@api_router.post("/auth/register", response_model=TokenResponse)
async def register(user_data: UserCreate):
    # Check if user exists
    existing = await db_find_user_by_email(user_data.email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create user
    user_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    
    user_doc = {
        "id": user_id,
        "email": user_data.email,
        "name": user_data.name,
        "password": hash_password(user_data.password),
        "created_at": created_at
    }
    
    await db_insert_user(user_doc)
    
    token = create_token(user_id, user_data.email)
    
    return TokenResponse(
        access_token=token,
        user=UserResponse(
            id=user_id,
            email=user_data.email,
            name=user_data.name,
            created_at=created_at
        )
    )
# Authenticate a user and return a JWT.
@api_router.post("/auth/login", response_model=TokenResponse)
async def login(credentials: UserLogin):
    user = await db_find_user_by_email(credentials.email)
    if not user or not verify_password(credentials.password, user["password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = create_token(user["id"], user["email"])
    
    return TokenResponse(
        access_token=token,
        user=UserResponse(
            id=user["id"],
            email=user["email"],
            name=user["name"],
            created_at=user["created_at"]
        )
    )
# Return the authenticated user's profile.
@api_router.get("/auth/me", response_model=UserResponse)
async def get_me(current_user: dict = Depends(get_current_user)):
    return UserResponse(
        id=current_user["id"],
        email=current_user["email"],
        name=current_user["name"],
        created_at=current_user["created_at"]
    )

# Analysis Routes
# Run analysis synchronously and return the final payload in one request.
@api_router.post("/analysis/create", response_model=AnalysisResponse)
async def create_analysis(data: AnalysisCreate, current_user: dict = Depends(get_current_user)):
    job_id = str(uuid.uuid4())
    create_analysis_job(job_id, current_user["id"], data.farm_profile.model_dump())
    await _run_analysis_job(job_id, data, current_user)
    job = get_analysis_job(job_id)
    if not job or not job.get("result"):
        raise HTTPException(status_code=500, detail=job.get("error") if job else "Analysis failed")
    return AnalysisResponse(**job["result"])

# Start analysis asynchronously and return a pollable job object.
@api_router.post("/analysis/start", response_model=AnalysisJobResponse)
async def start_analysis(data: AnalysisCreate, current_user: dict = Depends(get_current_user)):
    job_id = str(uuid.uuid4())
    create_analysis_job(job_id, current_user["id"], data.farm_profile.model_dump())
    asyncio.create_task(_run_analysis_job(job_id, data, current_user))
    job = get_analysis_job(job_id)
    return AnalysisJobResponse(**job)

# Return live status for an async analysis job.
@api_router.get("/analysis/jobs/{job_id}", response_model=AnalysisJobResponse)
async def get_analysis_job_status(job_id: str, current_user: dict = Depends(get_current_user)):
    job = get_analysis_job(job_id)
    if not job or job.get("user_id") != current_user["id"]:
        raise HTTPException(status_code=404, detail="Analysis job not found")
    if job.get("result"):
        job["result"] = AnalysisResponse(**job["result"])
    return AnalysisJobResponse(**job)
# Fetch one saved completed analysis.
@api_router.get("/analysis/{analysis_id}", response_model=AnalysisResponse)
async def get_analysis(analysis_id: str, current_user: dict = Depends(get_current_user)):
    analysis = await db_get_analysis(analysis_id, current_user["id"])
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    
    return AnalysisResponse(**analysis)
# Fetch the current user's saved analysis history.
@api_router.get("/analysis", response_model=List[AnalysisResponse])
async def get_analyses(current_user: dict = Depends(get_current_user)):
    analyses = await db_get_analyses(current_user["id"])
    
    return [AnalysisResponse(**a) for a in analyses]

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# Close the Mongo client on app shutdown when Mongo is enabled.
@app.on_event("shutdown")
async def shutdown_db_client():
    if client:
        client.close()
