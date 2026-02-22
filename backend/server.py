from fastapi import FastAPI, APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta
import jwt
import bcrypt
import random
from pymongo.errors import PyMongoError

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


async def db_find_user_by_email(email: str):
    if USE_INMEMORY_DB:
        return MEMORY_USERS.get(email)
    return await db.users.find_one({"email": email})


async def db_find_user_by_id(user_id: str):
    if USE_INMEMORY_DB:
        for user in MEMORY_USERS.values():
            if user["id"] == user_id:
                return {k: v for k, v in user.items() if k != "password"}
        return None
    return await db.users.find_one({"id": user_id}, {"_id": 0, "password": 0})


async def db_insert_user(user_doc: dict):
    if USE_INMEMORY_DB:
        MEMORY_USERS[user_doc["email"]] = user_doc
        return
    await db.users.insert_one(user_doc)


async def db_insert_analysis(analysis_doc: dict):
    if USE_INMEMORY_DB:
        MEMORY_ANALYSES[analysis_doc["id"]] = analysis_doc
        return
    await db.analyses.insert_one(analysis_doc)


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
    soil_ph: float
    crop_constraints: List[str] = []
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
    price_forecast: float
    soil_compatibility: float
    risk_score: float
    risk_level: str
    soil_explanation: str

class AnalysisResponse(BaseModel):
    id: str
    user_id: str
    farm_profile: FarmProfile
    results: List[CropResult]
    weather_summary: str
    market_outlook: str
    created_at: str
    status: str

# ============ Auth Helpers ============

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_token(user_id: str, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS),
        "iat": datetime.now(timezone.utc)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

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

# ============ Mock Data Generator ============

def generate_mock_crop_results(farm_profile: FarmProfile) -> List[CropResult]:
    """Generate realistic mock crop analysis results based on farm profile"""
    
    crops_data = [
        {"name": "Corn", "base_yield": 180, "base_price": 5.50, "water_need": "medium"},
        {"name": "Soybeans", "base_yield": 55, "base_price": 13.50, "water_need": "low"},
        {"name": "Wheat", "base_yield": 60, "base_price": 7.20, "water_need": "low"},
        {"name": "Cotton", "base_yield": 900, "base_price": 0.85, "water_need": "high"},
        {"name": "Rice", "base_yield": 7500, "base_price": 0.15, "water_need": "very_high"},
        {"name": "Alfalfa", "base_yield": 8, "base_price": 220, "water_need": "medium"},
        {"name": "Sorghum", "base_yield": 75, "base_price": 5.80, "water_need": "low"},
        {"name": "Sunflower", "base_yield": 1800, "base_price": 0.22, "water_need": "low"},
    ]
    
    # Filter based on constraints
    excluded = [c.lower() for c in farm_profile.crop_constraints]
    available_crops = [c for c in crops_data if c["name"].lower() not in excluded]
    
    results = []
    for crop in available_crops[:6]:  # Top 6 crops
        # Soil compatibility based on pH
        optimal_ph = {"Corn": 6.5, "Soybeans": 6.5, "Wheat": 6.5, "Cotton": 6.2, 
                      "Rice": 6.0, "Alfalfa": 6.8, "Sorghum": 6.5, "Sunflower": 6.5}
        ph_diff = abs(farm_profile.soil_ph - optimal_ph.get(crop["name"], 6.5))
        soil_compat = max(0.5, 1 - ph_diff * 0.15)
        
        # Irrigation impact
        irrigation_mult = 1.2 if farm_profile.has_irrigation and crop["water_need"] in ["high", "very_high"] else 1.0
        
        # Calculate yields and profits
        yield_variation = random.uniform(0.85, 1.15)
        price_variation = random.uniform(0.90, 1.10)
        
        base_yield = crop["base_yield"] * soil_compat * irrigation_mult * yield_variation
        base_price = crop["base_price"] * price_variation
        
        gross_revenue = base_yield * base_price * farm_profile.acres
        cost_per_acre = random.uniform(350, 550)
        total_cost = cost_per_acre * farm_profile.acres
        expected_profit = gross_revenue - total_cost
        
        # Risk calculations
        risk_factor = {"conservative": 0.8, "moderate": 1.0, "aggressive": 1.2}.get(farm_profile.risk_preference, 1.0)
        std_dev = expected_profit * 0.25 * risk_factor
        
        profit_p10 = expected_profit - 1.28 * std_dev
        profit_p50 = expected_profit
        profit_p90 = expected_profit + 1.28 * std_dev
        
        # Risk score (0-100, lower is less risky)
        risk_score = random.uniform(20, 80) * risk_factor
        risk_level = "Low" if risk_score < 35 else "Medium" if risk_score < 65 else "High"
        
        # Soil explanation
        soil_explanations = {
            "Corn": f"Corn thrives in {farm_profile.soil_type} soil. pH of {farm_profile.soil_ph} is {'optimal' if ph_diff < 0.5 else 'acceptable' if ph_diff < 1 else 'challenging'}.",
            "Soybeans": f"Soybeans fix nitrogen and perform well in {farm_profile.soil_type}. Current pH supports {'excellent' if ph_diff < 0.3 else 'good'} nodulation.",
            "Wheat": f"Winter wheat adapts well to {farm_profile.soil_type}. Drainage is {'ideal' if farm_profile.has_irrigation else 'dependent on rainfall'}.",
            "Cotton": f"Cotton requires well-drained soil. {farm_profile.soil_type} provides {'good' if 'loam' in farm_profile.soil_type.lower() else 'adequate'} structure.",
            "Rice": f"Rice cultivation {'benefits from' if farm_profile.has_irrigation else 'requires'} irrigation systems in {farm_profile.soil_type} soil.",
            "Alfalfa": f"Alfalfa prefers {farm_profile.soil_type} with good depth. pH {farm_profile.soil_ph} {'optimal' if ph_diff < 0.4 else 'may need amendment'}.",
            "Sorghum": f"Sorghum is drought-tolerant in {farm_profile.soil_type}. {'Irrigation adds yield security' if farm_profile.has_irrigation else 'Suitable for dryland farming'}.",
            "Sunflower": f"Sunflowers perform well in {farm_profile.soil_type} with moderate fertility. Deep taproot accesses subsoil moisture.",
        }
        
        results.append(CropResult(
            crop_name=crop["name"],
            expected_profit=round(expected_profit, 2),
            profit_p10=round(profit_p10, 2),
            profit_p50=round(profit_p50, 2),
            profit_p90=round(profit_p90, 2),
            yield_forecast=round(base_yield, 1),
            price_forecast=round(base_price, 2),
            soil_compatibility=round(soil_compat * 100, 1),
            risk_score=round(risk_score, 1),
            risk_level=risk_level,
            soil_explanation=soil_explanations.get(crop["name"], f"{crop['name']} is compatible with {farm_profile.soil_type} soil.")
        ))
    
    # Sort by expected profit (descending)
    results.sort(key=lambda x: x.expected_profit, reverse=True)
    return results

def generate_weather_summary(location: FarmLocation) -> str:
    summaries = [
        "Historical data indicates favorable growing conditions with adequate precipitation patterns. 30-year average shows reliable frost-free periods.",
        "NOAA climate analysis suggests moderate drought risk. Consider irrigation-ready crops or drought-tolerant varieties.",
        "Weather patterns show above-average precipitation expected. Plan for crops that tolerate wet conditions or ensure proper drainage.",
        "Temperature trends indicate earlier spring onset. Extended growing season may allow for double-cropping opportunities.",
    ]
    return random.choice(summaries)

def generate_market_outlook() -> str:
    outlooks = [
        "USDA AMS data shows strengthening commodity prices driven by global demand. Export markets remain robust.",
        "Market analysis indicates stable pricing with slight upward pressure from reduced planted acres nationwide.",
        "Futures markets suggest volatility ahead. Diversification recommended to hedge against price swings.",
        "Strong domestic demand combined with favorable export conditions support premium pricing opportunities.",
    ]
    return random.choice(outlooks)

# ============ Routes ============

@api_router.get("/")
async def root():
    return {"message": "Crop Portfolio Optimizer API", "version": "1.0.0"}

# Auth Routes
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

@api_router.get("/auth/me", response_model=UserResponse)
async def get_me(current_user: dict = Depends(get_current_user)):
    return UserResponse(
        id=current_user["id"],
        email=current_user["email"],
        name=current_user["name"],
        created_at=current_user["created_at"]
    )

# Analysis Routes
@api_router.post("/analysis/create", response_model=AnalysisResponse)
async def create_analysis(data: AnalysisCreate, current_user: dict = Depends(get_current_user)):
    analysis_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    
    # Generate mock results
    results = generate_mock_crop_results(data.farm_profile)
    weather_summary = generate_weather_summary(data.farm_profile.location)
    market_outlook = generate_market_outlook()
    
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
    
    return AnalysisResponse(
        id=analysis_id,
        user_id=current_user["id"],
        farm_profile=data.farm_profile,
        results=results,
        weather_summary=weather_summary,
        market_outlook=market_outlook,
        created_at=created_at,
        status="completed"
    )

@api_router.get("/analysis/{analysis_id}", response_model=AnalysisResponse)
async def get_analysis(analysis_id: str, current_user: dict = Depends(get_current_user)):
    analysis = await db_get_analysis(analysis_id, current_user["id"])
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    
    return AnalysisResponse(**analysis)

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

@app.on_event("shutdown")
async def shutdown_db_client():
    if client:
        client.close()
