from dotenv import load_dotenv
from pathlib import Path

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

import os
import uuid
import logging
import bcrypt
import jwt
import secrets
import requests
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, APIRouter, HTTPException, Depends, Request, Response, UploadFile, File, Form, Query, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse, Response as FastAPIResponse
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field, EmailStr, ConfigDict
import io
import asyncio

try:
    import resend
    HAS_RESEND = True
except ImportError:
    HAS_RESEND = False

# ============ Config ============
JWT_ALGORITHM = "HS256"
JWT_SECRET = os.environ["JWT_SECRET"]
APP_NAME = os.environ.get("APP_NAME", "dilse")
STORAGE_URL = "https://integrations.emergentagent.com/objstore/api/v1/storage"
EMERGENT_KEY = os.environ.get("EMERGENT_LLM_KEY")

# ============ DB ============
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# ============ App ============
app = FastAPI(title="DilSe API")
api_router = APIRouter(prefix="/api")

# ============ CORS Config ============
def _parse_origins() -> list[str]:
    raw = os.environ.get("CORS_ORIGINS", "")
    if raw.strip():
        return [origin.strip().rstrip("/") for origin in raw.split(",") if origin.strip()]
    return [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]

ALLOWED_ORIGINS = _parse_origins()


# ============ Storage ============
storage_key: Optional[str] = None

def init_storage():
    global storage_key
    if storage_key:
        return storage_key
    resp = requests.post(f"{STORAGE_URL}/init", json={"emergent_key": EMERGENT_KEY}, timeout=30)
    resp.raise_for_status()
    storage_key = resp.json()["storage_key"]
    return storage_key

def put_object(path: str, data: bytes, content_type: str) -> dict:
    key = init_storage()
    resp = requests.put(
        f"{STORAGE_URL}/objects/{path}",
        headers={"X-Storage-Key": key, "Content-Type": content_type},
        data=data, timeout=120
    )
    resp.raise_for_status()
    return resp.json()

def get_object(path: str):
    key = init_storage()
    resp = requests.get(
        f"{STORAGE_URL}/objects/{path}",
        headers={"X-Storage-Key": key}, timeout=60
    )
    resp.raise_for_status()
    return resp.content, resp.headers.get("Content-Type", "application/octet-stream")

# ============ WebSocket Manager ============
class ConnectionManager:
    def __init__(self):
        # user_id -> list[WebSocket]
        self.user_conns: Dict[str, List[WebSocket]] = {}

    async def connect(self, user_id: str, ws: WebSocket):
        await ws.accept()
        self.user_conns.setdefault(user_id, []).append(ws)

    def disconnect(self, user_id: str, ws: WebSocket):
        if user_id in self.user_conns:
            try:
                self.user_conns[user_id].remove(ws)
            except ValueError:
                pass
            if not self.user_conns[user_id]:
                del self.user_conns[user_id]

    async def send_to_user(self, user_id: str, payload: dict):
        if user_id not in self.user_conns:
            return
        dead = []
        for ws in self.user_conns[user_id]:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.disconnect(user_id, d)

ws_manager = ConnectionManager()


# ============ Email Service ============
RESEND_API_KEY = os.environ.get("RESEND_API_KEY")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL", "onboarding@resend.dev")
APP_URL = os.environ.get("APP_URL", "https://dilse.app")

if HAS_RESEND and RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY


def email_template(title: str, body_html: str, cta_label: Optional[str] = None, cta_url: Optional[str] = None) -> str:
    cta_block = ""
    if cta_label and cta_url:
        cta_block = f"""
        <table cellpadding="0" cellspacing="0" border="0" align="center" style="margin: 32px auto;">
            <tr><td style="background: linear-gradient(90deg, #FF2A85 0%, #8B5CF6 100%); border-radius: 999px; padding: 14px 32px;">
                <a href="{cta_url}" style="color: #ffffff; text-decoration: none; font-weight: 700; font-family: sans-serif; font-size: 14px;">{cta_label}</a>
            </td></tr>
        </table>
        """
    return f"""<!doctype html>
<html><body style="margin:0; background:#09090b; font-family: -apple-system, BlinkMacSystemFont, sans-serif; color:#ffffff;">
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#09090b; padding: 40px 16px;">
  <tr><td align="center">
    <table cellpadding="0" cellspacing="0" border="0" width="600" style="max-width:600px; background:#18181b; border:1px solid rgba(255,255,255,0.08); border-radius: 24px; padding: 40px;">
      <tr><td align="center" style="padding-bottom: 24px;">
        <div style="display:inline-block; background: linear-gradient(135deg, #FF2A85 0%, #8B5CF6 100%); width: 56px; height: 56px; border-radius: 16px; line-height: 56px; font-size: 28px;">❤</div>
        <div style="font-size: 22px; font-weight: 700; color:#ffffff; margin-top: 12px;">DilSe</div>
        <div style="font-size: 10px; color: #71717a; letter-spacing: 2px; text-transform: uppercase;">Premium Rishta & Dating</div>
      </td></tr>
      <tr><td style="color:#ffffff; font-size: 24px; font-weight: 700; padding-bottom: 16px;">{title}</td></tr>
      <tr><td style="color:#a1a1aa; font-size: 14px; line-height: 1.6;">{body_html}</td></tr>
      <tr><td>{cta_block}</td></tr>
      <tr><td style="color:#52525b; font-size: 11px; padding-top: 24px; border-top: 1px solid rgba(255,255,255,0.05); margin-top: 32px;">
        © 2026 DilSe. Made with ♥ for love. <br/>
        If you didn't request this email, you can safely ignore it.
      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>"""


async def send_email(to_email: str, subject: str, html: str) -> bool:
    """Send email via Resend (non-blocking). Returns True on success."""
    if not HAS_RESEND or not RESEND_API_KEY:
        logging.warning(f"[Email Mock] To={to_email} | Subject={subject}")
        return False
    try:
        params = {"from": SENDER_EMAIL, "to": [to_email], "subject": subject, "html": html}
        result = await asyncio.to_thread(resend.Emails.send, params)
        logging.info(f"Email sent to {to_email}: {result.get('id')}")
        return True
    except Exception as e:
        logging.error(f"Email send error: {e}")
        return False


# ============ Auth Helpers ============

IS_PROD = os.environ.get("ENV", "").lower() == "production"
COOKIE_KW = {
    "httponly": True,
    "secure": IS_PROD,
    "samesite": "none" if IS_PROD else "lax",
    "max_age": 604800,
    "path": "/",
}

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False

def create_access_token(user_id: str, email: str, role: str = "user") -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(days=7),
        "type": "access"
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def decode_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except Exception:
        return None

async def get_current_user(request: Request) -> dict:
    token = request.cookies.get("access_token")
    if not token:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Invalid token")
    user = await db.users.find_one({"id": payload["sub"]}, {"password_hash": 0})
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    user.pop("_id", None)
    return user

async def get_current_admin(request: Request) -> dict:
    user = await get_current_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

# ============ Models ============
class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    name: str
    age: int
    gender: str

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str

class ProfileUpdate(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = None
    age: Optional[int] = None
    gender: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    profession: Optional[str] = None
    education: Optional[str] = None
    religion: Optional[str] = None
    sect: Optional[str] = None
    height: Optional[str] = None
    marital_status: Optional[str] = None
    interests: Optional[List[str]] = None
    bio: Optional[str] = None
    looking_for: Optional[str] = None  # dating | rishta | both
    mode: Optional[str] = None  # dating | rishta
    family_details: Optional[Dict[str, Any]] = None
    wali_required: Optional[bool] = None
    invisible_browsing: Optional[bool] = None

class SwipeAction(BaseModel):
    target_user_id: str
    action: str  # like | pass | superlike

class MessageCreate(BaseModel):
    match_id: str
    text: Optional[str] = None
    voice_path: Optional[str] = None
    type: str = "text"  # text | voice

class RishtaRequestCreate(BaseModel):
    target_user_id: str
    message: Optional[str] = None

class AIMatchRequest(BaseModel):
    target_user_id: str

class CheckoutRequest(BaseModel):
    plan_id: str
    origin_url: str

# ============ Utility ============
def serialize_user(user: dict, full: bool = False) -> dict:
    if not user:
        return {}
    user.pop("_id", None)
    user.pop("password_hash", None)
    if not full:
        user.pop("email", None)
    return user

def public_profile(user: dict) -> dict:
    return {
        "id": user.get("id"),
        "name": user.get("name"),
        "age": user.get("age"),
        "gender": user.get("gender"),
        "city": user.get("city"),
        "country": user.get("country"),
        "profession": user.get("profession"),
        "education": user.get("education"),
        "religion": user.get("religion"),
        "sect": user.get("sect"),
        "height": user.get("height"),
        "marital_status": user.get("marital_status"),
        "interests": user.get("interests", []),
        "bio": user.get("bio"),
        "photos": user.get("photos", []),
        "voice_intro": user.get("voice_intro"),
        "verified": user.get("verified", False),
        "premium": user.get("premium", False),
        "mode": user.get("mode", "dating"),
        "looking_for": user.get("looking_for", "both"),
        "family_details": user.get("family_details") if user.get("mode") == "rishta" else None,
        "wali_required": user.get("wali_required", False),
        "online": user.get("online", False),
        "last_seen": user.get("last_seen"),
    }

# ============ AUTH ============
@api_router.post("/auth/register")
async def register(req: RegisterRequest, response: Response):
    email = req.email.lower().strip()
    existing = await db.users.find_one({"email": email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    user_doc = {
        "id": user_id,
        "email": email,
        "password_hash": hash_password(req.password),
        "name": req.name,
        "age": req.age,
        "gender": req.gender,
        "role": "user",
        "verified": False,
        "premium": False,
        "premium_until": None,
        "approved": True,
        "banned": False,
        "mode": "dating",
        "looking_for": "both",
        "photos": [],
        "voice_intro": None,
        "interests": [],
        "bio": "",
        "city": "",
        "country": "",
        "profession": "",
        "education": "",
        "religion": "",
        "sect": "",
        "height": "",
        "marital_status": "Single",
        "family_details": {},
        "wali_required": False,
        "invisible_browsing": False,
        "online": True,
        "last_seen": now,
        "created_at": now,
    }
    await db.users.insert_one(user_doc)
    token = create_access_token(user_id, email, "user")
    response.set_cookie("access_token", token, **COOKIE_KW)
    # Welcome email (non-blocking)
    asyncio.create_task(send_email(
        email,
        "Welcome to DilSe 💖",
        email_template(
            f"Welcome, {req.name}!",
            "Your DilSe account is ready. Complete your profile with photos and a voice intro to get the best matches. Our AI is ready to find your perfect match.",
            "Complete Your Profile",
            f"{APP_URL}/profile/me",
        )
    ))
    return {"token": token, "user": serialize_user(user_doc, full=True)}

@api_router.post("/auth/login")
async def login(req: LoginRequest, response: Response):
    email = req.email.lower().strip()
    user = await db.users.find_one({"email": email})
    if not user or not verify_password(req.password, user.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if user.get("banned"):
        raise HTTPException(status_code=403, detail="Account is banned")
    token = create_access_token(user["id"], email, user.get("role", "user"))
    response.set_cookie("access_token", token, **COOKIE_KW)
    await db.users.update_one({"id": user["id"]}, {"$set": {"online": True, "last_seen": datetime.now(timezone.utc).isoformat()}})
    return {"token": token, "user": serialize_user(user, full=True)}

@api_router.post("/auth/logout")
async def logout(response: Response, current_user: dict = Depends(get_current_user)):
    await db.users.update_one({"id": current_user["id"]}, {"$set": {"online": False, "last_seen": datetime.now(timezone.utc).isoformat()}})
    response.delete_cookie("access_token", path="/", samesite=COOKIE_KW["samesite"], secure=COOKIE_KW["secure"])
    return {"ok": True}

@api_router.get("/auth/me")
async def me(current_user: dict = Depends(get_current_user)):
    return {"user": serialize_user(current_user, full=True)}

@api_router.post("/auth/forgot-password")
async def forgot_password(req: ForgotPasswordRequest):
    email = req.email.lower().strip()
    user = await db.users.find_one({"email": email})
    if user:
        token = secrets.token_urlsafe(32)
        await db.password_reset_tokens.insert_one({
            "token": token,
            "user_id": user["id"],
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
            "used": False,
        })
        reset_url = f"{APP_URL}/reset-password?token={token}"
        # Send email
        asyncio.create_task(send_email(
            email,
            "Reset your DilSe password",
            email_template(
                "Reset your password",
                f"We received a request to reset your password. Click the button below to choose a new one. This link expires in 1 hour.",
                "Reset Password",
                reset_url,
            )
        ))
        logging.warning(f"[DilSe] Password reset link for {email}: /reset-password?token={token}")
    return {"ok": True, "message": "If the email exists, a reset link has been sent."}

@api_router.post("/auth/reset-password")
async def reset_password(req: ResetPasswordRequest):
    record = await db.password_reset_tokens.find_one({"token": req.token, "used": False})
    if not record:
        raise HTTPException(status_code=400, detail="Invalid or used token")
    if datetime.fromisoformat(record["expires_at"]) < datetime.now(timezone.utc):
        raise HTTPException(status_code=400, detail="Token expired")
    await db.users.update_one(
        {"id": record["user_id"]},
        {"$set": {"password_hash": hash_password(req.new_password)}}
    )
    await db.password_reset_tokens.update_one({"token": req.token}, {"$set": {"used": True}})
    return {"ok": True}

# ============ PROFILE ============
@api_router.put("/profile")
async def update_profile(updates: ProfileUpdate, current_user: dict = Depends(get_current_user)):
    update_data = {k: v for k, v in updates.model_dump().items() if v is not None}
    if update_data:
        await db.users.update_one({"id": current_user["id"]}, {"$set": update_data})
    user = await db.users.find_one({"id": current_user["id"]}, {"password_hash": 0})
    return {"user": serialize_user(user, full=True)}

@api_router.get("/profile/{user_id}")
async def get_profile(user_id: str, current_user: dict = Depends(get_current_user)):
    user = await db.users.find_one({"id": user_id}, {"password_hash": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    # Track visitor (skip if user has invisible browsing enabled)
    if user_id != current_user["id"] and not current_user.get("invisible_browsing"):
        await db.visitors.insert_one({
            "id": str(uuid.uuid4()),
            "profile_id": user_id,
            "visitor_id": current_user["id"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    profile = public_profile(user)
    # Check if matched / liked
    match = await db.matches.find_one({
        "$or": [
            {"user_a": current_user["id"], "user_b": user_id},
            {"user_a": user_id, "user_b": current_user["id"]}
        ]
    })
    profile["is_matched"] = bool(match)
    profile["match_id"] = match.get("id") if match else None
    liked = await db.swipes.find_one({"user_id": current_user["id"], "target_user_id": user_id, "action": {"$in": ["like", "superlike"]}})
    profile["i_liked"] = bool(liked)
    return profile

# ============ UPLOADS ============
@api_router.post("/upload/photo")
async def upload_photo(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    ext = (file.filename or "img.jpg").split(".")[-1].lower()
    path = f"{APP_NAME}/users/{current_user['id']}/photos/{uuid.uuid4()}.{ext}"
    data = await file.read()
    result = put_object(path, data, file.content_type or "image/jpeg")
    # Add to user's photos array
    photo_id = str(uuid.uuid4())
    photo_obj = {
        "id": photo_id,
        "path": result["path"],
        "private": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.users.update_one({"id": current_user["id"]}, {"$push": {"photos": photo_obj}})
    return photo_obj

@api_router.delete("/upload/photo/{photo_id}")
async def delete_photo(photo_id: str, current_user: dict = Depends(get_current_user)):
    await db.users.update_one({"id": current_user["id"]}, {"$pull": {"photos": {"id": photo_id}}})
    return {"ok": True}

@api_router.post("/upload/voice")
async def upload_voice(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    ext = (file.filename or "voice.webm").split(".")[-1].lower()
    path = f"{APP_NAME}/users/{current_user['id']}/voice/{uuid.uuid4()}.{ext}"
    data = await file.read()
    result = put_object(path, data, file.content_type or "audio/webm")
    voice_obj = {
        "path": result["path"],
        "duration": 30,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.users.update_one({"id": current_user["id"]}, {"$set": {"voice_intro": voice_obj}})
    return voice_obj

@api_router.get("/files/{path:path}")
async def serve_file(path: str, auth: Optional[str] = Query(None), request: Request = None):
    # Validate auth via query or header
    token = auth
    if not token:
        auth_header = request.headers.get("Authorization", "") if request else ""
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        elif request:
            token = request.cookies.get("access_token")
    if not token or not decode_token(token):
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        data, content_type = get_object(path)
        return FastAPIResponse(content=data, media_type=content_type)
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")

# ============ DISCOVER / SWIPE ============
@api_router.get("/discover")
async def discover(
    current_user: dict = Depends(get_current_user),
    mode: str = Query("dating"),
    min_age: int = Query(18),
    max_age: int = Query(99),
    city: Optional[str] = Query(None),
    religion: Optional[str] = Query(None),
):
    # Get user IDs already swiped
    swipes = await db.swipes.find({"user_id": current_user["id"]}).to_list(10000)
    swiped_ids = [s["target_user_id"] for s in swipes]
    swiped_ids.append(current_user["id"])
    query: Dict[str, Any] = {
        "id": {"$nin": swiped_ids},
        "banned": {"$ne": True},
        "approved": True,
        "age": {"$gte": min_age, "$lte": max_age},
    }
    # Filter by opposite gender for rishta/dating norms (optional)
    user_gender = current_user.get("gender", "").lower()
    if user_gender in ["male", "female"]:
        opp = "female" if user_gender == "male" else "male"
        query["gender"] = {"$regex": f"^{opp}$", "$options": "i"}
    if city:
        query["city"] = {"$regex": city, "$options": "i"}
    if religion:
        query["religion"] = religion
    profiles = await db.users.find(query, {"password_hash": 0}).limit(50).to_list(50)
    return {"profiles": [public_profile(p) for p in profiles]}

@api_router.post("/swipe")
async def swipe(action: SwipeAction, current_user: dict = Depends(get_current_user)):
    # Premium check for unlimited likes (basic users limited to 20/day)
    if not current_user.get("premium"):
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        count = await db.swipes.count_documents({
            "user_id": current_user["id"],
            "action": {"$in": ["like", "superlike"]},
            "created_at": {"$gte": today_start.isoformat()}
        })
        if count >= 20 and action.action in ["like", "superlike"]:
            raise HTTPException(status_code=403, detail="Daily like limit reached. Upgrade to Premium for unlimited likes!")
    
    swipe_doc = {
        "id": str(uuid.uuid4()),
        "user_id": current_user["id"],
        "target_user_id": action.target_user_id,
        "action": action.action,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.swipes.insert_one(swipe_doc)
    
    # Check for match
    is_match = False
    match_id = None
    if action.action in ["like", "superlike"]:
        reverse = await db.swipes.find_one({
            "user_id": action.target_user_id,
            "target_user_id": current_user["id"],
            "action": {"$in": ["like", "superlike"]}
        })
        if reverse:
            match_id = str(uuid.uuid4())
            await db.matches.insert_one({
                "id": match_id,
                "user_a": current_user["id"],
                "user_b": action.target_user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            is_match = True
    return {"ok": True, "is_match": is_match, "match_id": match_id}

@api_router.get("/likes/received")
async def likes_received(current_user: dict = Depends(get_current_user)):
    if not current_user.get("premium"):
        # Return only count for non-premium
        count = await db.swipes.count_documents({
            "target_user_id": current_user["id"],
            "action": {"$in": ["like", "superlike"]}
        })
        return {"count": count, "profiles": [], "blurred": True}
    swipes = await db.swipes.find({
        "target_user_id": current_user["id"],
        "action": {"$in": ["like", "superlike"]}
    }).to_list(100)
    user_ids = [s["user_id"] for s in swipes]
    users = await db.users.find({"id": {"$in": user_ids}}, {"password_hash": 0}).to_list(100)
    return {"count": len(users), "profiles": [public_profile(u) for u in users], "blurred": False}

@api_router.get("/visitors")
async def visitors(current_user: dict = Depends(get_current_user)):
    visits = await db.visitors.find({"profile_id": current_user["id"]}).sort("created_at", -1).limit(50).to_list(50)
    user_ids = list({v["visitor_id"] for v in visits})
    users = await db.users.find({"id": {"$in": user_ids}}, {"password_hash": 0}).to_list(100)
    return {"profiles": [public_profile(u) for u in users]}

@api_router.get("/matches")
async def get_matches(current_user: dict = Depends(get_current_user)):
    matches = await db.matches.find({
        "$or": [
            {"user_a": current_user["id"]},
            {"user_b": current_user["id"]}
        ]
    }).sort("created_at", -1).to_list(200)
    result = []
    for m in matches:
        other_id = m["user_b"] if m["user_a"] == current_user["id"] else m["user_a"]
        other = await db.users.find_one({"id": other_id}, {"password_hash": 0})
        if not other:
            continue
        last_msg = await db.messages.find_one({"match_id": m["id"]}, sort=[("created_at", -1)])
        result.append({
            "match_id": m["id"],
            "user": public_profile(other),
            "last_message": last_msg.get("text") if last_msg else None,
            "last_message_at": last_msg.get("created_at") if last_msg else m["created_at"],
            "created_at": m["created_at"],
        })
    return {"matches": result}

# ============ MESSAGES ============
@api_router.post("/messages")
async def send_message(msg: MessageCreate, current_user: dict = Depends(get_current_user)):
    match = await db.matches.find_one({"id": msg.match_id})
    if not match or current_user["id"] not in [match["user_a"], match["user_b"]]:
        raise HTTPException(status_code=403, detail="Not authorized for this match")
    msg_doc = {
        "id": str(uuid.uuid4()),
        "match_id": msg.match_id,
        "sender_id": current_user["id"],
        "text": msg.text,
        "voice_path": msg.voice_path,
        "type": msg.type,
        "read": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.messages.insert_one(msg_doc)
    msg_doc.pop("_id", None)
    # Notify the other user via WebSocket
    other_id = match["user_b"] if match["user_a"] == current_user["id"] else match["user_a"]
    asyncio.create_task(ws_manager.send_to_user(other_id, {"type": "message", "data": msg_doc}))
    asyncio.create_task(ws_manager.send_to_user(current_user["id"], {"type": "message", "data": msg_doc}))
    return msg_doc

@api_router.get("/messages/{match_id}")
async def get_messages(match_id: str, current_user: dict = Depends(get_current_user)):
    match = await db.matches.find_one({"id": match_id})
    if not match or current_user["id"] not in [match["user_a"], match["user_b"]]:
        raise HTTPException(status_code=403, detail="Not authorized")
    msgs = await db.messages.find({"match_id": match_id}).sort("created_at", 1).to_list(1000)
    # Mark as read
    await db.messages.update_many(
        {"match_id": match_id, "sender_id": {"$ne": current_user["id"]}, "read": False},
        {"$set": {"read": True}}
    )
    for m in msgs:
        m.pop("_id", None)
    return {"messages": msgs}

@api_router.post("/messages/voice")
async def upload_voice_message(
    match_id: str = Form(...),
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    match = await db.matches.find_one({"id": match_id})
    if not match or current_user["id"] not in [match["user_a"], match["user_b"]]:
        raise HTTPException(status_code=403, detail="Not authorized")
    ext = (file.filename or "msg.webm").split(".")[-1].lower()
    path = f"{APP_NAME}/messages/{match_id}/{uuid.uuid4()}.{ext}"
    data = await file.read()
    result = put_object(path, data, file.content_type or "audio/webm")
    msg_doc = {
        "id": str(uuid.uuid4()),
        "match_id": match_id,
        "sender_id": current_user["id"],
        "text": None,
        "voice_path": result["path"],
        "type": "voice",
        "read": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.messages.insert_one(msg_doc)
    msg_doc.pop("_id", None)
    return msg_doc

# ============ AI MATCHMAKING ============
def calculate_compatibility(a: dict, b: dict) -> dict:
    scores = {}
    # Interests overlap
    a_int = set([i.lower() for i in a.get("interests", []) if i])
    b_int = set([i.lower() for i in b.get("interests", []) if i])
    if a_int or b_int:
        union = len(a_int | b_int)
        scores["interests"] = round(len(a_int & b_int) / union * 100) if union else 50
    else:
        scores["interests"] = 50
    # Education match
    edu_levels = {"high school": 1, "bachelor": 2, "bs": 2, "ba": 2, "master": 3, "ms": 3, "ma": 3, "mbbs": 3, "phd": 4, "md": 4}
    def edu_score(e):
        e = (e or "").lower()
        for k, v in edu_levels.items():
            if k in e:
                return v
        return 2
    diff = abs(edu_score(a.get("education", "")) - edu_score(b.get("education", "")))
    scores["education"] = max(50, 100 - diff * 15)
    # Religion match
    scores["religion"] = 100 if (a.get("religion") and a.get("religion") == b.get("religion")) else 60
    # Lifestyle (city proximity)
    scores["lifestyle"] = 90 if a.get("city") == b.get("city") else 70
    # Age compatibility
    age_diff = abs((a.get("age") or 25) - (b.get("age") or 25))
    scores["personality"] = max(50, 100 - age_diff * 5)
    overall = round(sum(scores.values()) / len(scores))
    return {"overall": overall, "breakdown": scores}

@api_router.post("/ai/compatibility")
async def ai_compatibility(req: AIMatchRequest, current_user: dict = Depends(get_current_user)):
    target = await db.users.find_one({"id": req.target_user_id}, {"password_hash": 0})
    if not target:
        raise HTTPException(status_code=404, detail="User not found")
    compat = calculate_compatibility(current_user, target)
    # AI-powered insight
    insight = None
    try:
        from emergentintegrations.llm.chat import LlmChat, UserMessage
        chat = LlmChat(
            api_key=EMERGENT_KEY,
            session_id=f"match-{current_user['id']}-{target['id']}",
            system_message="You are a warm, insightful AI matchmaker for DilSe (a premium South Asian rishta and dating platform). Give a 2-3 sentence compatibility insight that highlights strengths and shared values. Keep it positive, respectful, and culturally sensitive."
        ).with_model("openai", "gpt-5.4-mini")
        prompt = f"""User A: {current_user.get('name')}, {current_user.get('age')}y, {current_user.get('profession')}, {current_user.get('education')}, {current_user.get('religion')}, interests: {', '.join(current_user.get('interests', []))[:200]}. Bio: {(current_user.get('bio') or '')[:200]}.
User B: {target.get('name')}, {target.get('age')}y, {target.get('profession')}, {target.get('education')}, {target.get('religion')}, interests: {', '.join(target.get('interests', []))[:200]}. Bio: {(target.get('bio') or '')[:200]}.
Compatibility scores: {compat['breakdown']}. Overall: {compat['overall']}%.
Provide a 2-3 sentence compatibility insight."""
        insight = await chat.send_message(UserMessage(text=prompt))
    except Exception as e:
        logging.error(f"AI compatibility error: {e}")
        insight = f"You share {compat['breakdown']['interests']}% interest alignment and strong values compatibility. A promising match worth exploring!"
    return {"compatibility": compat, "insight": insight, "target": public_profile(target)}

@api_router.get("/ai/recommendations")
async def ai_recommendations(current_user: dict = Depends(get_current_user)):
    # Find top compatible profiles
    swipes = await db.swipes.find({"user_id": current_user["id"]}).to_list(10000)
    swiped_ids = [s["target_user_id"] for s in swipes] + [current_user["id"]]
    user_gender = current_user.get("gender", "").lower()
    query: Dict[str, Any] = {
        "id": {"$nin": swiped_ids},
        "banned": {"$ne": True},
        "approved": True,
    }
    if user_gender in ["male", "female"]:
        opp = "female" if user_gender == "male" else "male"
        query["gender"] = {"$regex": f"^{opp}$", "$options": "i"}
    candidates = await db.users.find(query, {"password_hash": 0}).limit(40).to_list(40)
    scored = []
    for c in candidates:
        compat = calculate_compatibility(current_user, c)
        scored.append({"profile": public_profile(c), "compatibility": compat})
    scored.sort(key=lambda x: x["compatibility"]["overall"], reverse=True)
    return {"recommendations": scored[:12]}

# ============ RISHTA REQUESTS ============
@api_router.post("/rishta/request")
async def rishta_request(req: RishtaRequestCreate, current_user: dict = Depends(get_current_user)):
    existing = await db.rishta_requests.find_one({
        "from_user": current_user["id"],
        "to_user": req.target_user_id,
        "status": "pending"
    })
    if existing:
        raise HTTPException(status_code=400, detail="Request already sent")
    doc = {
        "id": str(uuid.uuid4()),
        "from_user": current_user["id"],
        "to_user": req.target_user_id,
        "message": req.message or "",
        "status": "pending",  # pending | accepted | rejected
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.rishta_requests.insert_one(doc)
    doc.pop("_id", None)
    return doc

@api_router.get("/rishta/requests")
async def list_rishta_requests(current_user: dict = Depends(get_current_user)):
    incoming = await db.rishta_requests.find({"to_user": current_user["id"]}).sort("created_at", -1).to_list(100)
    outgoing = await db.rishta_requests.find({"from_user": current_user["id"]}).sort("created_at", -1).to_list(100)
    async def hydrate(reqs, key):
        for r in reqs:
            r.pop("_id", None)
            user = await db.users.find_one({"id": r[key]}, {"password_hash": 0})
            r["user"] = public_profile(user) if user else None
        return reqs
    return {
        "incoming": await hydrate(incoming, "from_user"),
        "outgoing": await hydrate(outgoing, "to_user"),
    }

@api_router.put("/rishta/request/{request_id}")
async def respond_rishta(request_id: str, status: str, current_user: dict = Depends(get_current_user)):
    if status not in ["accepted", "rejected"]:
        raise HTTPException(status_code=400, detail="Invalid status")
    req = await db.rishta_requests.find_one({"id": request_id})
    if not req or req["to_user"] != current_user["id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    await db.rishta_requests.update_one({"id": request_id}, {"$set": {"status": status}})
    # Create match if accepted
    if status == "accepted":
        match_id = str(uuid.uuid4())
        await db.matches.insert_one({
            "id": match_id,
            "user_a": req["from_user"],
            "user_b": req["to_user"],
            "type": "rishta",
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
    return {"ok": True}

# ============ PREMIUM / MANUAL BANK PAYMENT ============
PREMIUM_PLANS = {
    "silver": {
        "id": "silver",
        "name": "Silver VIP",
        "amount": 499,
        "currency": "pkr",
        "days": 30,
        "features": [
            "Unlimited Likes",
            "5 Daily Super Likes",
            "See Who Liked You",
            "Profile Boost 1x Weekly",
            "Premium Badge",
        ],
    },
    "gold": {
        "id": "gold",
        "name": "Gold VIP",
        "amount": 999,
        "currency": "pkr",
        "days": 30,
        "features": [
            "Everything in Silver",
            "Unlimited Super Likes",
            "Unlimited Chat Access",
            "AI Match Priority",
            "Private Gallery",
            "Invisible Browsing",
            "Profile Boost 3x Weekly",
            "Gold Verified Badge",
        ],
    },
}

PAYMENT_METHODS = [
    {"id": "nayapay", "name": "NayaPay", "title": "Sadam Hussain", "account": "03423642324", "type": "wallet"},
    {"id": "hbl", "name": "HBL Bank", "title": "Sadam Hussain", "account": "04557901780603", "type": "bank"},
]


@api_router.get("/premium/plans")
async def get_plans():
    return {"plans": list(PREMIUM_PLANS.values()), "payment_methods": PAYMENT_METHODS}


class PaymentSubmission(BaseModel):
    plan_id: str
    method_id: str
    transaction_id: Optional[str] = None
    screenshot_path: Optional[str] = None


@api_router.post("/premium/submit-payment")
async def submit_payment(req: PaymentSubmission, current_user: dict = Depends(get_current_user)):
    if req.plan_id not in PREMIUM_PLANS:
        raise HTTPException(status_code=400, detail="Invalid plan")
    if req.method_id not in {m["id"] for m in PAYMENT_METHODS}:
        raise HTTPException(status_code=400, detail="Invalid payment method")
    plan = PREMIUM_PLANS[req.plan_id]
    doc = {
        "id": str(uuid.uuid4()),
        "user_id": current_user["id"],
        "user_email": current_user.get("email"),
        "user_name": current_user.get("name"),
        "plan_id": req.plan_id,
        "amount": plan["amount"],
        "currency": plan["currency"],
        "method_id": req.method_id,
        "transaction_id": req.transaction_id,
        "screenshot_path": req.screenshot_path,
        "status": "pending",  # pending | approved | rejected
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.payment_submissions.insert_one(doc)
    doc.pop("_id", None)
    return {"ok": True, "submission": doc}


@api_router.post("/premium/upload-screenshot")
async def upload_payment_screenshot(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    ext = (file.filename or "proof.jpg").split(".")[-1].lower()
    path = f"{APP_NAME}/payments/{current_user['id']}/{uuid.uuid4()}.{ext}"
    data = await file.read()
    put_object(path, data, file.content_type or "image/jpeg")
    return {"path": path}


@api_router.get("/premium/my-submissions")
async def my_submissions(current_user: dict = Depends(get_current_user)):
    subs = await db.payment_submissions.find({"user_id": current_user["id"]}).sort("created_at", -1).to_list(50)
    for s in subs:
        s.pop("_id", None)
    return {"submissions": subs}


@api_router.get("/admin/payments")
async def admin_payments(admin: dict = Depends(get_current_admin), status: Optional[str] = Query(None)):
    query = {}
    if status:
        query["status"] = status
    subs = await db.payment_submissions.find(query).sort("created_at", -1).to_list(500)
    for s in subs:
        s.pop("_id", None)
    return {"submissions": subs}


@api_router.put("/admin/payments/{submission_id}")
async def admin_review_payment(submission_id: str, action: str, admin: dict = Depends(get_current_admin)):
    if action not in ["approve", "reject"]:
        raise HTTPException(status_code=400, detail="Invalid action")
    sub = await db.payment_submissions.find_one({"id": submission_id})
    if not sub:
        raise HTTPException(status_code=404, detail="Not found")
    new_status = "approved" if action == "approve" else "rejected"
    await db.payment_submissions.update_one({"id": submission_id}, {"$set": {"status": new_status, "reviewed_at": datetime.now(timezone.utc).isoformat(), "reviewed_by": admin["id"]}})
    if action == "approve":
        plan = PREMIUM_PLANS.get(sub["plan_id"])
        if plan:
            premium_until = (datetime.now(timezone.utc) + timedelta(days=plan["days"])).isoformat()
            await db.users.update_one(
                {"id": sub["user_id"]},
                {"$set": {"premium": True, "premium_until": premium_until, "premium_tier": sub["plan_id"]}}
            )
            # Notify user via email
            asyncio.create_task(send_email(
                sub.get("user_email", ""),
                "🎉 Your DilSe Premium is Active!",
                email_template(
                    f"Welcome to {plan['name']}!",
                    f"Your payment has been approved. You now have full access to all premium features for 30 days. Find your perfect match faster with priority AI matchmaking, unlimited likes, and more.",
                    "Open DilSe",
                    f"{APP_URL}/dashboard",
                )
            ))
    return {"ok": True}


# ============ POPUP MESSAGES ============
class PopupMessageCreate(BaseModel):
    type: str  # success | error | warning | promo
    title: str
    body: str
    cta_label: Optional[str] = None
    cta_url: Optional[str] = None
    active: bool = True
    show_once: bool = True


@api_router.get("/popup")
async def get_active_popup():
    doc = await db.popup_messages.find_one({"active": True}, sort=[("created_at", -1)])
    if not doc:
        return {"popup": None}
    doc.pop("_id", None)
    return {"popup": doc}


@api_router.get("/admin/popups")
async def admin_list_popups(admin: dict = Depends(get_current_admin)):
    popups = await db.popup_messages.find({}).sort("created_at", -1).to_list(50)
    for p in popups:
        p.pop("_id", None)
    return {"popups": popups}


@api_router.post("/admin/popups")
async def admin_create_popup(req: PopupMessageCreate, admin: dict = Depends(get_current_admin)):
    if req.active:
        # Deactivate other popups
        await db.popup_messages.update_many({}, {"$set": {"active": False}})
    doc = {**req.model_dump(), "id": str(uuid.uuid4()), "created_at": datetime.now(timezone.utc).isoformat()}
    await db.popup_messages.insert_one(doc)
    doc.pop("_id", None)
    return doc


@api_router.delete("/admin/popups/{popup_id}")
async def admin_delete_popup(popup_id: str, admin: dict = Depends(get_current_admin)):
    await db.popup_messages.delete_one({"id": popup_id})
    return {"ok": True}


# ============ ADMIN ============
@api_router.get("/admin/stats")
async def admin_stats(admin: dict = Depends(get_current_admin)):
    total_users = await db.users.count_documents({})
    verified = await db.users.count_documents({"verified": True})
    premium = await db.users.count_documents({"premium": True})
    banned = await db.users.count_documents({"banned": True})
    matches = await db.matches.count_documents({})
    messages = await db.messages.count_documents({})
    revenue_docs = await db.payment_transactions.find({"payment_status": "paid"}).to_list(10000)
    revenue = sum(d.get("amount", 0) for d in revenue_docs)
    return {
        "total_users": total_users,
        "verified_users": verified,
        "premium_users": premium,
        "banned_users": banned,
        "total_matches": matches,
        "total_messages": messages,
        "total_revenue": round(revenue, 2),
    }

@api_router.get("/admin/users")
async def admin_users(admin: dict = Depends(get_current_admin), q: Optional[str] = None):
    query: Dict[str, Any] = {}
    if q:
        query["$or"] = [
            {"name": {"$regex": q, "$options": "i"}},
            {"email": {"$regex": q, "$options": "i"}},
        ]
    users = await db.users.find(query, {"password_hash": 0}).sort("created_at", -1).limit(200).to_list(200)
    for u in users:
        u.pop("_id", None)
    return {"users": users}

@api_router.put("/admin/users/{user_id}")
async def admin_update_user(user_id: str, action: str, admin: dict = Depends(get_current_admin)):
    updates: Dict[str, Any] = {}
    if action == "verify":
        updates["verified"] = True
    elif action == "unverify":
        updates["verified"] = False
    elif action == "ban":
        updates["banned"] = True
    elif action == "unban":
        updates["banned"] = False
    elif action == "approve":
        updates["approved"] = True
    elif action == "grant_premium":
        updates["premium"] = True
        updates["premium_until"] = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    else:
        raise HTTPException(status_code=400, detail="Invalid action")
    await db.users.update_one({"id": user_id}, {"$set": updates})
    return {"ok": True}

# ============ STARTUP ============
DEFAULT_LOGO_URL = "https://customer-assets.emergentagent.com/job_dilse-premium/artifacts/8c0kbul1_WhatsApp%20Image%202026-05-26%20at%2012.30.49%20AM.jpeg"


# ============ BRANDING ============
@api_router.get("/branding")
async def get_branding():
    doc = await db.branding.find_one({"key": "default"})
    if not doc:
        return {"logo_url": DEFAULT_LOGO_URL, "brand_name": "DilSe", "tagline": "Premium Rishta & Dating"}
    doc.pop("_id", None)
    return {
        "logo_url": doc.get("logo_url") or DEFAULT_LOGO_URL,
        "brand_name": doc.get("brand_name") or "DilSe",
        "tagline": doc.get("tagline") or "Premium Rishta & Dating",
    }


class BrandingUpdate(BaseModel):
    logo_url: Optional[str] = None
    brand_name: Optional[str] = None
    tagline: Optional[str] = None


@api_router.put("/admin/branding")
async def update_branding(req: BrandingUpdate, admin: dict = Depends(get_current_admin)):
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if updates:
        await db.branding.update_one(
            {"key": "default"},
            {"$set": {**updates, "key": "default", "updated_at": datetime.now(timezone.utc).isoformat()}},
            upsert=True,
        )
    doc = await db.branding.find_one({"key": "default"})
    doc.pop("_id", None)
    return {"ok": True, "branding": doc}


@api_router.post("/admin/branding/upload")
async def upload_branding_logo(file: UploadFile = File(...), admin: dict = Depends(get_current_admin)):
    ext = (file.filename or "logo.png").split(".")[-1].lower()
    path = f"{APP_NAME}/branding/logo-{uuid.uuid4()}.{ext}"
    data = await file.read()
    put_object(path, data, file.content_type or "image/png")
    # Build a public URL via files endpoint with admin token
    logo_url = f"/api/files/{path}"
    await db.branding.update_one(
        {"key": "default"},
        {"$set": {"logo_url": logo_url, "logo_path": path, "key": "default"}},
        upsert=True,
    )
    return {"logo_url": logo_url, "path": path}


@app.on_event("startup")
async def startup_event():
    await db.hero_slides.create_index([("order", 1)])
    await db.hero_slides.create_index([("enabled", 1), ("order", 1)])
    # Indexes
    await db.users.create_index("email", unique=True)
    await db.users.create_index("id", unique=True)
    await db.swipes.create_index([("user_id", 1), ("target_user_id", 1)])
    await db.matches.create_index([("user_a", 1), ("user_b", 1)])
    await db.messages.create_index([("match_id", 1), ("created_at", 1)])
    # Seed admin
    admin_email = os.environ.get("ADMIN_EMAIL", "admin@dilse.app")
    admin_password = os.environ.get("ADMIN_PASSWORD", "DilSeAdmin@2026")
    existing = await db.users.find_one({"email": admin_email})
    if not existing:
        await db.users.insert_one({
            "id": str(uuid.uuid4()),
            "email": admin_email,
            "password_hash": hash_password(admin_password),
            "name": "DilSe Admin",
            "age": 30,
            "gender": "other",
            "role": "admin",
            "verified": True,
            "premium": True,
            "approved": True,
            "banned": False,
            "mode": "dating",
            "photos": [],
            "interests": [],
            "bio": "Admin account",
            "city": "",
            "country": "",
            "online": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        logging.info(f"Seeded admin: {admin_email}")
    else:
        # Update password if changed
        if not verify_password(admin_password, existing.get("password_hash", "")):
            await db.users.update_one({"email": admin_email}, {"$set": {"password_hash": hash_password(admin_password)}})
    # Init storage
    try:
        init_storage()
        logging.info("Storage initialized")
    except Exception as e:
        logging.error(f"Storage init failed: {e}")
    # Seed default branding if not exists
    existing_brand = await db.branding.find_one({"key": "default"})
    if not existing_brand:
        await db.branding.insert_one({
            "key": "default",
            "logo_url": DEFAULT_LOGO_URL,
            "brand_name": "DilSe",
            "tagline": "Premium Rishta & Dating",
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

@api_router.get("/")
async def root():
    return {"app": "DilSe", "status": "running"}


# ============ WebSocket ============
@app.websocket("/api/ws/chat")
async def chat_websocket(ws: WebSocket, token: str = Query(...)):
    payload = decode_token(token)
    if not payload or payload.get("type") != "access":
        await ws.close(code=1008)
        return
    user_id = payload["sub"]
    await ws_manager.connect(user_id, ws)
    try:
        # Mark user online
        await db.users.update_one({"id": user_id}, {"$set": {"online": True}})
        while True:
            data = await ws.receive_json()
            # Handle typing indicator
            if data.get("type") == "typing":
                match_id = data.get("match_id")
                if match_id:
                    match = await db.matches.find_one({"id": match_id})
                    if match and user_id in [match["user_a"], match["user_b"]]:
                        other = match["user_b"] if match["user_a"] == user_id else match["user_a"]
                        await ws_manager.send_to_user(other, {"type": "typing", "match_id": match_id, "user_id": user_id})
            elif data.get("type") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    finally:
        ws_manager.disconnect(user_id, ws)
        if user_id not in ws_manager.user_conns:
            await db.users.update_one({"id": user_id}, {"$set": {"online": False, "last_seen": datetime.now(timezone.utc).isoformat()}})

# Register routes

# ============ HERO SLIDER ============
class HeroSlideIn(BaseModel):
    badge: Optional[str] = ""
    title: str
    description: Optional[str] = ""
    cta1_text: Optional[str] = "Get Started"
    cta1_link: Optional[str] = "/register"
    cta2_text: Optional[str] = "Explore"
    cta2_link: Optional[str] = "/discover"
    image_url: Optional[str] = ""
    floating_card_text: Optional[str] = ""
    compatibility_score: Optional[str] = "98%"
    voice_intro_text: Optional[str] = ""
    background_gradient: Optional[str] = "from-[#FF2A85]/20 to-[#8B5CF6]/20"
    enabled: Optional[bool] = True
    order: Optional[int] = 0

class HeroSlideUpdate(BaseModel):
    badge: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    cta1_text: Optional[str] = None
    cta1_link: Optional[str] = None
    cta2_text: Optional[str] = None
    cta2_link: Optional[str] = None
    image_url: Optional[str] = None
    floating_card_text: Optional[str] = None
    compatibility_score: Optional[str] = None
    voice_intro_text: Optional[str] = None
    background_gradient: Optional[str] = None
    enabled: Optional[bool] = None
    order: Optional[int] = None

def serialize_hero_slide(doc: dict) -> dict:
    if not doc:
        return {}
    doc.pop("_id", None)
    return doc

@api_router.get("/hero/slides")
async def public_hero_slides():
    slides = await db.hero_slides.find({"enabled": True}, {"_id": 0}).sort("order", 1).to_list(50)
    return {"slides": slides}

@api_router.get("/admin/hero/slides")
async def admin_hero_slides(current_admin: dict = Depends(get_current_admin)):
    slides = await db.hero_slides.find({}, {"_id": 0}).sort("order", 1).to_list(100)
    return {"slides": slides}

@api_router.post("/admin/hero/slides")
async def create_hero_slide(payload: HeroSlideIn, current_admin: dict = Depends(get_current_admin)):
    data = payload.model_dump()
    data["id"] = str(uuid.uuid4())
    data["created_at"] = datetime.now(timezone.utc).isoformat()
    data["updated_at"] = data["created_at"]
    if data.get("order") is None:
        count = await db.hero_slides.count_documents({})
        data["order"] = count
    await db.hero_slides.insert_one(data)
    data.pop("_id", None)
    return {"slide": data}

@api_router.put("/admin/hero/slides/{slide_id}")
async def update_hero_slide(slide_id: str, payload: HeroSlideUpdate, current_admin: dict = Depends(get_current_admin)):
    update_data = {k: v for k, v in payload.model_dump().items() if v is not None}
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    result = await db.hero_slides.update_one({"id": slide_id}, {"$set": update_data})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Hero slide not found")
    slide = await db.hero_slides.find_one({"id": slide_id}, {"_id": 0})
    return {"slide": slide}

@api_router.delete("/admin/hero/slides/{slide_id}")
async def delete_hero_slide(slide_id: str, current_admin: dict = Depends(get_current_admin)):
    result = await db.hero_slides.delete_one({"id": slide_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Hero slide not found")
    return {"ok": True}

@api_router.post("/admin/hero/slides/{slide_id}/toggle")
async def toggle_hero_slide(slide_id: str, current_admin: dict = Depends(get_current_admin)):
    slide = await db.hero_slides.find_one({"id": slide_id})
    if not slide:
        raise HTTPException(status_code=404, detail="Hero slide not found")
    enabled = not bool(slide.get("enabled", True))
    await db.hero_slides.update_one({"id": slide_id}, {"$set": {"enabled": enabled, "updated_at": datetime.now(timezone.utc).isoformat()}})
    return {"ok": True, "enabled": enabled}

@api_router.post("/admin/hero/slides/reorder")
async def reorder_hero_slides(order: List[str], current_admin: dict = Depends(get_current_admin)):
    for idx, slide_id in enumerate(order):
        await db.hero_slides.update_one({"id": slide_id}, {"$set": {"order": idx, "updated_at": datetime.now(timezone.utc).isoformat()}})
    return {"ok": True}

@api_router.post("/admin/hero/upload")
async def upload_hero_image(file: UploadFile = File(...), current_admin: dict = Depends(get_current_admin)):
    ext = (file.filename or "hero.jpg").split(".")[-1].lower()
    if ext not in ["jpg", "jpeg", "png", "webp", "gif"]:
        raise HTTPException(status_code=400, detail="Only image files are allowed")
    path = f"{APP_NAME}/hero/{uuid.uuid4()}.{ext}"
    data = await file.read()
    if len(data) > 8 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Image too large. Max 8MB allowed.")
    result = put_object(path, data, file.content_type or "image/jpeg")
    return {"url": f"/api/files/{result['path']}", "path": result["path"]}



app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_router)


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
