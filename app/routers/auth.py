import os
import requests
from functools import lru_cache
from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt

router = APIRouter()

AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")
AUTH0_ISSUER = os.getenv("AUTH0_ISSUER")

if not AUTH0_DOMAIN:
    raise RuntimeError("AUTH0_DOMAIN environment variable is not set")
if not AUTH0_AUDIENCE:
    raise RuntimeError("AUTH0_AUDIENCE environment variable is not set")
if not AUTH0_ISSUER:
    raise RuntimeError("AUTH0_ISSUER environment variable is not set")

bearer_scheme = HTTPBearer()


@lru_cache(maxsize=1)
def get_jwks():
    url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_public_key(token: str):
    jwks = get_jwks()
    header = jwt.get_unverified_header(token)
    key = next((k for k in jwks["keys"] if k["kid"] == header["kid"]), None)
    if key is None:
        raise HTTPException(status_code=401, detail="Signing key not found")
    return key


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)
):
    token = credentials.credentials
    try:
        public_key = get_public_key(token)
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=AUTH0_AUDIENCE,
            issuer=AUTH0_ISSUER
        )
        return payload
    except JWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")


@router.get("/auth/")
async def get_user(token: dict = Depends(verify_token)):
    return {
        "user_id": token.get("sub"),
        "email": token.get("email"),
    }