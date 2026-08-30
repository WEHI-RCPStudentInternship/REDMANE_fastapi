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
    AUTH0_ISSUER = f"https://{AUTH0_DOMAIN}/"

bearer_scheme = HTTPBearer()


@lru_cache(maxsize=1)
def get_jwks():
    if "keycloak" in AUTH0_DOMAIN or "8080" in AUTH0_DOMAIN:
        url = "http://keycloak:8080/realms/redmane/protocol/openid-connect/certs"
    else:
        url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    print(f"Fetching JWKS from: {url}", flush=True)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_public_key(token: str):
    jwks = get_jwks()
    header = jwt.get_unverified_header(token)
    print(f"Token kid: {header['kid']}", flush=True)
    print(f"Available kids: {[k['kid'] for k in jwks['keys']]}", flush=True)
    key = next((k for k in jwks["keys"] if k["kid"] == header["kid"]), None)
    if key is None:
        raise HTTPException(status_code=401, detail="Signing key not found")
    return key


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme)
):
    token = credentials.credentials
    get_jwks.cache_clear()
    try:
        public_key = get_public_key(token)
        print(f"Issuer: {AUTH0_ISSUER}", flush=True)
        print(f"Audience: {AUTH0_AUDIENCE}", flush=True)
        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            audience=AUTH0_AUDIENCE,
            issuer=AUTH0_ISSUER
        )
        return payload
    except JWTError as e:
        print(f"JWT Error: {e}", flush=True)
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")
    except Exception as e:
        print(f"Other Error: {e}", flush=True)
        raise HTTPException(status_code=401, detail=f"Error: {str(e)}")


@router.get("/auth/")
async def get_user(token: dict = Depends(verify_token)):
    return {
        "user_id": token.get("sub"),
        "email": token.get("email"),
    }