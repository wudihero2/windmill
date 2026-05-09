# Chapter 12: Access Token + Refresh Token Authentication

## Problem

The original design uses a single 24-hour JWT. This has three issues:

1. **Long exposure window** — A stolen token is valid for 24 hours
2. **No revocation** — Logout cannot invalidate a token (JWT is stateless)
3. **No session tracking** — No DB record of active sessions

## Solution: Dual Token Architecture

```
Access token:  short-lived JWT (15 min), stateless, Bearer header
Refresh token: long-lived opaque UUID (7 days), HttpOnly Cookie, DB-backed
```

Access token validation requires **no DB lookup** (signature + exp check only).
Refresh token is **hashed (SHA-256) before storing** — a DB breach does not expose raw tokens.

---

## Schema

### `session` table

```sql
CREATE TABLE session (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    email       VARCHAR(255) NOT NULL REFERENCES account(email) ON DELETE CASCADE,
    token_hash  VARCHAR(64)  NOT NULL UNIQUE,  -- SHA-256 hex of raw refresh token
    expires_at  TIMESTAMPTZ  NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ  DEFAULT NULL      -- NULL = active, non-NULL = revoked
);

CREATE INDEX idx_session_email ON session(email);
CREATE INDEX idx_session_token_hash ON session(token_hash) WHERE revoked_at IS NULL;
```

Design notes:
- `token_hash` stores SHA-256 hex (64 chars) of the raw UUID token, not the token itself
- `revoked_at` is nullable timestamp instead of boolean — provides both state and audit trail
- Partial index `WHERE revoked_at IS NULL` speeds up the common lookup path (active sessions only)
- `ON DELETE CASCADE` on email — deleting an account automatically purges all sessions

---

## Token Design

### Access Token (JWT)

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Claims {
    email: String,  // subject (who)
    exp: u64,       // expires at (15 min from issuance)
    iat: u64,       // issued at
}
```

- Algorithm: HS256 (HMAC-SHA256)
- TTL: **15 minutes** (`ACCESS_TOKEN_TTL_SECS = 900`)
- Signed with `JWT_SECRET` environment variable
- Validated by `require_auth` middleware — decode + check `exp`, no DB lookup
- Carried in `Authorization: Bearer <token>` header

### Refresh Token

- Format: UUID v4 (opaque string, not a JWT)
- TTL: **7 days** (`REFRESH_TOKEN_TTL_SECS = 604_800`)
- Stored in DB as SHA-256 hash
- Delivered via `Set-Cookie` header with security attributes
- **Never exposed in JSON response body**

### Cookie Attributes

```
Set-Cookie: refresh_token=<uuid>; HttpOnly; Secure; SameSite=Strict; Path=/api/auth; Max-Age=604800
```

| Attribute | Purpose |
|-----------|---------|
| `HttpOnly` | JavaScript cannot read it (XSS protection) |
| `Secure` | Only sent over HTTPS |
| `SameSite=Strict` | CSRF protection |
| `Path=/api/auth` | Cookie only sent to auth endpoints, not every API call |
| `Max-Age=604800` | 7 days, matches DB expiration |

---

## Token Rotation

Each `POST /api/auth/refresh` call **rotates** the refresh token:

1. Look up old `token_hash` in `session` (must be active and not expired)
2. Revoke old session: `SET revoked_at = now()`
3. Generate new UUID refresh token
4. Insert new session row with new hash
5. Set new refresh token cookie
6. Return new access token

Why rotate:
- If attacker steals and uses a refresh token first, the legitimate user's next refresh fails (token revoked) — anomaly detected
- If the legitimate user uses it first, the attacker's stolen token is already invalid
- Defense-in-depth against token theft

Race condition: Two concurrent requests with the same refresh token — one succeeds, one gets 401. This is the desired behavior. Client handles 401 on refresh by redirecting to login.

---

## API Changes

### Response Type

```rust
#[derive(serde::Serialize)]
pub struct AuthResponse {
    pub access_token: String,
    pub expires_in: u64,  // seconds, always 900
}
```

Replaces the old `LoginResponse { token }`. The refresh token is **only** in the `Set-Cookie` header.

### Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/auth/login` | POST | None | Verify password, return access token + set refresh cookie |
| `/api/auth/signup` | POST | None | Create account, auto-login (same response as login) |
| `/api/auth/refresh` | POST | Cookie | Rotate refresh token, return new access token |
| `/api/auth/logout` | POST | Cookie | Revoke refresh token, clear cookie |

### `POST /api/auth/login`

Request:
```json
{ "email": "alice@example.com", "password": "..." }
```

Response (200):
```json
{ "access_token": "eyJhbG...", "expires_in": 900 }
```
```
Set-Cookie: refresh_token=<uuid>; HttpOnly; Secure; SameSite=Strict; Path=/api/auth; Max-Age=604800
```

### `POST /api/auth/refresh`

Request: No body needed. Refresh token sent automatically via cookie.

Response (200): Same shape as login.
```json
{ "access_token": "eyJhbG...", "expires_in": 900 }
```
```
Set-Cookie: refresh_token=<new-uuid>; HttpOnly; Secure; ...
```

Error (401): Token expired, revoked, or missing.

### `POST /api/auth/logout`

Request: No body needed.

Response (200):
```json
{ "message": "logged out" }
```
```
Set-Cookie: refresh_token=; HttpOnly; Secure; SameSite=Strict; Path=/api/auth; Max-Age=0
```

The logout endpoint does **not** require an access token. Even if the access token has expired, the user should still be able to log out (clear the refresh token).

---

## Token Flow Diagrams

### Login

```
Client                           Server                          DB
  │                              │                                │
  │── POST /api/auth/login ─────>│                                │
  │   { email, password }        │── SELECT account ─────────────>│
  │                              │<── password_hash ──────────────│
  │                              │   Argon2 verify                │
  │                              │── INSERT session ─────────────>│
  │                              │   (email, sha256(uuid), exp)   │
  │<──── 200 OK ─────────────────│                                │
  │   Body: { access_token, expires_in: 900 }                     │
  │   Set-Cookie: refresh_token=<uuid>; HttpOnly; Secure; ...     │
```

### Authenticated API Request

```
Client                           Server
  │                              │
  │── GET /api/workspaces/... ──>│
  │   Authorization: Bearer <access_token>
  │                              │  decode JWT, check exp
  │                              │  (no DB lookup for the token)
  │<──── 200 OK ─────────────────│
```

### Refresh (access token expired)

```
Client                           Server                          DB
  │                              │                                │
  │── POST /api/auth/refresh ───>│                                │
  │   Cookie: refresh_token=<old_uuid>                             │
  │                              │── SELECT session ─────────────>│
  │                              │   WHERE hash = sha256(old)      │
  │                              │   AND revoked_at IS NULL        │
  │                              │   AND expires_at > now()        │
  │                              │<── session row ────────────────│
  │                              │── UPDATE SET revoked_at ──────>│
  │                              │   (revoke old token)            │
  │                              │── INSERT session ─────────────>│
  │                              │   (new token hash)              │
  │<──── 200 OK ─────────────────│                                │
  │   Body: { access_token, expires_in: 900 }                     │
  │   Set-Cookie: refresh_token=<new_uuid>; HttpOnly; Secure; ... │
```

### Logout

```
Client                           Server                          DB
  │                              │                                │
  │── POST /api/auth/logout ────>│                                │
  │   Cookie: refresh_token=<uuid>                                 │
  │                              │── UPDATE SET revoked_at ──────>│
  │<──── 200 OK ─────────────────│                                │
  │   Set-Cookie: refresh_token=; Max-Age=0                        │
```

---

## Session Management Utilities

### Revoke all sessions (password change, "sign out all devices")

```rust
pub async fn revoke_all_sessions(db: &PgPool, email: &str) -> Result<u64, ApiError> {
    let result = sqlx::query!(
        "UPDATE session SET revoked_at = now() WHERE email = $1 AND revoked_at IS NULL",
        email
    )
    .execute(db)
    .await?;
    Ok(result.rows_affected())
}
```

Call this on:
- Password change / password reset
- Explicit "sign out all devices" action

### Cleanup expired sessions (background job)

```sql
DELETE FROM session
WHERE expires_at < now() - INTERVAL '1 day'
   OR (revoked_at IS NOT NULL AND revoked_at < now() - INTERVAL '30 days');
```

---

## Implementation Helpers

No extra crate needed for cookies — use `axum::http::header::SET_COOKIE` directly.

SHA-256 hashing uses the `sha2` crate (add to workspace dependencies).

```rust
fn sha256_hex(input: &str) -> String {
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}
```

Cookie parsing — manual, no dependency:

```rust
fn extract_refresh_token(headers: &HeaderMap) -> Option<String> {
    headers.get(COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').map(str::trim)
                .find_map(|c| c.strip_prefix("refresh_token=").map(String::from))
        })
}
```

---

## Security Considerations

| Threat | Mitigation |
|--------|-----------|
| XSS steals token | Refresh token in HttpOnly cookie (JS cannot read). Access token is short-lived (15 min window). |
| CSRF | `SameSite=Strict` on cookie. Refresh/logout endpoints don't use Bearer header. |
| DB breach | Only SHA-256 hashes stored, not raw tokens. |
| Token replay | Refresh token rotation — used token is immediately revoked. |
| Long-lived session after logout | Refresh token revoked in DB on logout. Access token expires in 15 min max. |
| Password change | `revoke_all_sessions()` invalidates all refresh tokens. Access tokens expire naturally in 15 min. |

---

## Files Changed

| File | Action |
|------|--------|
| `Cargo.toml` (workspace) | Add `sha2 = "0.10"` to workspace deps |
| `crates/api/Cargo.toml` | Add `sha2 = { workspace = true }` |
| `migrations/20250509000001_session.up.sql` | New: session table |
| `migrations/20250509000001_session.down.sql` | New: drop session table |
| `crates/api/src/auth.rs` | Update Claims, response type, login/signup; add refresh/logout/helpers |
| `crates/api/src/lib.rs` | Add `/api/auth/refresh` and `/api/auth/logout` routes |
