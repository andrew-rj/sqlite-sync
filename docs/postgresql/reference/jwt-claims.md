# JWT Claims Reference

## Standard JWT Claims

| Claim | Needed? | Purpose |
|-------|---------|---------|
| `sub` | ✅ Yes | User ID |
| `email` | ✅ Yes | User email |
| `role` | ✅ Yes | Permission level |
| `iss` | ✅ Yes | Issuer (validated in app) |
| `aud` | ✅ Yes | Audience (validated in app) |
| `iat` | ⚠️ Maybe | Issued at timestamp |
| `exp` | ✅ Yes | Expiration (validated in app) |

## Custom Claims Examples

| Claim | Use Case |
|-------|----------|
| `org_id` | Multi-tenant apps |
| `team_id` | Team-based access |
| `permissions` | Fine-grained access |
| `scope` | OAuth scopes |
| `department_id` | Department-based filtering |
| `is_admin` | Admin flag |

---

## How RLS Works with JWT Claims

**Flow:**
```
1. Client sends JWT token to CloudSync
2. CloudSync validates JWT and extracts claims
3. CloudSync passes claims to PostgreSQL as session variables
4. RLS policies read session variables via current_setting()
5. Policies filter data based on claims
6. Only authorized rows returned to client
```

---

## How CloudSync Passes JWT Claims to PostgreSQL

**CloudSync validates the JWT and converts all claims to JSON, then passes as a PostgreSQL session variable:**

```go
// CloudSync (internal implementation)
userData := token.Claims  // map[string]any with all JWT claims
claimJSON, _ := json.Marshal(userData)

// Pass all claims as JSON to PostgreSQL session
db.Exec(
  `SELECT set_config('role', 'authenticated', true),
          set_config('request.jwt.claims', $1, true)`,
  string(claimJSON)
)
```

**Result:** All JWT claims available in PostgreSQL as JSON in `request.jwt.claims`

**Example:** If JWT contains:
```json
{
  "sub": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "role": "authenticated",
  "org_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}
```

Then in PostgreSQL:
```sql
-- Returns: {"sub":"550e8400...","email":"user@example.com","role":"authenticated","org_id":"aaaaaaaa..."}
current_setting('request.jwt.claims')

-- Access any claim from the JSON
user_id = (current_setting('request.jwt.claims')::jsonb->>'sub')::uuid
email = (current_setting('request.jwt.claims')::jsonb->>'email')
role = (current_setting('request.jwt.claims')::jsonb->>'role')
org_id = (current_setting('request.jwt.claims')::jsonb->>'org_id')::uuid
```

---

## Optional: Helper Functions for JWT Claims

CloudSync validates JWTs and passes all claims to PostgreSQL via `request.jwt.claims` — no PostgreSQL extension is required for JWT verification. The validation happens entirely in the CloudSync microservice.

However, writing `(current_setting('request.jwt.claims')::jsonb->>'sub')::uuid` in every RLS policy is verbose. Following the pattern used by Supabase and Neon, you can optionally create a small set of helper functions in a dedicated schema:

```sql
-- Create a schema for auth helpers (optional, but keeps things clean)
CREATE SCHEMA IF NOT EXISTS auth;

-- Returns all JWT claims as JSONB
CREATE OR REPLACE FUNCTION auth.session()
  RETURNS jsonb AS $$
    SELECT current_setting('request.jwt.claims', true)::jsonb;
$$ LANGUAGE SQL STABLE;

-- Returns the user ID (sub claim)
CREATE OR REPLACE FUNCTION auth.user_id()
  RETURNS text AS $$
    SELECT auth.session()->>'sub';
$$ LANGUAGE SQL STABLE;

-- Returns the user's role claim
CREATE OR REPLACE FUNCTION auth.role()
  RETURNS text AS $$
    SELECT auth.session()->>'role';
$$ LANGUAGE SQL STABLE;
```

> **Note:** These are just convenience wrappers — they read from the same `request.jwt.claims` session variable that CloudSync sets.

---
