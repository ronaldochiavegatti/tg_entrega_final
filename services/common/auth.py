from typing import List

from fastapi import Header, HTTPException, status


def require_roles(allowed_roles: List[str]):
    allowed = {role.lower() for role in allowed_roles}

    def checker(x_user_role: str = Header(..., alias="X-User-Role")) -> str:
        role = (x_user_role or "").lower()
        if role not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="forbidden")
        return role

    return checker
