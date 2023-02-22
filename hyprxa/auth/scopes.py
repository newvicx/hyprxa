from collections.abc import Collection

from fastapi import HTTPException, status
from fastapi.requests import HTTPConnection
from starlette.authentication import AuthCredentials
from starlette.types import Scope

from hyprxa.auth.models import BaseUser
from hyprxa._exceptions import NotConfiguredError



class requires:
    """Dependency for route authorization.
    
    The `AuthenticationMiddleware` must be installed in order to use this class.
    If the middleware is not installed, a `NotConfiguredError` is raised.

    Args:
        scopes: The required scopes to access the route.
        any_: If `True` and the user has any of the scopes, authorization will
            succeed. Otherwise, the user will need all scopes.
        raise_on_no_scopes: If `True` a `NotConfigured` error will be raised
            when a route with no required scopes is hit.

    Raises:
        HTTPException: 401 if user is unauthenticated, 403 if not authorized.

    Examples:
    Depending on how you've structured your API, the most common way to use `requires`
    is in a router dependency...
    >>> router = APIRouter(prefix="/admin", dependencies=[Depends(requires(["ADMIN"]))])
    Now all users will require the 'ADMIN' scope to access any route tied to
    this router.
    
    You can also layer scopes down to the route...
    >>> @router.post("/changepassword", dependencies=[Depends(requires(["SUPERADMIN"]))])
    ... async def change_password(...):
    ...     # You now need to have 'ADMIN' and 'SUPERADMIN' scopes to access this
    ...     ...
    
    Your organization may have layered permissions that grant increasing
    amounts of access (eg. READ, READ/WRITE, ADMIN). All scopes allow reading
    data in this case but the user may only belong to one group (eg. READ/WRITE).
    For this we can specify that a user have 'all' or 'any' of the scopes...
    >>> @router.get("/useraccounts", dependencies=[
    ...    Depends(requires(["READ", "READ/WRITE", "ADMIN"], any_=True))
    ... ])
    ... async def get_user(...):
    ...     # You only need to have one of (READ, READ/WRITE, ADMIN) to access
    ...     # this route
    ...     ...

    If no scopes are provided, the user must be authenticated but they do not
    require any specific scopes to access the route...
    >>> router_unprotected = APIRouter(prefix="/user", dependencies=[Depends(requires())])
    ... @router_unprotected.get("/me")
    ... async def get_me(...):
    ...     # The user only need be authenticated to access this route
    ...     ...

    The exception to this is if `raise_on_no_scopes=True`. Then, providing no
    scopes will raise a `NotConfiguredError`...
    >>> @router.get("/usercreds", dependencies=[Depends(requires(raise_on_no_scopes=True))])
    ... async def get_creds(...):
    ...     # This will raise a `NotConfigured` error because no scopes were
    ...     # provided
    ...     ...

    `requires` returns the user model when used as a dependency in a path
    operation...
    >>> @router.get(/me)
    ... async def get_me(user: BaseUser = requires()) -> str:
    ...     return user.username

    Note: For websocket routes you must pass `requires` as a dependency directly
    into the path operation. This is because websocket routes do not propagate
    dependencies from the parent router. For more information see
    [4957](https://github.com/tiangolo/fastapi/issues/4957).
    """
    def __init__(
        self,
        scopes: Collection[str] = [],
        any_: bool = False,
        raise_on_no_scopes: bool = False
    ) -> None:
        self.scopes = set(scopes)
        self.any = any_
        self.raise_on_no_scopes = raise_on_no_scopes

    async def __call__(self, connection: HTTPConnection) -> BaseUser:
        scope: Scope = connection.scope
        
        if scope["type"] not in ("http", "websocket"):
            raise RuntimeError("'requires' used outside of 'http', 'websocket' scope.")
        if "user" not in scope:
            raise NotConfiguredError("Authentication middleware not installed.")

        user: BaseUser = scope["user"]
        scopes = scope["auth"]

        if isinstance(scopes, AuthCredentials):
            scopes = scopes.scopes
        
        assert isinstance(scopes, Collection), f"Unhandled type for scopes {type(scopes)}"

        if not user.is_authenticated:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not Authenticated."
            )

        if not self.scopes:
            if self.raise_on_no_scopes:
                raise NotConfiguredError("No scopes specified.")
            return user
        
        if self.any and any([scope in scopes for scope in self.scopes]):
            return user
        elif all([scope in scopes for scope in self.scopes]):
            return user
        else:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User does not have the required scopes to access this resource."
            )