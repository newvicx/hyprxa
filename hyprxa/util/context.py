from hyprxa._context import user_context



def get_user_identity() -> str | None:
    """Get user identity from context."""
    user = user_context.get()
    if user is not None:
        return user.identity