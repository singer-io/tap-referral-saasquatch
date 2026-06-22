class ReferralSaasquatchError(Exception):
    """Base exception for tap-referral-saasquatch."""


class ReferralSaasquatchForbiddenError(ReferralSaasquatchError):
    """Raised when credentials do not have stream read access."""
