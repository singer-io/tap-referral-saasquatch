class ReferralSaasquatchError(Exception):
    """Base exception for tap-referral-saasquatch."""


class ReferralSaasquatchForbiddenError(ReferralSaasquatchError):
    """Raised when credentials do not have stream read access."""


class ReferralSaasquatchAuthenticationError(ReferralSaasquatchError):
    """Raised when the configured credentials cannot be authenticated."""
