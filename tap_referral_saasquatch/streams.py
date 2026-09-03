import singer

from tap_referral_saasquatch.exceptions import ReferralSaasquatchError, ReferralSaasquatchForbiddenError


LOGGER = singer.get_logger()


class BaseStream:
    name = None

    def __init__(self, client=None, catalog=None):
        self.client = client
        self.catalog = catalog

    def check_access(self) -> bool:
        if self.client is None:
            return True
        try:
            return self.client.probe_stream_access(self.name)
        except ReferralSaasquatchForbiddenError as err:
            LOGGER.warning(
                "Unauthorized Stream: %s, excluding from catalog. HTTP-Error-Message:'%s'",
                self.__class__.__name__,
                err,
            )
            return False
        except Exception as err:
            LOGGER.error(
                "Failed to check access for stream '%s': %s",
                self.name,
                err,
            )
            raise ReferralSaasquatchError(
                "HTTP-error-code: 500, Error: Failed to check stream access for '{}'".format(self.name)
            ) from err


class Referrals(BaseStream):
    name = "referrals"
    key_properties = ["id"]
    replication_keys = ("dateReferralStarted",)
    replication_method = "INCREMENTAL"


class RewardBalances(BaseStream):
    name = "reward_balances"
    key_properties = ["userId", "accountId"]
    replication_keys = None
    replication_method = "FULL_TABLE"


class Users(BaseStream):
    name = "users"
    key_properties = ["id", "accountId"]
    replication_keys = ("dateCreated",)
    replication_method = "INCREMENTAL"


STREAMS = {
    "referrals": Referrals,
    "reward_balances": RewardBalances,
    "users": Users
}
