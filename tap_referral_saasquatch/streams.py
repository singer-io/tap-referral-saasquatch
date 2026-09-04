import singer

from tap_referral_saasquatch.exceptions import ReferralSaasquatchError, ReferralSaasquatchForbiddenError


LOGGER = singer.get_logger()


class BaseStream:
    name = None

    def __init__(self, client=None, catalog=None):
        self.client = client
        self.catalog = catalog

    def check_access(self) -> bool:
        try:
            has_access = self.client.probe_stream_access(self.name)
        except ReferralSaasquatchForbiddenError as err:
            LOGGER.warning(
                "Unauthorized stream '%s' excluded from catalog. HTTP status: 403. Error: %s",
                self.name,
                err,
            )
            return False

        if not has_access:
            LOGGER.warning(
                "Unauthorized stream '%s' excluded from catalog. HTTP status: 403. Error: insufficient permissions",
                self.name,
            )
            return False

        return True


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
