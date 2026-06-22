class BaseStream:
    name = None
    parent = None

    def __init__(self, client=None, catalog=None):
        self.client = client
        self.catalog = catalog

    def check_access(self) -> bool:
        if self.parent:
            return True
        if self.client is None:
            return True
        return self.client.probe_stream_access(self.name)


class Referrals(BaseStream):
    name = "referrals"
    key_properties = ["id"]
    replication_keys = "dateReferralStarted"
    replication_method = "INCREMENTAL"


class RewardBalances(BaseStream):
    name = "reward_balances"
    key_properties = ["userId", "accountId"]
    replication_keys = None
    replication_method = "FULL_TABLE"


class Users(BaseStream):
    name = "users"
    key_properties = ["id", "accountId"]
    replication_keys = "dateCreated"
    replication_method = "INCREMENTAL"


STREAMS = {
    "referrals": Referrals,
    "reward_balances": RewardBalances,
    "users": Users
}
