class Referrals:
    name = "referrals"
    key_properties = ["id"]
    replication_keys = "dateReferralStarted"
    replication_method = "INCREMENTAL"


class RewardBalances:
    name = "reward_balances"
    key_properties = ["userId", "accountId"]
    replication_keys = None
    replication_method = "FULL_TABLE"


class Users:
    name = "users"
    key_properties = ["id", "accountId"]
    replication_keys = "dateCreated"
    replication_method = "INCREMENTAL"


STREAMS = {
    "referrals": Referrals,
    "reward_balances": RewardBalances,
    "users": Users
}
