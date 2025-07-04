class Referrals:
    name = "referrals"
    key_properties = ["id"]
    replication_keys = "dateReferralStarted"
    replication_method = "INCREMENTAL"


class RewardBalances:
    name = "reward_balances"
    key_properties = ["userId"]
    replication_keys = None
    replication_method = "INCREMENTAL"


class Users:
    name = "users"
    key_properties = ["id"]
    replication_keys = "dateCreated"
    replication_method = "INCREMENTAL"


STREAMS = {
    "referrals": Referrals,
    "reward_balances": RewardBalances,
    "users": Users
}
