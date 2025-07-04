# tap_referral_saasquatch/streams.py

class Referrals:
    name = "referrals"
    key_properties = ["id"]
    replication_keys = None
    replication_method = "FULL_TABLE"
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "programId": {"type": "string"},
            "referredUser": {"type": "string"},
            "referredAccount": {"type": "string"},
            "referredReward": {"type": "string"},
            "referredModerationStatus": {"type": "string"},
            "referrerUser": {"type": "string"},
            "referrerAccount": {"type": "string"},
            "referrerReward": {"type": "string"},
            "referrerModerationStatus": {"type": "string"},
            "dateReferralStarted": {
                "anyOf": [
                    {"type": "string", "format": "date-time"},
                    {"type": "null"}
                ]
            },
            "dateConverted": {
                "anyOf": [
                    {"type": "string", "format": "date-time"},
                    {"type": "null"}
                ]
            },
            "dateReferralPaid": {
                "anyOf": [
                    {"type": "string", "format": "date-time"},
                    {"type": "null"}
                ]
            },
            "dateReferralEnded": {
                "anyOf": [
                    {"type": "string", "format": "date-time"},
                    {"type": "null"}
                ]
            },
            "dateModerated": {
                "anyOf": [
                    {"type": "string", "format": "date-time"},
                    {"type": "null"}
                ]
            }
        }
    }


class RewardBalances:
    name = "reward_balances"
    key_properties = ["userId", "accountId"]
    replication_keys = None
    replication_method = "FULL_TABLE"
    schema = {
        "type": "object",
        "properties": {
            "userId": {"type": "string"},
            "accountId": {"type": "string"},
            "type": {"type": "string"},
            "amount": {"type": "integer"},
            "unit": {"type": "string"}
        }
    }


class Users:
    name = "users"
    key_properties = ["id", "accountId"]
    replication_keys = None
    replication_method = "FULL_TABLE"
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "accountId": {"type": "string"},
            "email": {"type": "string"},
            "firstName": {"type": "string"},
            "lastName": {"type": "string"},
            "imageUrl": {"type": "string"},
            "firstSeenIP": {"type": "string"},
            "lastSeenIP": {"type": "string"},
            "dateCreated": {
                "anyOf": [
                    {"type": "string", "format": "date-time"},
                    {"type": "null"}
                ]
            },
            "emailHash": {"type": "string"},
            "referralSource": {"type": "string"},
            "locale": {"type": "string"},
            "shareLink": {"type": "string"},
            "facebookShareLink": {"type": "string"},
            "twitterShareLink": {"type": "string"},
            "emailShareLink": {"type": "string"},
            "linkedinShareLink": {"type": "string"}
        }
    }


# Add all streams to dictionary for discover()
STREAMS = {
    Referrals.name: Referrals,
    RewardBalances.name: RewardBalances,
    Users.name: Users
}
