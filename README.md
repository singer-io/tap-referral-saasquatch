# tap-referral-saasquatch

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Referral SaaSquatch's Export API
- Extracts the following resources from Referral SaaSquatch:
  - Referrals
  - Reward Balances
  - Users
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Quick start

1. Install

    ```bash
    > pip install tap-referral-saasquatch
    ```

2. Get your Tenant Alias and API Key

    Login to your Referral SaaSquatch account, navigate Setup -> Install in your left navigation. Note your Tenant Alias and API Key for the next step.

3. Create the config file

    Create a JSON file called `config.json` containing the Tenant Alias and API Key.

    ```json
    {"tenant_alias": "your-tenant-alias",
     "api_key": "your-api-token"}
    ```

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the API endpoints
    to force the application to only fetch data newer than those dates.
    If you omit the file it will fetch all Referral SaaSquatch data.

    ```json
    {"referrals": "2017-01-17T20:32:05Z",
     "reward_balances": "2017-01-17T20:32:05Z",
     "users": "2017-01-17T20:32:05Z"}
    ```

5. Run the application

    `tap-referral-saasquatch` can be run with:

    ```bash
    tap-referral-saasquatch --config config.json [--state state.json]
    ```

---

Copyright &copy; 2017 Stitch
