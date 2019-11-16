# tap-ujet

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [UJET API](https://support.ujet.co/hc/en-us/articles/115006908127-UJET-Data-API#h_7d95eafc-6c02-446b-bcc6-b733f4e1143e)
- Extracts the following resources:
  - Agents
  - Agent Activity Logs
  - Calls
  - Chats
  - Menus
  - Menu Tree
  - Teams
  - Team Tree
  - User Statuses
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams

**agents**
- Endpoint: https://{subdomain}.ujet.co/manager/api/v1/agents
- Primary key fields: id
- Foreign key fields: teams > id
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: status_updated_at
  - Bookmark query field: status_updated_at[from]
- Transformations: none

**agent_activity_logs**
- Endpoint: https://{subdomain}.ujet.co/manager/api/v1/agent_activity_logs
- Primary key fields: id
- Foreign key fields: teams > id
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark: status_updated_at
  - Bookmark query field: status_updated_at[from]
- Transformations: none

**ADD OTHER ENDPOINTS**


## Authentication


## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-ujet
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. The `server_subdomain` is everything before `.ujet.com.` in the ujet URL.  The `account_name` is everything between `.ujet.com.` and `api` in the ujet URL. The `date_window_size` is the integer number of days (between the from and to dates) for date-windowing through the date-filtered endpoints (default = 60).

    ```json
    {
        "company_key": "YOUR_COMPANY_KEY",
        "company_secret": "YOUR_COMPANY_SECRET_CODE",
        "subdomain": "YOUR_COMPANY",
        "domain": "ujet",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-ujet <api_user_email@your_company.com>",
        "date_window_size": "14"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "calls",
        "bookmarks": {
            "agnets": "2019-09-27T22:34:39.000000Z",
            "agent_activity_logs": "2019-09-28T15:30:26.000000Z",
            "calls": "2019-09-28T18:23:53Z",
            "chats": "2019-09-27T22:40:30.000000Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-ujet --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_code/bytecode/StitchOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-ujet --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-ujet --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-ujet --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While code/bytecode/Stitchoping the ujet tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_ujet -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    TBD LATER
    Your code has been rated at TBD/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-ujet --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    TBD LATER

    The output is valid.
    It contained 127 messages for 10 streams.

        10 schema messages
        92 record messages
        25 state messages

    Details by stream:
    +---------------------+---------+---------+
    | stream              | records | schemas |
    +---------------------+---------+---------+
    | TBD                 | 99      | 1       |
    +---------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
