# gas-framework

An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:

- `/web` - The GAS web app files
- `/ann` - Annotator files
- `/util` - Utility scripts for notifications, archival, and restoration
- `/aws` - AWS user data files

## Job Archive Mechanism Implementation

### Completion and Archive Request:

Upon completion of a job, `run.py` initiates the archival process by sending a `restore request` to the `restore_request_queue`. This queue has a configured delay of 5 minutes to begin processing the request.

### Archive Process Activation:

After the 5-minute delay, `archive.py` starts processing the message received in the queue.

### Premium User Verification:

The initial step involves verifying if the user holds a premium account status. For non-premium users, the mechanism proceeds to archive the result file.

## Job Restore Mechanism Implementation

### Premium Plan Upgrade and File Identification

When a user upgrades to a premium plan, the web server queries `DynamoDB` to identify all files associated with the user that are stored in `Amazon Glacier`.

### Restore Request Initiation

Subsequently, the web server sends a message to the `restore_request_queue` to initiate the restoration of the identified files.

### Restoration Process Start

The `restore.py script`, running on a utility server, picks up the restoration task and commences the process. It then sends a message to the `restore_pendings_queue` to track the pending restoration tasks.

### Restoration Completion and S3 Transfer

The `thaw.py` script periodically checks the `restore_pendings_queue` for any items that have been restored. Once confirmed, it moves these items back to the standard `S3` storage, completing the restoration process.
