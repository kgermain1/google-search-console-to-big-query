# Google Search Console Data to Big Query

2 Scripts to pull performance data from Google Search Console and upload it to a big query table. One for historical data, and one that can pull data on a daily basis if run in Google Cloud Functions/Cloud Scheduler.

Requires to:
1. Have or create a Google Cloud Platform project with valid billing
2. Enable the Google Search Console API
3. Enable the Big Query API.
4. Create a service account for authentification, see here: https://cloud.google.com/compute/docs/access/service-accounts

The script can be run for multiple properties one after the other by specifying it in the parameters.

A service account JSON File needs to be hosted in the same directory when the script runs on a local machine, and similarly in Google Cloud Functions. When running the script in Google Cloud Functions, a requirements.txt file is required to install dependencies.

