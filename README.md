# Data Pipeline: Google Ads to Google Sheets with HubSpot Deal Tracking

## Table of Contents
* [Overview](#overview)
* [Technologies Used](#technologies-used)
* [Prerequisites](#prerequisites)
* [Setup and Deployment](#setup-and-deployment)
    * [1. Clone the Repository](#1-clone-the-repository)
    * [2. Install Dependencies](#2-install-dependencies)
    * [3. Create .env File](#3-create-env-file)
    * [4. Configure config.js](#4-configure-configjs)
    * [5. Deploy to Vercel](#5-deploy-to-vercel)
    * [6. Set up a Cron Job (Optional)](#6-set-up-a-cron-job-optional)
* [Notes](#notes)
* [Further Development](#further-development)

## Overview

The pipeline performs the following steps:

1.  **Fetches Google Ads Campaigns:** Retrieves a list of active and paused campaigns from Google Ads, including their names, ad network types, and cost for the last 30 days.
2.  **Counts HubSpot Deals:** Fetches all deals from HubSpot and, for each deal associated with a marketing campaign, retrieves the names of those campaigns and the deal stage. It then counts the number of open and closed-won deals.
3.  **Combines Data:** Matches Google Ads campaigns with associated HubSpot deals (based on campaign names) to count the number of open and closed-won deals attributed to each Google Ads campaign.
4.  **Writes to Google Sheets:** Sends the combined data, including Google Ads campaign performance and the corresponding HubSpot deal counts, to a specified Google Sheets spreadsheet.

## Technologies Used

* **Node.js:** The runtime environment for the serverless functions.
* **`google-ads-api`:** A Node.js library for interacting with the Google Ads API.
* **`@hubspot/api-client`:** The official Node.js client library for interacting with the HubSpot API.
* **`googleapis`:** The official Google API client library for Node.js, used here for interacting with the Google Sheets API.
* **`dotenv`:** A library to load environment variables from a `.env` file.
* **Vercel:** A platform for serverless function deployment and hosting.

## Prerequisites

Before deploying and running this pipeline, ensure you have the following:

1.  **Google Cloud Project with Google Ads API Enabled:** You need a Google Cloud Project with the Google Ads API enabled.
2.  **Google Ads API Credentials:** You will need your `client_id`, `client_secret`, `developer_token`, `customer_id`, and `refresh_token` for the Google Ads API. Ensure your developer token has the necessary access level (Basic or Standard for non-test accounts).
3.  **HubSpot Private App Token:** Create a private app in your HubSpot account with the necessary scopes to read deals (`crm.objects.deals.read`, `crm.objects.deals.search`) and marketing campaigns (`marketing.campaigns.read`).
4.  **Google Sheets API Credentials:** You need credentials to access and write to your Google Sheets spreadsheet. This typically involves setting up a service account in your Google Cloud Project and downloading a JSON key file.
5.  **Google Sheets ID:** The ID of the Google Sheets spreadsheet where the data will be written.
6.  **Node.js and npm (or yarn) installed on your local machine (for development and deployment).
7.  **Vercel CLI installed (for deployment to Vercel).**

## Setup and Deployment

1.  ### 1. Clone the Repository
    Clone the project repository from GitHub:
    ```bash
    git clone [https://github.com/AntonioBudal/DataPipeline.git](https://github.com/AntonioBudal/DataPipeline.git)
    cd DataPipeline
    ```

2.  ### 2. Install Dependencies
    Navigate to the project directory in your terminal and run:
    ```bash
    npm install
    # or
    yarn install
    ```

3.  ### 3. Create .env File
    Create a `.env` file in the root of your project and add your API credentials and configuration:
    ```dotenv
    ADS_CLIENT_ID=YOUR_GOOGLE_ADS_CLIENT_ID
    ADS_CLIENT_SECRET=YOUR_GOOGLE_ADS_CLIENT_SECRET
    ADS_DEVELOPER_TOKEN=YOUR_GOOGLE_ADS_DEVELOPER_TOKEN
    ADS_CUSTOMER_ID=YOUR_GOOGLE_ADS_CUSTOMER_ID
    ADS_REFRESH_TOKEN=YOUR_GOOGLE_ADS_REFRESH_TOKEN

    HUBSPOT_PRIVATE_APP_TOKEN=YOUR_HUBSPOT_PRIVATE_APP_TOKEN

    GOOGLE_SHEETS_CREDENTIALS=path/to/your/google-sheets-credentials.json
    GOOGLE_SHEET_ID=YOUR_GOOGLE_SHEET_ID
    ```
    **Note:** Ensure the path to your Google Sheets credentials JSON file is correct. For Vercel deployment, it's often better to set these as environment variables directly in the Vercel project settings instead of relying on a local file.

4.  ### 4. Configure `config.js`
    Create a `config.js` file (if it doesn't exist) to export your configuration:
    ```javascript
    module.exports = {
      ads: {
        clientId: process.env.ADS_CLIENT_ID,
        clientSecret: process.env.ADS_CLIENT_SECRET,
        developerToken: process.env.ADS_DEVELOPER_TOKEN,
        customerId: process.env.ADS_CUSTOMER_ID,
        refreshToken: process.env.ADS_REFRESH_TOKEN,
      },
      hubspot: {
        privateAppToken: process.env.HUBSPOT_PRIVATE_APP_TOKEN,
      },
      sheets: {
        credentialsPath: process.env.GOOGLE_SHEETS_CREDENTIALS,
        sheetId: process.env.GOOGLE_SHEET_ID,
      },
    };
    ```

5.  ### 5. Deploy to Vercel
    * If you haven't already, initialize a Vercel project in your repository:
        ```bash
        vercel
        ```
    * Follow the prompts to link your project to your Vercel account.
    * Deploy your project:
        ```bash
        vercel --prod
        ```
    * Set the environment variables in your Vercel project settings (under the "Environment Variables" tab) using the values from your `.env` file. This is the recommended way to handle sensitive credentials in a production environment.

6.  ### 6. Set up a Cron Job (Optional)
    To automate the pipeline execution, you can set up a cron job in your Vercel project settings under the "Webhooks/Cron Jobs" tab. Configure the schedule according to your needs.

## Notes

* The matching between Google Ads campaigns and HubSpot deals is currently done based on the **campaign name**. Ensure that your campaign names are consistent across both platforms for accurate attribution.
* Error handling and logging are included in the code to help with debugging. Check the Vercel function logs for any issues.
* The Google Ads API developer token used in the initial setup might be for test accounts only. Apply for Basic or Standard access to access non-test accounts.
* The HubSpot API calls are rate-limited. Be mindful of the limits and implement appropriate error handling and potential backoff mechanisms for high-volume scenarios.
* Consider using more robust matching logic (e.g., using campaign IDs if available in both platforms) for more accurate attribution between Google Ads and HubSpot.

## Further Development

* Implement more sophisticated matching logic between Google Ads campaigns and HubSpot data.
* Add error handling and retry mechanisms for API calls.
* Implement logging to a dedicated service for better monitoring.
* Allow configuration of the date range for Google Ads data.
* Potentially integrate with other data sources or destinations.