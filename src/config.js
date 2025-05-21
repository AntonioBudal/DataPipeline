// config.js
module.exports = {
  ads: {
    clientId: process.env.GOOGLE_ADS_CLIENT_ID,
    clientSecret: process.env.GOOGLE_ADS_CLIENT_SECRET,
    developerToken: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    customerId: process.env.GOOGLE_ADS_CUSTOMER_ID,
    refreshToken: process.env.GOOGLE_ADS_REFRESH_TOKEN,
  },
  hubspot: {
    privateAppToken: process.env.HUBSPOT_PRIVATE_APP_TOKEN,
    // --- IMPORTANT: Update these with your actual HubSpot Deal Stage IDs ---
    // You can find these in HubSpot by going to Settings > Objects > Deals > Pipelines,
    // then clicking on a pipeline and inspecting the stage properties (often found in the URL or API docs).
    // Common values are 'closedwon' and 'closedlost', but some accounts might have custom IDs.
    dealStageIdForClosedWon: 'closedwon', // Example: 'closedwon' or '148309307' (if custom)
    dealStageIdForClosedLost: 'closedlost', // Example: 'closedlost'
  },
  sheets: {
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    clientId: process.env.GOOGLE_SHEETS_CLIENT_ID,
    clientSecret: process.env.GOOGLE_SHEETS_CLIENT_SECRET,
    refreshToken: process.env.GOOGLE_SHEETS_REFRESH_TOKEN,
  },
};