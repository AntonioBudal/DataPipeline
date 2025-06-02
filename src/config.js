// config.js
module.exports = {
    ads: {
        clientId: process.env.GOOGLE_ADS_CLIENT_ID,
        clientSecret: process.env.GOOGLE_ADS_CLIENT_SECRET,
        developerToken: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
        refreshToken: process.env.GOOGLE_ADS_REFRESH_TOKEN, // <--- Make sure this line exists and points to the correct env var
        customerId: process.env.GOOGLE_ADS_CUSTOMER_ID,
    },
    hubspot: {
        privateAppToken: process.env.HUBSPOT_PRIVATE_APP_TOKEN,
        // Make sure these are correctly mapped if you use them
        dealStageIdForClosedWon: process.env.HUBSPOT_DEAL_STAGE_CLOSED_WON || 'closedwon',
        dealStageIdForClosedLost: process.env.HUBSPOT_DEAL_STAGE_CLOSED_LOST || 'closedlost',
    },
    sheets: {
        clientId: process.env.GOOGLE_SHEETS_CLIENT_ID,
        clientSecret: process.env.GOOGLE_SHEETS_CLIENT_SECRET,
        refreshToken: process.env.GOOGLE_SHEETS_REFRESH_TOKEN,
        sheetId: process.env.GOOGLE_SHEETS_ID,
    }
};