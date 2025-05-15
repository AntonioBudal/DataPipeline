// config.js
module.exports = {
  ads: {
    clientId: process.env.GOOGLE_ADS_CLIENT_ID,
    clientSecret: process.env.GOOGLE_ADS_CLIENT_SECRET,
    developerToken: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    customerId: process.env.GOOGLE_ADS_CUSTOMER_ID, // Adicione esta variável de ambiente
    refreshToken: process.env.GOOGLE_ADS_REFRESH_TOKEN,
  },
  hubspot: {
    privateAppToken: process.env.HUBSPOT_PRIVATE_APP_TOKEN,
  },
  sheets: {
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
    clientId: process.env.GOOGLE_SHEETS_CLIENT_ID,
    clientSecret: process.env.GOOGLE_SHEETS_CLIENT_SECRET,
    refreshToken: process.env.GOOGLE_SHEETS_REFRESH_TOKEN,
  },
};