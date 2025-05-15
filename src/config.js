require('dotenv').config()

const { checkEnv } = require('./checkEnv');
checkEnv(); // valida variáveis

module.exports = {
  ads: {
    clientId: process.env.GOOGLE_ADS_CLIENT_ID,
    clientSecret: process.env.GOOGLE_ADS_CLIENT_SECRET,
    developerToken: process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    refreshToken: process.env.GOOGLE_ADS_REFRESH_TOKEN,
    customerId: process.env.GOOGLE_ADS_CUSTOMER_ID,
  },
  hubspot: process.env.HUBSPOT_PRIVATE_APP_TOKEN,
  sheets: {
    clientId: process.env.GOOGLE_SHEETS_CLIENT_ID,
    clientSecret: process.env.GOOGLE_SHEETS_CLIENT_SECRET,
    refreshToken: process.env.GOOGLE_SHEETS_REFRESH_TOKEN,
    spreadsheetId: process.env.GOOGLE_SHEETS_ID,
  },
};
