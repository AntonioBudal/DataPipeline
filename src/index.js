// src/index.js
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');
const config = require('./config');

// Google Ads
const adsApi = new GoogleAdsApi({
  client_id: config.ads.clientId,
  client_secret: config.ads.clientSecret,
  developer_token: config.ads.developerToken,
});
const adsCustomer = adsApi.Customer({
  customer_id: config.ads.customerId,
  refresh_token: config.ads.refreshToken,
});

// HubSpot
const hubspotClient = new Hubspot.Client({ apiKey: config.hubspot });

// Google Sheets
const sheetsAuth = new google.auth.OAuth2(
  config.sheets.clientId,
  config.sheets.clientSecret,
  'https://developers.google.com/oauthplayground'
);
sheetsAuth.setCredentials({ refresh_token: config.sheets.refreshToken });
const sheetsApi = google.sheets({ version: 'v4', auth: sheetsAuth });

// src/index.js (final)
const { writeToSheet } = require('./sheetsWriter');

(async () => {
  try {
    console.log('🚀 Pipeline iniciado');
    const campaigns = await fetchCampaigns();
    const results = [];
    for (const camp of campaigns) {
      const counts = await countDeals(camp.name);
      results.push({ ...camp, ...counts });
    }
    await writeToSheet(results);
    console.log('✅ Pipeline concluído com sucesso');
  } catch (error) {
    console.error('❌ Pipeline falhou:', error.message);
    process.exit(1);
  }
})();




