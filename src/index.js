require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');
const config = require('./config');
const { writeToSheet } = require('./sheetsWriter');

// ✅ Função simulada para buscar campanhas (substitua depois com dados reais do Google Ads)
async function fetchCampaigns() {
  return [
    { name: 'Campanha A', network: 'SEARCH', cost: 1200 },
    { name: 'Campanha B', network: 'DISPLAY', cost: 800 },
  ];
}

// ✅ Função simulada para contar negócios no HubSpot (substitua depois com lógica real)
async function countDeals(campaignName) {
  return {
    open: Math.floor(Math.random() * 10),
    closed: Math.floor(Math.random() * 5),
  };
}

// Inicializa cliente do Google Ads
const adsApi = new GoogleAdsApi({
  client_id: config.ads.clientId,
  client_secret: config.ads.clientSecret,
  developer_token: config.ads.developerToken,
});

const adsCustomer = adsApi.Customer({
  customer_id: config.ads.customerId,
  refresh_token: config.ads.refreshToken,
});

// Inicializa cliente do HubSpot
const hubspotClient = new Hubspot.Client({
  accessToken: config.hubspot,
});

// 👉 Função principal do pipeline
async function executarPipeline() {
  console.log('🚀 Pipeline iniciado');
  const campaigns = await fetchCampaigns();
  const results = [];

  for (const camp of campaigns) {
    const counts = await countDeals(camp.name);
    results.push({ ...camp, ...counts });
  }

  await writeToSheet(results);
  console.log('✅ Pipeline concluído com sucesso');
}

// 👉 Exporta função para execução via Vercel Serverless Function
module.exports = async (req, res) => {
  try {
    await executarPipeline();                // executa tudo internamente
    return res.status(200).send('OK');       // retorna apenas "OK"
  } catch (error) {
    console.error('Pipeline falhou:', error);
    return res.status(500).send('ERROR');
  }
};

