// src/index.js

require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');

// Carrega configurações do arquivo separado
const config = require('./config');
const { writeToSheet } = require('./sheetsWriter');

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
  accessToken: config.hubspot.privateAppToken, // Usando a chave correta do config
});

// Função para coletar campanhas do Google Ads
async function fetchCampaigns() {
  console.time('ads-fetch');
  try {
    const stream = adsCustomer.reportStream({
      entity: 'campaign',
      attributes: ['campaign.name', 'segments.ad_network_type'],
      metrics: ['metrics.cost_micros'],
      constraints: {
        'campaign.status': ['ENABLED', 'PAUSED'],
        'segments.date': 'DURING LAST_30_DAYS',
      },
    });

    const campaigns = [];
    for await (const row of stream) {
      const cost = Number(row.metrics.cost_micros) / 1e6;
      const network = row.segments.ad_network_type || 'Desconhecida';
      campaigns.push({ name: row.campaign.name, network, cost });
    }
    console.timeEnd('ads-fetch');
    return campaigns;
  } catch (error) {
    console.error('❌ Erro ao buscar campanhas do Google Ads:', error);
    return []; // Retorna um array vazio em caso de erro para não quebrar o pipeline
  }
}

// Função para contar negócios no HubSpot
async function countDeals(campaignName) {
  const request = {
    filterGroups: [
      {
        filters: [
          { propertyName: 'campaign_name', operator: 'EQ', value: campaignName }
        ]
      }
    ],
    properties: ['dealstage'],
  };

  try {
    const response = await hubspotClient.crm.objects.deals.search(request);
    let open = 0, closed = 0;
    (response.results || []).forEach(deal => {
      if (deal.properties.dealstage === 'closedwon') closed++;
      else open++;
    });
    return { open, closed };
  } catch (err) {
    console.error(`❌ Erro HubSpot (${campaignName}):`, err.message);
    return { open: 0, closed: 0 };
  }
}

// Função principal que executa todo o pipeline
async function executarPipeline() {
  console.log('🚀 Pipeline iniciado');
  try {
    const campaigns = await fetchCampaigns();
    const results = [];

    for (const camp of campaigns) {
      const counts = await countDeals(camp.name);
      results.push({ ...camp, ...counts });
    }

    await writeToSheet(results);
    console.log('✅ Pipeline concluído com sucesso');
  } catch (error) {
    console.error('❌ Erro no pipeline principal:', error);
    throw error; // Rejoga o erro para ser capturado pelo handler da Vercel
  }
}

// Handler para Vercel Serverless Function
export default async function handler(req, res) {
  try {
    await executarPipeline();
    return res.status(200).send('✅ Pipeline executado com sucesso');
  } catch (error) {
    console.error('❌ Pipeline falhou no handler:', error);
    return res.status(500).send('❌ Pipeline falhou: ' + error.message);
  }
}