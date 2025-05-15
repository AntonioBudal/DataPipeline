// src/index.js
import 'dotenv/config';
import { GoogleAdsApi } from 'google-ads-api';
import { Client as HubspotClient } from '@hubspot/api-client';
import { google } from 'googleapis';
import config from './config.js';
import { writeToSheet } from './sheetsWriter.js';

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
const hubspotClient = new HubspotClient({ accessToken: config.hubspot });

// Função para coletar campanhas do Google Ads
// src/index.js (apenas a parte de fetchCampaigns)
async function fetchCampaigns() {
  console.time('ads-fetch');

  // Cria a string GAQL manualmente
  const gaql = `
    SELECT
      campaign.id,
      campaign.name,
      metrics.cost_micros,
      segments.ad_network_type
    FROM campaign
    WHERE
      campaign.status IN ('ENABLED', 'PAUSED')
      AND segments.date DURING LAST_30_DAYS
  `;

  // Usa .query em vez de reportStream
  const rows = await adsCustomer.query(gaql);

  // Mapeia o resultado
  const campaigns = rows.map(row => ({
    name: row.campaign.name,
    network: row.segments.ad_network_type || 'Desconhecida',
    cost: Number(row.metrics.cost_micros) / 1e6
  }));

  console.timeEnd('ads-fetch');
  return campaigns;
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
      deal.properties.dealstage === 'closedwon' ? closed++ : open++;
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
  const campaigns = await fetchCampaigns();
  const results = [];

  for (const camp of campaigns) {
    const counts = await countDeals(camp.name);
    results.push({ ...camp, ...counts });
  }

  await writeToSheet(results);
  console.log('✅ Pipeline concluído com sucesso');
}

// Handler para Vercel Serverless Function
export default async function handler(req, res) {
  try {
    await executarPipeline();
    return res.status(200).send('✅ Pipeline executado com sucesso');
  } catch (error) {
    console.error('❌ Pipeline falhou:', error);
    return res.status(500).send('❌ Pipeline falhou: ' + error.message);
  }
}
