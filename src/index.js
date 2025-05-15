require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');

const config = require('./config');
const { writeToSheet } = require('./sheetsWriter');

// DEBUG: Log para verificar se o token está sendo carregado do config
console.log(`DEBUG: HubSpot Token from config (initial load - first 5 chars): ${config.hubspot.privateAppToken ? config.hubspot.privateAppToken.substring(0, 5) : 'NOT FOUND'}`);

// ... (inicialização do Google Ads API) ...

// Inicializa cliente do HubSpot
let hubspotClient;
try {
  const tokenForHubspot = config.hubspot.privateAppToken;
  console.log(`DEBUG: Attempting to initialize HubSpot client. Token available (first 5 chars): ${tokenForHubspot ? tokenForHubspot.substring(0, 5) : 'NO TOKEN'}`);
  if (!tokenForHubspot) {
    console.error('❌ CRITICAL: HubSpot Private App Token is MISSING in config before client initialization!');
    // Considerar lançar um erro aqui ou garantir que hubspotClient permaneça undefined
  }
  hubspotClient = new Hubspot.Client({
    accessToken: tokenForHubspot,
  });
  console.log('✅ Cliente HubSpot inicializado com sucesso (ou assim parece).');

  // DEBUG: Verificar se os métodos esperados existem
  if (hubspotClient && hubspotClient.crm && hubspotClient.crm.deals && hubspotClient.crm.deals.searchApi && typeof hubspotClient.crm.deals.searchApi.doSearch === 'function') {
    console.log('DEBUG: hubspotClient.crm.deals.searchApi.doSearch IS a function and available.');
  } else {
    console.warn('DEBUG: hubspotClient.crm.deals.searchApi.doSearch IS NOT available or not a function. Structure might be wrong.');
    // Logar mais detalhes da estrutura se necessário
    // console.log('DEBUG: hubspotClient.crm keys:', hubspotClient && hubspotClient.crm ? Object.keys(hubspotClient.crm) : 'hubspotClient.crm is undefined');
    // console.log('DEBUG: hubspotClient.crm.deals keys:', hubspotClient && hubspotClient.crm && hubspotClient.crm.deals ? Object.keys(hubspotClient.crm.deals) : 'hubspotClient.crm.deals is undefined');
  }

} catch (error) {
  console.error('❌ Erro CRÍTICO ao inicializar o cliente HubSpot:', error.message);
  console.error('DEBUG: HubSpot Initialization Error Stack:', error.stack);
  hubspotClient = undefined; // Garante que o cliente não seja usado se a inicialização falhar
}

// Função para contar negócios no HubSpot e obter nomes de campanhas associadas
async function countDeals() {
  console.log('DEBUG: Entered countDeals function.');
  if (!hubspotClient) {
    console.warn('⚠️ Cliente HubSpot não inicializado ou falhou na inicialização. Pulando busca de negócios.');
    return { openDeals: 0, closedWonDeals: 0, dealCampaigns: {} };
  }
  console.log('DEBUG: HubSpot client seems initialized, proceeding to prepare request.');

  const request = {
    filterGroups: [
      // Adicione aqui seus filtros de negócio, se necessário
    ],
    properties: ['dealstage'],
    associations: ['marketing_campaign'],
    limit: 100,
  };
  console.log('DEBUG: HubSpot search request object:', JSON.stringify(request, null, 2));

  try {
    console.log('DEBUG: Attempting API call: hubspotClient.crm.deals.searchApi.doSearch(request)...');
    // Usando a correção sugerida anteriormente para a chamada da API
    const response = await hubspotClient.crm.deals.searchApi.doSearch(request);
    console.log('DEBUG: HubSpot API call for deals search supposedly successful. Response received.');
    // ... (resto da sua lógica para processar a resposta) ...

    let openDeals = 0;
    let closedWonDeals = 0;
    const dealCampaigns = {};

    for (const deal of response.results || []) {
      // ... (sua lógica de processamento de deals) ...
    }
    return { openDeals, closedWonDeals, dealCampaigns };

  } catch (err) {
    console.error('❌ Erro HubSpot ao buscar negócios com associações de campanha (dentro do try/catch da API call):', err.message);
    // Logar mais detalhes do erro da API do HubSpot
    if (err.response && err.response.body) {
        console.error('DEBUG: HubSpot API Error Response Body:', JSON.stringify(err.response.body, null, 2));
    } else if (err.code) {
        console.error('DEBUG: HubSpot Error Code:', err.code);
    }
    if (err.stack) {
        console.error('DEBUG: HubSpot Error Stack:', err.stack);
    }
    return { openDeals: 0, closedWonDeals: 0, dealCampaigns: {} };
  }
}


// Função principal que executa todo o pipeline
async function executarPipeline() {
  console.log('🚀 Pipeline iniciado');
  try {
    const campaigns = await fetchCampaigns();
    console.log('🔍 Campanhas do Google Ads (resultado da busca):', campaigns); // DEBUG

    const { openDeals, closedWonDeals, dealCampaigns } = await countDeals();
    console.log('📊 Contagem de negócios do HubSpot:', { openDeals, closedWonDeals }); // DEBUG
    console.log('🤝 Negócios do HubSpot e suas campanhas:', dealCampaigns); // DEBUG

    const results = [];

    for (const camp of campaigns) {
      let openCount = 0;
      let closedCount = 0;

      for (const dealId in dealCampaigns) {
        if (dealCampaigns[dealId].campaignNames.includes(camp.name)) {
          if (dealCampaigns[dealId].dealstage === '148309307') {
            closedCount++;
          } else {
            openCount++;
          }
        }
      }

      results.push({
        ...camp,
        open: openCount,
        closed: closedCount,
      });
    }

    console.log('📝 Dados combinados antes de escrever na planilha:', results); // DEBUG

    // ADICIONANDO LINHA DE TESTE PARA O GOOGLE SHEETS
    const testData = [{ name: 'TESTE', network: 'Manual', cost: 0, open: 0, closed: 0 }];
    await writeToSheet(testData);
    console.log('✅ Dados de teste enviados para o Google Sheets');

    await writeToSheet(results);
    console.log('✅ Planilha do Google Sheets atualizada com sucesso');
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