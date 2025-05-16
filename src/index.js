require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');

const config = require('./config');
const { writeToSheet } = require('./sheetsWriter');

console.log(`DEBUG: HubSpot Token from config (initial load - first 5 chars): ${config.hubspot && config.hubspot.privateAppToken ? config.hubspot.privateAppToken.substring(0, 5) + '...' : 'NOT FOUND or config.hubspot is undefined'}`);

let adsApi, adsCustomer;
try {
  adsApi = new GoogleAdsApi({
    client_id: config.ads.clientId,
    client_secret: config.ads.clientSecret,
    developer_token: config.ads.developerToken,
  });
  adsCustomer = adsApi.Customer({
    customer_id: config.ads.customerId,
    refresh_token: config.ads.refreshToken,
  });
  console.log('Cliente Google Ads inicializado com sucesso.');
} catch (error) {
  console.error('Erro CRÍTICO ao inicializar o cliente Google Ads:', error.message);
  console.error('DEBUG: Google Ads Initialization Error Stack:', error.stack);
}

let hubspotClient;
try {
  const tokenForHubspot = config.hubspot && config.hubspot.privateAppToken;
  console.log(`DEBUG: Attempting to initialize HubSpot client. Token available (first 5 chars): ${tokenForHubspot ? tokenForHubspot.substring(0, 5) + '...' : 'NO TOKEN'}`);
  if (!tokenForHubspot) {
    console.error('CRITICAL: HubSpot Private App Token is MISSING in config before client initialization!');
    throw new Error('HubSpot Private App Token is missing.');
  }
  hubspotClient = new Hubspot.Client({ accessToken: tokenForHubspot });
  console.log('Cliente HubSpot inicializado (tentativa).');

  if (hubspotClient && hubspotClient.crm && hubspotClient.crm.deals && hubspotClient.crm.deals.searchApi && typeof hubspotClient.crm.deals.searchApi.doSearch === 'function') {
    console.log('DEBUG: hubspotClient.crm.deals.searchApi.doSearch IS a function and available.');
  } else {
    console.warn('DEBUG: hubspotClient.crm.deals.searchApi.doSearch IS NOT available or not a function.');
  }
  if (hubspotClient && hubspotClient.marketing && hubspotClient.marketing.campaigns && typeof hubspotClient.marketing.campaigns.getById === 'function') {
    console.log('DEBUG: hubspotClient.marketing.campaigns.getById IS a function and available (checked at init).');
  } else {
    console.warn('DEBUG: hubspotClient.marketing.campaigns.getById IS NOT available or not a function (checked at init).');
  }
} catch (error) {
  console.error('Erro CRÍTICO ao inicializar o cliente HubSpot:', error.message);
  console.error('DEBUG: HubSpot Initialization Error Stack:', error.stack);
  hubspotClient = undefined;
}

async function fetchCampaigns() {
  if (!adsCustomer) {
    console.warn('Cliente Google Ads não inicializado. Pulando busca de campanhas.');
    return [];
  }
  console.time('ads-fetch');
  try {
    const stream = adsCustomer.reportStream({
      entity: 'campaign',
      attributes: ['campaign.name', 'segments.ad_network_type'],
      metrics: ['metrics.cost_micros'],
      constraints: { 'campaign.status': ['ENABLED', 'PAUSED'], 'segments.date': 'DURING LAST_30_DAYS' },
    });
    const campaigns = [];
    for await (const row of stream) {
      const cost = Number(row.metrics.cost_micros) / 1e6;
      const network = row.segments.ad_network_type || 'Desconhecida';
      campaigns.push({ name: row.campaign.name, network, cost });
    }
    return campaigns;
  } catch (error) {
    console.error('Erro ao buscar campanhas do Google Ads:', error);
    if (error.errors) console.error('DEBUG: Google Ads API Error Details:', JSON.stringify(error.errors, null, 2));
    if (error.stack) console.error('DEBUG: Google Ads API Error Stack:', error.stack);
    return [];
  } finally {
    console.timeEnd('ads-fetch');
  }
}

async function countDeals() {
  console.log('DEBUG: Entered countDeals function.');
  if (!hubspotClient) {
    console.warn('Cliente HubSpot não inicializado. Pulando busca de negócios.');
    return {
      totalOpenHubSpotDeals: 0,
      totalClosedWonHubSpotDeals: 0,
      totalLostHubSpotDeals: 0,
      dealCampaigns: {}
    };
  }
  console.log('DEBUG: HubSpot client seems initialized, proceeding to prepare request for deals.');

  let totalOpenHubSpotDeals = 0;
  let totalClosedWonHubSpotDeals = 0;
  let totalLostHubSpotDeals = 0;
  const dealCampaignsData = {};

  // função utilitária para delay
  const delay = ms => new Promise(res => setTimeout(res, ms));

  let after; // cursor para paginação
  do {
    const request = {
      filterGroups: [],
      properties: ['dealstage'],
      associations: ['marketing_campaign'],
      limit: 100,
      after
    };
    console.log('DEBUG: HubSpot search request object for deals:', JSON.stringify(request, null, 2));

    // lógica de retry em caso de 429
    let response;
    const maxAttempts = 5;
    let attempt = 0;
    const baseDelay = 500; // ms

    while (attempt < maxAttempts) {
      try {
        console.log(`DEBUG: Attempt ${attempt + 1} for doSearch`);
        response = await hubspotClient.crm.deals.searchApi.doSearch(request);
        break; // sucesso
      } catch (err) {
        const status = err.response?.status || err.code;
        console.error(`Erro na API HubSpot (tentativa ${attempt + 1}):`, status, err.message);
        // se for rate limit, espera e tenta de novo
        if (status === 429 && attempt < maxAttempts - 1) {
          const wait = baseDelay * Math.pow(2, attempt);
          console.log(`DEBUG: Rate limit hit, aguardando ${wait}ms antes do retry`);
          await delay(wait);
          attempt++;
          continue;
        }
        // falha definitiva
        console.error('DEBUG: Falha ao buscar negócios, abortando countDeals');
        return {
          totalOpenHubSpotDeals: 0,
          totalClosedWonHubSpotDeals: 0,
          totalLostHubSpotDeals: 0,
          dealCampaigns: {}
        };
      }
    }

    console.log('DEBUG: HubSpot API call for deals search successful. Number of results:', response.results?.length);
    console.log('DEBUG: Paging next.after:', response.paging?.next?.after);

    // processa cada deal retornado
    for (const deal of response.results || []) {
      const dealId = deal.id;
      const currentDealStage = deal.properties.dealstage;
      const associatedCampaignNames = [];

      if (deal.associations?.marketing_campaign?.results?.length) {
        console.log(`DEBUG: Deal ID ${dealId} has ${deal.associations.marketing_campaign.results.length} associated marketing campaigns. Fetching names...`);
        for (const assoc of deal.associations.marketing_campaign.results) {
          try {
            console.log(`DEBUG: Fetching campaign details for ID ${assoc.id}`);
            const camp = await hubspotClient.marketing.campaigns.getById(assoc.id);
            associatedCampaignNames.push(camp.name);
            console.log(`DEBUG: Fetched campaign name: ${camp.name}`);
          } catch (campErr) {
            console.error(`Erro ao buscar detalhes da campanha ID ${assoc.id}:`, campErr.message);
            associatedCampaignNames.push('Nome da Campanha Desconhecido');
          }
        }
      }

      dealCampaignsData[dealId] = {
        campaignNames: associatedCampaignNames,
        dealstage: currentDealStage
      };

      // contagem por estágio currentDealStage === 'closedwon' ||
      if ( currentDealStage === '148309307') {
        totalClosedWonHubSpotDeals++;
      } else if (currentDealStage === 'closedlost') {
        totalLostHubSpotDeals++;
      } else {
        totalOpenHubSpotDeals++;
      }
    }

    // atualiza cursor e repete
    after = response.paging?.next?.after;
  } while (after);

  console.log('DEBUG: Recalculated HubSpot Totals by countDeals:', {
    totalOpenHubSpotDeals,
    totalClosedWonHubSpotDeals,
    totalLostHubSpotDeals
  });

  return {
    totalOpenHubSpotDeals,
    totalClosedWonHubSpotDeals,
    totalLostHubSpotDeals,
    dealCampaigns: dealCampaignsData
  };
}

async function executarPipeline() {
  console.log('🚀 Pipeline iniciado');
  try {
    const googleAdsCampaigns = await fetchCampaigns();
    console.log('Campanhas do Google Ads (resultado da busca):', googleAdsCampaigns);

    const { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals, dealCampaigns } = await countDeals();
    console.log('📊 Contagem de negócios do HubSpot:', { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals });
    console.log('🤝 Negócios do HubSpot e suas campanhas:', dealCampaigns);

    const resultsForAdsSheet = [];

    if (googleAdsCampaigns && googleAdsCampaigns.length > 0) {
      console.log(`ℹProcessando ${googleAdsCampaigns.length} campanhas do Google Ads...`);
      for (const adCampaign of googleAdsCampaigns) {
        let openCountForAdCampaign = 0;
        let closedWonCountForAdCampaign = 0;

        for (const dealId in dealCampaigns) {
          if (dealCampaigns[dealId].campaignNames.includes(adCampaign.name)) {
            const stage = dealCampaigns[dealId].dealstage;
            if (stage === 'closedwon' || stage === '148309307') {
              closedWonCountForAdCampaign++;
            } else {
              openCountForAdCampaign++;
            }
          }
        }
        resultsForAdsSheet.push({
          'Nome da Campanha': adCampaign.name,
          'Rede': adCampaign.network,
          'Custo Total no Período': adCampaign.cost.toFixed(2),
          'Negócios Abertos': openCountForAdCampaign,
          'Negócios Fechados': closedWonCountForAdCampaign,
        });
      }

      const adsHeaders = ['Nome da Campanha', 'Rede', 'Custo Total no Período', 'Negócios Abertos', 'Negócios Fechados'];
      await writeToSheet(resultsForAdsSheet, 'Campanhas Ads', adsHeaders);
      console.log('✅ Dados de campanhas do Google Ads enviados para a aba "Campanhas Ads".');
    } else if (totalOpenHubSpotDeals > 0 || totalClosedWonHubSpotDeals > 0) {
      console.log('Nenhuma campanha do Google Ads. Criando linha de resumo para HubSpot na aba "Campanhas Ads".');
      await writeToSheet([{
        'Nome da Campanha': 'HubSpot - Resumo Geral de Negócios',
        'Rede': 'N/A (HubSpot)',
        'Custo Total no Período': 0,
        'Negócios Abertos': totalOpenHubSpotDeals,
        'Negócios Fechados': totalClosedWonHubSpotDeals,
      }], 'Campanhas Ads', ['Nome da Campanha', 'Rede', 'Custo Total no Período', 'Negócios Abertos', 'Negócios Fechados']);
    }

    // Lista de formulários que você quer analisar
    const formsToAnalyze = [
      'Contato Inicial',
      'Pedido de Orçamento',
      // Adicione outros nomes de formulários aqui
    ];

    const formDataForSheet = [];

    for (const formName of formsToAnalyze) {
      const matchingAdCampaign = googleAdsCampaigns.find(campaign => campaign.name === formName);
      let openDealsForForm = 0;
      let closedDealsForForm = 0;

      if (matchingAdCampaign) {
        for (const dealId in dealCampaigns) {
          if (dealCampaigns[dealId].campaignNames.includes(formName)) {
            const stage = dealCampaigns[dealId].dealstage;
            if (stage === 'closedwon' || stage === '148309307') {
              closedDealsForForm++;
            } else {
              openDealsForForm++;
            }
          }
        }
        formDataForSheet.push({
          'Nome do Formulário': formName,
          'Visualizações': 'N/A', // Você precisará obter esses dados de algum lugar
          'Envios': 'N/A',     // Você precisará obter esses dados de algum lugar
          'Negócios Abertos': openDealsForForm,
          'Negócios Fechados': closedDealsForForm,
        });
      } else {
        formDataForSheet.push({
          'Nome do Formulário': formName,
          'Visualizações': 'N/A',
          'Envios': 'N/A',
          'Negócios Abertos': totalOpenHubSpotDeals,
          'Negócios Fechados': totalClosedWonHubSpotDeals,
        });
      }
    }

    const formHeaders = ['Nome do Formulário', 'Visualizações', 'Envios', 'Negócios Abertos', 'Negócios Fechados'];
    await writeToSheet(formDataForSheet, 'Dados de Formulários', formHeaders);
    console.log('✅ Dados de formulários enviados para a aba "Dados de Formulários".');

    console.log('📝 Dados combinados e enviados para as planilhas.');

  } catch (error) {
    console.error('❌ Erro no pipeline principal:', error);
    throw error;
  }
}

export default async function handler(req, res) {
  console.log(`ℹ️ Handler da Vercel invocado. Método: ${req.method}`);
  try {
    await executarPipeline();
    return res.status(200).send('Pipeline executado com sucesso');
  } catch (error) {
    console.error('Pipeline falhou no handler da Vercel (erro pego no handler):', error.message);
    if (error.stack && !error.message.includes(error.stack.split('\n')[0])) {
      console.error('DEBUG: Pipeline falhou no handler da Vercel (Stack):', error.stack);
    }
    return res.status(500).send(`Pipeline falhou: ${error.message}`);
  }
}