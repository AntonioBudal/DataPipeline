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

    // Desestruturação direta, os nomes das variáveis agora são os mesmos que as chaves do objeto retornado
    const { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals, dealCampaigns } = await countDeals();
    
    console.log('Contagem TOTAL de negócios do HubSpot (de countDeals):', { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals });

    const resultsForSheet = [];

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
        resultsForSheet.push({
          name: adCampaign.name,
          network: adCampaign.network,
          cost: adCampaign.cost,
          open: openCountForAdCampaign,
          closed: closedWonCountForAdCampaign,
        });
      }
    } else if (totalOpenHubSpotDeals > 0 || totalClosedWonHubSpotDeals > 0) {
      // Usa as variáveis desestruturadas diretamente
      console.log('Nenhuma campanha do Google Ads. Criando linha de resumo para HubSpot.');
      resultsForSheet.push({
        name: 'HubSpot - Resumo Geral de Negócios',
        network: 'N/A (HubSpot)',
        cost: 0,
        open: totalOpenHubSpotDeals,       // Uso direto da variável desestruturada
        closed: totalClosedWonHubSpotDeals, // Uso direto da variável desestruturada
      });
    }

    console.log('Dados combinados antes de escrever na planilha:', resultsForSheet);

    if (resultsForSheet.length > 0) {
      await writeToSheet(resultsForSheet);
    } else {
      console.log('Nenhum dado (nem Ads, nem HubSpot agregado) para escrever na planilha.');
    }

  } catch (error) {
    console.error('Erro no pipeline principal:', error.message);
    if (error.stack) console.error('DEBUG: Erro no pipeline principal (Stack):', error.stack);
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