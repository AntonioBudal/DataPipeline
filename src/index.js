require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
// const { google } = require('googleapis'); // Não é usado diretamente em index.js

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

  if (!(hubspotClient && hubspotClient.crm && hubspotClient.crm.deals && hubspotClient.crm.deals.searchApi && typeof hubspotClient.crm.deals.searchApi.doSearch === 'function')) {
    console.warn('DEBUG: hubspotClient.crm.deals.searchApi.doSearch IS NOT available or not a function.');
  }
  if (!(hubspotClient && hubspotClient.marketing && hubspotClient.marketing.campaigns && typeof hubspotClient.marketing.campaigns.getById === 'function')) {
    console.warn('DEBUG: hubspotClient.marketing.campaigns.getById IS NOT available or not a function (checked at init).');
  }
  if (!(hubspotClient && hubspotClient.crm && hubspotClient.crm.associations && hubspotClient.crm.associations.v4 && hubspotClient.crm.associations.v4.basicApi && typeof hubspotClient.crm.associations.v4.basicApi.getPage === 'function')) {
    console.warn('DEBUG: hubspotClient.crm.associations.v4.basicApi.getPage IS NOT available or not a function.');
  }
  if (!(hubspotClient && hubspotClient.crm && hubspotClient.crm.engagements && hubspotClient.crm.engagements.batchApi && typeof hubspotClient.crm.engagements.batchApi.read === 'function')) {
    console.warn('DEBUG: hubspotClient.crm.engagements.batchApi.read IS NOT available or not a function.');
  }

} catch (error) {
  console.error('Erro CRÍTICO ao inicializar o cliente HubSpot:', error.message);
  console.error('DEBUG: HubSpot Initialization Error Stack:', error.stack);
  hubspotClient = undefined;
}

const delay = ms => new Promise(res => setTimeout(res, ms));

async function getContactFormSubmission(contactId) {
  // Checa inicialização do cliente
  if (!hubspotClient) {
    console.warn('WARN: Cliente HubSpot não inicializado. Impossível buscar envio de formulário.');
    return null;
  }

  // Verifica parâmetro
  if (!contactId) {
    console.log('DEBUG: contactId não fornecido para getContactFormSubmission.');
    return null;
  }
  console.log(`DEBUG: Iniciando busca de envio de formulário para contactId: ${contactId}`);

  try {
    // 1. Buscar associações de engajamento
    console.log('DEBUG: Chamando associations.v4.basicApi.getPage...');
    const associatedEngagementsResponse = await hubspotClient.crm.associations.v4.basicApi.getPage(
      'contacts', contactId, 'engagements'
    );
    console.log('DEBUG: Resposta de associações:', JSON.stringify(associatedEngagementsResponse, null, 2));

    const engagementIds = (associatedEngagementsResponse.results || []).map(assoc => assoc.toObjectId);
    console.log(`DEBUG: engagementIds extraídos (count=${engagementIds.length}):`, engagementIds);
    if (!engagementIds.length) {
      console.log(`DEBUG: Nenhum engajamento encontrado para contactId: ${contactId}`);
      return null;
    }

    // 2. Ler detalhes dos engajamentos
    const propertiesToFetch = ['hs_engagement_type', 'hs_createdate', 'metadata'];
    const batchReadRequest = {
      inputs: engagementIds.map(id => ({ id })),
      properties: propertiesToFetch,
    };
    console.log('DEBUG: batchReadRequest:', JSON.stringify(batchReadRequest, null, 2));

    const engagementDetails = await hubspotClient.crm.engagements.batchApi.read(batchReadRequest, false);
    console.log('DEBUG: Resposta batchApi.read:', JSON.stringify(engagementDetails, null, 2));

    // 3. Filtrar FORM_SUBMISSION
    let latestFormSubmission = null;
    for (const engagement of engagementDetails.results || []) {
      const type = engagement.properties.hs_engagement_type;
      const created = engagement.properties.hs_createdate;
      console.log(`DEBUG: Engagement ID=${engagement.id}, type=${type}, createdate=${created}`);
      if (type === 'FORM_SUBMISSION') {
        if (!latestFormSubmission || (created && new Date(created) > new Date(latestFormSubmission.properties.hs_createdate))) {
          console.log(`DEBUG: Atualizando latestFormSubmission para ID=${engagement.id}`);
          latestFormSubmission = engagement;
        }
      }
    }
    if (!latestFormSubmission) {
      console.log(`DEBUG: Nenhum engajamento do tipo FORM_SUBMISSION encontrado para contactId: ${contactId}.`);
      return null;
    }

    // 4. Processar metadata
    const rawMetadata = latestFormSubmission.properties.metadata;
    console.log('DEBUG: rawMetadata do latestFormSubmission:', rawMetadata);
    let parsedMetadata = null;
    if (rawMetadata) {
      if (typeof rawMetadata === 'string') {
        try {
          parsedMetadata = JSON.parse(rawMetadata);
          console.log('DEBUG: metadata parseado com sucesso:', parsedMetadata);
        } catch (e) {
          console.error(`ERROR: Falha ao parsear metadata (string JSON) para engajamento ${latestFormSubmission.id} do contato ${contactId}:`, e);
        }
      } else if (typeof rawMetadata === 'object') {
        parsedMetadata = rawMetadata;
        console.log('DEBUG: metadata já é objeto:', parsedMetadata);
      } else {
        console.warn('WARN: metadata em formato inesperado:', typeof rawMetadata);
      }
    } else {
      console.log('DEBUG: metadata não presente no latestFormSubmission.');
    }

    // 5. Retornar resultado
    if (parsedMetadata && parsedMetadata.formId && parsedMetadata.title) {
      console.log(`DEBUG: Envio de formulário encontrado para contato ${contactId}: Form ID ${parsedMetadata.formId}, Nome: ${parsedMetadata.title}`);
      return { formId: String(parsedMetadata.formId), formName: String(parsedMetadata.title) };
    } else {
      console.warn(`WARN: FORM_SUBMISSION encontrado para contato ${contactId} (Eng. ID ${latestFormSubmission.id}), mas metadata não contém formId/title. Metadata:`, parsedMetadata);
      return { formId: null, formName: 'Nome do Formulário Indisponível (metadata)' };
    }
  } catch (error) {
    console.error(`ERRO CRÍTICO ao buscar envio de formulário para contactId ${contactId}:`, error);
    if (error.body) console.error('Detalhes do erro (body):', typeof error.body === 'string' ? error.body : JSON.stringify(error.body, null, 2));
    console.error('Stack:', error.stack);
    return null;
  }
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
    return [];
  } finally {
    console.timeEnd('ads-fetch');
  }
}




async function countDeals() {
  console.log('DEBUG: Entrou na função countDeals.');
  if (!hubspotClient) {
    console.warn('Cliente HubSpot não inicializado. Pulando busca de negócios.');
    return {
      totalOpenHubSpotDeals: 0, totalClosedWonHubSpotDeals: 0, totalLostHubSpotDeals: 0,
      dealCampaigns: {}, dealFormSubmissions: {},
    };
  }
  let totalOpenHubSpotDeals = 0, totalClosedWonHubSpotDeals = 0, totalLostHubSpotDeals = 0;
  const dealCampaignsData = {}, dealFormSubmissionsData = {};
  let after, processedDealsCount = 0;
  const MAX_DEALS_TO_PROCESS = 10000;

  do {
    const request = {
      filterGroups: [],
      properties: ['dealstage', 'dealname', 'hs_object_id'],
      associations: ['marketing_campaign', 'contacts'],
      limit: 100, after
    };
    let response;
    const maxAttempts = 5, baseDelay = 1000;
    let attempt = 0;
    while (attempt < maxAttempts) {
      try {
        response = await hubspotClient.crm.deals.searchApi.doSearch(request);
        break;
      } catch (err) {
        const status = err.response?.status || err.code || err.message;
        console.error(`Erro na API HubSpot (tentativa ${attempt + 1} para buscar negócios): Status ${status}, Mensagem: ${err.message}`);
        if (String(status).includes('429') && attempt < maxAttempts - 1) {
          const wait = baseDelay * Math.pow(2, attempt);
          await delay(wait);
          attempt++;
        } else {
          console.error('DEBUG: Falha definitiva ao buscar negócios. Abortando.');
          return {
            totalOpenHubSpotDeals: 0, totalClosedWonHubSpotDeals: 0, totalLostHubSpotDeals: 0,
            dealCampaigns: {}, dealFormSubmissions: {}
          };
        }
      }
    }
    if (!response || !response.results) {
      console.warn('WARN: Nenhuma resposta ou resultados da API de negócios do HubSpot.');
      break;
    }
    for (const deal of response.results || []) {
      processedDealsCount++;
      const dealId = deal.id;
      const currentDealStage = deal.properties.dealstage;
      const associatedCampaignNames = [];
      if (deal.associations?.marketing_campaign?.results?.length) {
        for (const assoc of deal.associations.marketing_campaign.results) {
          try {
            const camp = await hubspotClient.marketing.campaigns.getById(assoc.id);
            associatedCampaignNames.push(camp.name);
          } catch (campErr) {
            associatedCampaignNames.push('Nome da Campanha Desconhecido');
          }
        }
      }
      dealCampaignsData[dealId] = { campaignNames: associatedCampaignNames, dealstage: currentDealStage };
      const contactAssociations = deal.associations?.contacts?.results;
      const primaryContactId = contactAssociations?.[0]?.id;
      let formNameForDeal = 'Formulário Desconhecido (sem contato)';
      if (primaryContactId) {
        const formSubmissionInfo = await getContactFormSubmission(primaryContactId);
        if (formSubmissionInfo?.formName) {
          formNameForDeal = formSubmissionInfo.formName;
        } else {
          formNameForDeal = 'Formulário Desconhecido (sem envio registrado)';
        }
      }
      dealFormSubmissionsData[dealId] = formNameForDeal;
      if (currentDealStage === '148309307') {
        totalClosedWonHubSpotDeals++;
      } else if (currentDealStage === 'closedlost') {
        totalLostHubSpotDeals++;
      } else {
        totalOpenHubSpotDeals++;
      }
    }
    after = response.paging?.next?.after;
    if (processedDealsCount >= MAX_DEALS_TO_PROCESS && after) {
      console.warn(`WARN: Limite de ${MAX_DEALS_TO_PROCESS} negócios processados atingido.`);
      after = null;
    }
  } while (after);
  return {
    totalOpenHubSpotDeals, totalClosedWonHubSpotDeals, totalLostHubSpotDeals,
    dealCampaigns: dealCampaignsData, dealFormSubmissions: dealFormSubmissionsData,
  };
}

// Retry helper
// Retry helper
async function retryableCall(fn, args = [], retries = 3, backoffMs = 500) {
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn(...args);
    } catch (e) {
      const code = e.code || (e.response && e.response.status);
      if (code === 403) {
        console.warn('WARN:', fn.name, 'sem escopo necessário:', e.message);
        return null;
      }
      if (code === 429) {
        const wait = backoffMs * Math.pow(2, i);
        console.warn(`WARN: Rate limit em ${fn.name}, retry em ${wait}ms (tentativa ${i+1})`);
        await new Promise(res => setTimeout(res, wait));
        continue;
      }
      throw e;
    }
  }
  console.error(`ERROR: ${fn.name} falhou após ${retries+1} tentativas`);
  return null;
}

// Busca o envio de formulário mais recente para um contato
async function getContactFormSubmission(contactId) {
  if (!hubspotClient) {
    console.warn('WARN: Cliente HubSpot não inicializado.');
    return null;
  }
  if (!contactId) {
    console.log('DEBUG: contactId não fornecido.');
    return null;
  }
  console.log(`DEBUG: Buscando FORM_SUBMISSION para contato ${contactId}`);
  try {
    // 1. buscar associações com retry
    const assocResp = await retryableCall(
      hubspotClient.crm.associations.v4.basicApi.getPage.bind(hubspotClient.crm.associations.v4.basicApi),
      ['contacts', contactId, 'engagements']
    );
    const engagementIds = (assocResp?.results || []).map(a => a.toObjectId);
    console.log(`DEBUG: engagementIds(${engagementIds.length}):`, engagementIds);
    if (!engagementIds.length) return null;

    // 2. ler detalhes com retry
    const props = ['hs_engagement_type','hs_createdate','metadata'];
    const batchReq = { inputs: engagementIds.map(id=>({id})), properties: props };
    const details = await retryableCall(
      hubspotClient.crm.engagements.batchApi.read.bind(hubspotClient.crm.engagements.batchApi),
      [batchReq, false]
    );
    console.log('DEBUG: detalhes engajamentos:', details?.results.length);

    // 3. filtrar o mais recente
    let latest = null;
    for (const eng of (details?.results || [])) {
      if (eng.properties.hs_engagement_type === 'FORM_SUBMISSION') {
        const dt = new Date(eng.properties.hs_createdate);
        if (!latest || dt > new Date(latest.properties.hs_createdate)) {
          latest = eng;
        }
      }
    }
    if (!latest) return null;

    // 4. processar metadata
    let meta = latest.properties.metadata;
    if (!meta) return { formId: null, formName: 'Sem metadata' };
    if (typeof meta === 'string') {
      try { meta = JSON.parse(meta); } catch { console.warn('WARN: JSON metadata inválido'); return null; }
    }
    return { formId: meta.formId, formName: meta.title || 'Sem title' };
  } catch (e) {
    console.error(`ERRO ao buscar FORM_SUBMISSION ${contactId}:`, e.message);
    return null;
  }
}

// Obter contactId a partir de associações deal→contacts
async function getContactIdFromDeal(dealId) {
  if (!dealId) return null;
  const assocResp = await retryableCall(
    hubspotClient.crm.associations.v4.basicApi.getPage.bind(hubspotClient.crm.associations.v4.basicApi),
    ['deals', dealId, 'contacts']
  );
  const results = assocResp?.results ?? [];
  return results[0]?.toObjectId || null;
}

// Pipeline principal
async function executarPipeline() {
  console.log('🚀 Pipeline iniciado');
  try {
    // 1. Buscar campanhas do Google Ads
    const googleAdsCampaigns = await fetchCampaigns();
    const adsMap = {};
    googleAdsCampaigns.forEach(ad => adsMap[ad.name] = { network: ad.network, cost: ad.cost });

    // 2. Contar negócios no HubSpot
    const { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals, dealCampaigns } = await countDeals();
    console.log('📊 Totais HubSpot:', { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals });

    // 3. Obter formulario de cada deal
    const dealForms = {};
    for (const dealId of Object.keys(dealCampaigns)) {
      let contactId = dealCampaigns[dealId].contactId;
      if (!contactId) {
        contactId = await getContactIdFromDeal(dealId);
      }
      if (!contactId) { console.warn(`WARN: dealId ${dealId} sem contactId`); continue; }
      dealForms[dealId] = await retryableCall(getContactFormSubmission, [contactId]) || 'Desconhecido';
    }

    // 4. Agrupar dados
    const aggregation = {};
    for (const dealId of Object.keys(dealForms)) {
      const formName = dealForms[dealId];
      const { campaignNames = [], dealstage } = dealCampaigns[dealId];
      campaignNames.forEach(camp => {
        const key = `${camp}||${formName}`;
        if (!aggregation[key]) {
          const ad = adsMap[camp] || { network: 'N/A', cost: 0 };
          aggregation[key] = { campaign: camp, network: ad.network, cost: ad.cost.toFixed(2), received: 0, open: 0, closed: 0 };
        }
        aggregation[key].received++;
        if (dealstage === 'closedwon' || dealstage === '148309307') aggregation[key].closed++;
        else if (dealstage !== 'closedlost') aggregation[key].open++;
      });
    }

    // 5. Escrever resultados
    const rows = Object.values(aggregation).map(item => ({
      'Nome da Campanha': item.campaign,
      'Rede': item.network,
      'Custo Total no Período': item.cost,
      'Contatos/Formulários Recebidos': item.received,
      'Negócios Abertos': item.open,
      'Negócios Fechados': item.closed
    }));
    const headers = ['Nome da Campanha','Rede','Custo Total no Período','Contatos/Formulários Recebidos','Negócios Abertos','Negócios Fechados'];
    await writeToSheet(rows, 'Dados de Formulários', headers);
    console.log(`✅ Pipeline concluído com ${rows.length} linhas`);
  } catch (error) {
    console.error('❌ Erro no pipeline:', error.message);
    console.error(error.stack);
  }
}

// Vercel Serverless Function Handler (CommonJS)
async function handler(req, res) {
  const invocationId = Math.random().toString(36).substring(2, 15);
  console.log(`[${invocationId}] Handler invocado. Método: ${req.method}`);
  try {
    await executarPipeline();
    console.log(`[${invocationId}] Pipeline executado com sucesso.`);
    res.status(200).send('Pipeline executado com sucesso');
  } catch (error) {
    console.error(`[${invocationId}] Falha no handler:`, error.message);
    res.status(500).send(`Pipeline falhou: ${error.message}`);
  }
}

module.exports = handler;


// Para execução local (opcional)
/*
if (require.main === module) {
  console.log("Executando localmente...");
  (async () => {
    try {
      await executarPipeline();
      console.log("Execução local concluída com sucesso.");
    } catch (error) {
      console.error("Erro na execução local:", error);
    }
  })();
}
*/