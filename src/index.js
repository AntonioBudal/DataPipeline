require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
// const { google } = require('googleapis'); // Não é usado diretamente em index.js

const config = require('./config');
const { writeToSheet } = require('./sheetsWriter'); // Assumindo que sheetsWriter.js está no mesmo diretório

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
  if (!hubspotClient) {
    console.warn('WARN: Cliente HubSpot não inicializado. Impossível buscar envio de formulário.');
    return null;
  }
  if (!contactId) {
    console.log('DEBUG: contactId não fornecido para getContactFormSubmission.');
    return null;
  }
  console.log(`DEBUG: Iniciando busca de envio de formulário para contactId: ${contactId}`);
  try {
    const associatedEngagementsResponse = await hubspotClient.crm.associations.v4.basicApi.getPage(
      'contacts', contactId, 'engagements'
    );
    const engagementIds = associatedEngagementsResponse.results.map(assoc => assoc.toObjectId);
    if (!engagementIds || engagementIds.length === 0) {
      console.log(`DEBUG: Nenhum engajamento encontrado para contactId: ${contactId}`);
      return null;
    }
    const propertiesToFetch = ['hs_engagement_type', 'hs_createdate', 'metadata'];
    const batchReadRequest = {
      inputs: engagementIds.map(id => ({ id })),
      properties: propertiesToFetch,
    };
    const engagementDetails = await hubspotClient.crm.engagements.batchApi.read(batchReadRequest, false);
    let latestFormSubmission = null;
    for (const engagement of engagementDetails.results) {
      if (engagement.properties.hs_engagement_type === 'FORM_SUBMISSION') {
        const engagementCreateDate = engagement.properties.hs_createdate;
        if (!latestFormSubmission || (engagementCreateDate && new Date(engagementCreateDate) > new Date(latestFormSubmission.properties.hs_createdate))) {
          latestFormSubmission = engagement;
        }
      }
    }
    if (!latestFormSubmission) {
      console.log(`DEBUG: Nenhum engajamento do tipo FORM_SUBMISSION encontrado para contactId: ${contactId}.`);
      return null;
    }
    let metadata = latestFormSubmission.properties.metadata;
    let parsedMetadata = null;
    if (metadata) {
      if (typeof metadata === 'string') {
        try { parsedMetadata = JSON.parse(metadata); } catch (e) {
          console.error(`ERROR: Falha ao parsear metadata (string JSON) para engajamento ${latestFormSubmission.id} do contato ${contactId}:`, e.message);
        }
      } else if (typeof metadata === 'object') {
        parsedMetadata = metadata;
      }
    }
    if (parsedMetadata && parsedMetadata.formId && parsedMetadata.title) {
      console.log(`DEBUG: Envio de formulário encontrado para contato ${contactId}: Form ID ${parsedMetadata.formId}, Nome: ${parsedMetadata.title}`);
      return { formId: String(parsedMetadata.formId), formName: String(parsedMetadata.title) };
    } else {
      console.warn(`WARN: FORM_SUBMISSION encontrado para contato ${contactId} (Eng. ID ${latestFormSubmission.id}), mas metadata não contém formId/title. Metadata:`, parsedMetadata);
      return { formId: null, formName: 'Nome do Formulário Indisponível (metadata)' };
    }
  } catch (error) {
    console.error(`ERRO CRÍTICO ao buscar envio de formulário para contactId ${contactId}:`, error.message);
    if (error.body) console.error("Detalhes do erro (body):", typeof error.body === 'string' ? error.body : JSON.stringify(error.body));
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
      if (currentDealStage === 'closedwon' || currentDealStage === '148309307') {
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

async function executarPipeline() {
  console.log('🚀 Pipeline iniciado');
  try {
    const googleAdsCampaigns = await fetchCampaigns();
    const {
      totalOpenHubSpotDeals, totalClosedWonHubSpotDeals, totalLostHubSpotDeals,
      dealCampaigns, dealFormSubmissions
    } = await countDeals();

    console.log('📊 Contagem de negócios HubSpot (Totais):', { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals, totalLostHubSpotDeals });

    // --- Lógica para Google Ads (aba "Campanhas Ads") ---
    const resultsForAdsSheet = [];
    const adsHeaders = ['Nome da Campanha', 'Rede', 'Custo Total no Período', 'Negócios Abertos', 'Negócios Fechados'];

    if (googleAdsCampaigns && googleAdsCampaigns.length > 0) {
      console.log(`ℹ️ Processando ${googleAdsCampaigns.length} campanhas do Google Ads...`);
      for (const adCampaign of googleAdsCampaigns) {
        let openCountForAdCampaign = 0;
        let closedWonCountForAdCampaign = 0;
        for (const dealId in dealCampaigns) {
          if (dealCampaigns[dealId].campaignNames.includes(adCampaign.name)) {
            const stage = dealCampaigns[dealId].dealstage;
            if (stage === 'closedwon' || stage === '148309307') {
              closedWonCountForAdCampaign++;
            } else if (stage !== 'closedlost') { // Contar como aberto se não for ganho nem perdido
              openCountForAdCampaign++;
            }
          }
        }
        // FILTRO: Adicionar campanha à planilha somente se tiver negócios associados
        if (openCountForAdCampaign > 0 || closedWonCountForAdCampaign > 0) {
          resultsForAdsSheet.push({
            'Nome da Campanha': adCampaign.name,
            'Rede': adCampaign.network,
            'Custo Total no Período': adCampaign.cost.toFixed(2),
            'Negócios Abertos': openCountForAdCampaign,
            'Negócios Fechados': closedWonCountForAdCampaign,
          });
        } else {
          console.log(`INFO: Campanha Ads "${adCampaign.name}" não possui negócios HubSpot associados (abertos/ganhos). Não será incluída.`);
        }
      }
    }

    if (resultsForAdsSheet.length > 0) {
      await writeToSheet(resultsForAdsSheet, 'Campanhas Ads', adsHeaders);
      console.log(`✅ ${resultsForAdsSheet.length} campanha(s) Ads com negócios enviada(s) para "Campanhas Ads".`);
    } else if (totalOpenHubSpotDeals > 0 || totalClosedWonHubSpotDeals > 0) {
      console.log('INFO: Nenhuma campanha do Google Ads com negócios associados. Escrevendo resumo HubSpot em "Campanhas Ads".');
      await writeToSheet([{
        'Nome da Campanha': 'HubSpot - Resumo Geral de Negócios',
        'Rede': 'N/A (HubSpot)',
        'Custo Total no Período': 0,
        'Negócios Abertos': totalOpenHubSpotDeals,
        'Negócios Fechados': totalClosedWonHubSpotDeals,
      }], 'Campanhas Ads', adsHeaders);
    } else {
      console.log('INFO: Nenhuma campanha Ads com negócios e nenhum negócio HubSpot geral para "Campanhas Ads". Aba será limpa com cabeçalhos.');
      await writeToSheet([], 'Campanhas Ads', adsHeaders); // Limpa e escreve apenas cabeçalhos
    }

    // --- Lógica para Formulários (aba "Dados de Formulários") - MODIFICADO ---
    const formDataForSheet = [];
    const formHeaders = ['Nome do Formulário', 'Visualizações', 'Envios', 'Negócios Abertos', 'Negócios Fechados'];
    const formNameToCounts = {}; // Objeto para armazenar contagens por nome de formulário

    for (const dealId in dealFormSubmissions) {
      const formName = dealFormSubmissions[dealId] || 'Formulário Desconhecido (geral)';
      const dealInfo = dealCampaigns[dealId];
      const stage = dealInfo ? dealInfo.dealstage : null;

      if (!formNameToCounts[formName]) {
        formNameToCounts[formName] = {
          open: 0,
          closed: 0,
          // Adicione aqui 'Visualizações' e 'Envios' se você tiver essa informação
        };
      }

      if (stage === 'closedwon' || stage === '148309307') {
        formNameToCounts[formName].closed++;
      } else if (stage !== 'closedlost' && stage) {
        formNameToCounts[formName].open++;
      }
    }

    for (const formName in formNameToCounts) {
      const counts = formNameToCounts[formName];
      if (counts.open > 0 || counts.closed > 0) {
        formDataForSheet.push({
          'Nome do Formulário': formName,
          'Visualizações': 'N/A', // Se disponível, substitua
          'Envios': 'N/A',     // Se disponível, substitua
          'Negócios Abertos': counts.open,
          'Negócios Fechados': counts.closed,
        });
      } else {
        console.log(`INFO: Formulário "${formName}" não possui negócios HubSpot associados (abertos/ganhos). Não será incluído.`);
      }
    }

    if (formDataForSheet.length > 0) {
      await writeToSheet(formDataForSheet, 'Dados de Formulários', formHeaders);
      console.log(`✅ ${formDataForSheet.length} formulário(s) com negócios enviado(s) para "Dados de Formulários".`);
    } else {
      console.log('INFO: Nenhum formulário com negócios associados para "Dados de Formulários". Aba será limpa com cabeçalhos.');
      await writeToSheet([], 'Dados de Formulários', formHeaders);
    }

    console.log('📝 Pipeline concluído.');

  } catch (error) {
    console.error('❌ Erro CRÍTICO no pipeline principal:', error.message);
    console.error('DEBUG: Stack do erro no pipeline principal:', error.stack);
  }
}

// Vercel Serverless Function Handler
export default async function handler(req, res) {
  const invocationId = Math.random().toString(36).substring(2, 15);
  console.log(`[${invocationId}] Handler da Vercel invocado. Método: ${req.method}, Hora: ${new Date().toISOString()}`);
  try {
    await executarPipeline();
    console.log(`[${invocationId}] Pipeline executado com sucesso.`);
    return res.status(200).send('Pipeline executado com sucesso');
  } catch (error) {
    console.error(`[${invocationId}] Pipeline falhou no handler da Vercel:`, error.message);
    if (error.stack) console.error(`DEBUG: [${invocationId}] Stack (handler Vercel):`, error.stack);
    return res.status(500).send(`Pipeline falhou: ${error.message}`);
  }
}

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