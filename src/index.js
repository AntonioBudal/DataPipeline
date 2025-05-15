require('dotenv').config();
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');
const { google } = require('googleapis');

// Carrega configurações do arquivo separado
const config = require('./config'); // Certifique-se que este arquivo existe e exporta as configs corretamente
const { writeToSheet } = require('./sheetsWriter'); // Certifique-se que este arquivo existe e exporta a função

// DEBUG: Log para verificar se o token do HubSpot está sendo carregado do config
console.log(`DEBUG: HubSpot Token from config (initial load - first 5 chars): ${config.hubspot && config.hubspot.privateAppToken ? config.hubspot.privateAppToken.substring(0, 5) + '...' : 'NOT FOUND or config.hubspot is undefined'}`);

// Inicializa cliente do Google Ads
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
  console.log('✅ Cliente Google Ads inicializado com sucesso.');
} catch (error) {
  console.error('❌ Erro CRÍTICO ao inicializar o cliente Google Ads:', error.message);
  console.error('DEBUG: Google Ads Initialization Error Stack:', error.stack);
  // Dependendo da criticidade, você pode querer impedir a execução do pipeline aqui
}


// Inicializa cliente do HubSpot
let hubspotClient;
try {
  const tokenForHubspot = config.hubspot && config.hubspot.privateAppToken;
  console.log(`DEBUG: Attempting to initialize HubSpot client. Token available (first 5 chars): ${tokenForHubspot ? tokenForHubspot.substring(0, 5) + '...' : 'NO TOKEN'}`);

  if (!tokenForHubspot) {
    console.error('❌ CRITICAL: HubSpot Private App Token is MISSING in config before client initialization!');
    // Lançar um erro ou garantir que hubspotClient permaneça undefined é uma boa prática
    throw new Error('HubSpot Private App Token is missing.');
  }

  hubspotClient = new Hubspot.Client({
    accessToken: tokenForHubspot,
  });
  console.log('✅ Cliente HubSpot inicializado (tentativa).');

  // DEBUG: Verificar se os métodos esperados existem
  if (hubspotClient && hubspotClient.crm && hubspotClient.crm.deals && hubspotClient.crm.deals.searchApi && typeof hubspotClient.crm.deals.searchApi.doSearch === 'function') {
    console.log('DEBUG: hubspotClient.crm.deals.searchApi.doSearch IS a function and available.');
  } else {
    console.warn('DEBUG: hubspotClient.crm.deals.searchApi.doSearch IS NOT available or not a function. Structure might be wrong or initialization failed silently.');
    console.log('DEBUG: typeof hubspotClient:', typeof hubspotClient);
    if (hubspotClient) {
      console.log('DEBUG: typeof hubspotClient.crm:', typeof hubspotClient.crm);
      if (hubspotClient.crm) {
        console.log('DEBUG: hubspotClient.crm.deals:', typeof hubspotClient.crm.deals);
        if (hubspotClient.crm.deals) {
          console.log('DEBUG: hubspotClient.crm.deals.searchApi:', typeof hubspotClient.crm.deals.searchApi);
        }
      }
    }
  }
  // Verificação para a API de campanhas de marketing
  if (hubspotClient && hubspotClient.marketing && hubspotClient.marketing.campaigns && typeof hubspotClient.marketing.campaigns.getById === 'function') {
    console.log('DEBUG: hubspotClient.marketing.campaigns.getById IS a function and available.');
  } else {
    console.warn('DEBUG: hubspotClient.marketing.campaigns.getById IS NOT available or not a function.');
  }

} catch (error) {
  console.error('❌ Erro CRÍTICO ao inicializar o cliente HubSpot:', error.message);
  console.error('DEBUG: HubSpot Initialization Error Stack:', error.stack);
  hubspotClient = undefined; // Garante que o cliente não seja usado se a inicialização falhar
}

// Função para coletar campanhas do Google Ads
async function fetchCampaigns() {
  if (!adsCustomer) {
    console.warn('⚠️ Cliente Google Ads não inicializado. Pulando busca de campanhas.');
    return [];
  }
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
    return campaigns;
  } catch (error) {
    console.error('❌ Erro ao buscar campanhas do Google Ads:', error);
    if (error.errors) { // Erros da API do Google Ads geralmente têm um array 'errors'
        console.error('DEBUG: Google Ads API Error Details:', JSON.stringify(error.errors, null, 2));
    }
    if (error.stack) {
        console.error('DEBUG: Google Ads API Error Stack:', error.stack);
    }
    return []; // Retorna um array vazio em caso de erro para não quebrar o pipeline
  } finally {
    console.timeEnd('ads-fetch'); // Garante que o timeEnd seja chamado
  }
}

// Função para contar negócios no HubSpot e obter nomes de campanhas associadas
async function countDeals() {
  console.log('DEBUG: Entered countDeals function.');
  if (!hubspotClient) {
    console.warn('⚠️ Cliente HubSpot não inicializado ou falhou na inicialização. Pulando busca de negócios.');
    return { openDeals: 0, closedWonDeals: 0, dealCampaigns: {} };
  }
  console.log('DEBUG: HubSpot client seems initialized, proceeding to prepare request for deals.');

  const request = {
    filterGroups: [
      // Adicione aqui seus filtros de negócio, se necessário
    ],
    properties: ['dealstage'], // Certifique-se que 'dealstage' é uma propriedade válida e você tem permissão.
    associations: ['marketing_campaign'], // Verifique se 'marketing_campaign' é o nome correto da associação.
    limit: 100,
  };
  console.log('DEBUG: HubSpot search request object for deals:', JSON.stringify(request, null, 2));

  try {
    console.log('DEBUG: Attempting API call: hubspotClient.crm.deals.searchApi.doSearch(request)...');
    const response = await hubspotClient.crm.deals.searchApi.doSearch(request);
    console.log('DEBUG: HubSpot API call for deals search supposedly successful. Number of results:', response.results ? response.results.length : 0);

    let openDeals = 0;
    let closedWonDeals = 0;
    const dealCampaigns = {};

    for (const deal of response.results || []) {
      const dealId = deal.id;
      const associatedCampaignNames = [];

      console.log(`DEBUG: Processing deal ID ${dealId}. Associations found:`, deal.associations ? Object.keys(deal.associations) : 'None');

      if (deal.associations && deal.associations.marketing_campaign && deal.associations.marketing_campaign.results) {
        console.log(`DEBUG: Deal ID ${dealId} has ${deal.associations.marketing_campaign.results.length} associated marketing campaigns.`);
        for (const campaignAssociation of deal.associations.marketing_campaign.results) {
          const campaignId = campaignAssociation.id;
          try {
            console.log(`DEBUG: Attempting to fetch campaign details for HubSpot Campaign ID: ${campaignId}`);
            const campaignResponse = await hubspotClient.marketing.campaigns.getById(campaignId); // Make sure this API path is correct
            associatedCampaignNames.push(campaignResponse.name);
            console.log(`DEBUG: Successfully fetched campaign name: "${campaignResponse.name}" for ID: ${campaignId}`);
          } catch (campaignError) {
            console.error(`❌ Erro ao buscar detalhes da campanha HubSpot ID ${campaignId}:`, campaignError.message);
            if (campaignError.response && campaignError.response.body) {
                console.error(`DEBUG: HubSpot Campaign Fetch Error Body (ID ${campaignId}):`, JSON.stringify(campaignError.response.body, null, 2));
            } else if (campaignError.code) {
                console.error(`DEBUG: HubSpot Campaign Fetch Error Code (ID ${campaignId}):`, campaignError.code);
            }
            if (campaignError.stack) {
                console.error(`DEBUG: HubSpot Campaign Fetch Error Stack (ID ${campaignId}):`, campaignError.stack);
            }
            associatedCampaignNames.push('Nome da Campanha Desconhecido');
          }
        }
      } else {
        console.log(`DEBUG: Deal ID ${dealId} has no 'marketing_campaign' associations or no results in it.`);
      }

      dealCampaigns[dealId] = {
        campaignNames: associatedCampaignNames,
        dealstage: deal.properties.dealstage,
      };

      if (deal.properties.dealstage === '148309307') { // Presumindo que '148309307' é o ID do estágio "ganho"
        closedWonDeals++;
      } else {
        openDeals++;
      }
    }
    return { openDeals, closedWonDeals, dealCampaigns };

  } catch (err) {
    console.error(`❌ Erro HubSpot ao buscar negócios (deals) com associações de campanha:`, err.message);
    if (err.response && err.response.body) { // Erros da API do HubSpot geralmente têm detalhes no body
        console.error('DEBUG: HubSpot API Error Response Body (deals search):', JSON.stringify(err.response.body, null, 2));
    } else if (err.code) { // Para erros de rede ou outros que não são respostas HTTP da API
        console.error('DEBUG: HubSpot Error Code (deals search):', err.code);
    }
    if (err.stack) {
        console.error('DEBUG: HubSpot Error Stack (deals search):', err.stack);
    }
    return { openDeals: 0, closedWonDeals: 0, dealCampaigns: {} }; // Retorna padrão em caso de erro
  }
}

// Função principal que executa todo o pipeline
async function executarPipeline() {
  console.log('🚀 Pipeline iniciado');
  try {
    const campaigns = await fetchCampaigns();
    console.log('🔍 Campanhas do Google Ads (resultado da busca):', campaigns);

    const { openDeals, closedWonDeals, dealCampaigns } = await countDeals();
    console.log('📊 Contagem de negócios do HubSpot:', { openDeals, closedWonDeals });
    console.log('🤝 Negócios do HubSpot e suas campanhas:', dealCampaigns);

    const results = [];
    if (campaigns && campaigns.length > 0) {
        for (const camp of campaigns) {
          let openCount = 0;
          let closedCount = 0;

          for (const dealId in dealCampaigns) {
            if (dealCampaigns[dealId].campaignNames.includes(camp.name)) {
              if (stage === 'closedwon' || stage === '148309307') {
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
    } else {
        console.log('ℹ️ Nenhuma campanha do Google Ads para processar ou ocorreu um erro ao buscá-las.');
    }


    console.log('📝 Dados combinados antes de escrever na planilha:', results);

    // ADICIONANDO LINHA DE TESTE PARA O GOOGLE SHEETS (Comentado para depuração do HubSpot)
    // const testData = [{ name: 'TESTE', network: 'Manual', cost: 0, open: 0, closed: 0 }];
    // await writeToSheet(testData);
    // console.log('✅ Dados de teste enviados para o Google Sheets');

    if (results.length > 0) {
      await writeToSheet(results);
      console.log('✅ Planilha do Google Sheets atualizada com sucesso.');
    } else {
      console.log('ℹ️ Nenhum dado combinado para escrever na planilha.');
      // Opcional: escrever uma linha de "sem dados" ou limpar a planilha.
      // await writeToSheet([{ name: 'SEM DADOS', network: '-', cost: 0, open: 0, closed: 0 }]);
      // console.log('ℹ️ Planilha atualizada para indicar ausência de dados.');
    }

  } catch (error) {
    console.error('❌ Erro no pipeline principal:', error.message);
    if (error.stack) {
        console.error('DEBUG: Erro no pipeline principal (Stack):', error.stack);
    }
    throw error; // Rejoga o erro para ser capturado pelo handler da Vercel
  }
}

// Handler para Vercel Serverless Function
// Se você estiver usando CommonJS (com `require`), a forma mais comum de exportar é `module.exports`.
// No entanto, a Vercel pode ser flexível. Mantendo o `export default` do seu original.
export default async function handler(req, res) {
  console.log(`ℹ️ Handler da Vercel invocado. Método: ${req.method}`);
  try {
    await executarPipeline();
    return res.status(200).send('✅ Pipeline executado com sucesso');
  } catch (error) {
    // O erro já deve ter sido logado no `executarPipeline`
    console.error('❌ Pipeline falhou no handler da Vercel (erro pego no handler):', error.message);
    if (error.stack && !error.message.includes(error.stack.split('\n')[1])) { // Evitar log duplicado do stack se já no message
        console.error('DEBUG: Pipeline falhou no handler da Vercel (Stack):', error.stack);
    }
    return res.status(500).send(`❌ Pipeline falhou: ${error.message}`);
  }
}

// Para permitir execução local para teste rápido (opcional, descomentar se necessário):
/*
if (require.main === module && process.env.NODE_ENV !== 'test') { // Evita executar em testes se houver
  console.log('ℹ️ Executando pipeline localmente para teste (chamada direta)...');
  executarPipeline().then(() => {
    console.log('✅ Pipeline local concluído com sucesso.');
    process.exit(0);
  }).catch(err => {
    console.error("❌ Erro fatal na execução local do pipeline:", err);
    process.exit(1);
  });
}
*/