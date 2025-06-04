// index.js - Vercel Serverless Function for HubSpot and Google Ads Data Processing

// Import necessary libraries
const { GoogleAdsApi } = require("google-ads-api");
const hubspot = require("@hubspot/api-client");
const config = require("./config");
const { writeToSheet } = require("./sheetsWriter");
let adsCustomer, adsApi;

// --- Global Client Initializations ---
// These clients are initialized once when the function starts (cold start)
// and reused for subsequent invocations (warm start).

// Log initial HubSpot token status for debugging
console.log(
  `DEBUG: config.ads.refreshToken from loaded config: ${
    config.ads.refreshToken
      ? config.ads.refreshToken.substring(0, 10) + "..."
      : "NOT LOADED"
  }`
);
console.log(
  `DEBUG: HubSpot Token from config (initial load - first 5 chars): ${
    config.hubspot && config.hubspot.privateAppToken
      ? config.hubspot.privateAppToken.substring(0, 5) + "..."
      : "NOT FOUND or config.hubspot is undefined"
  }`
);

// HubSpot Client Initialization
let hubspotClient;
try {
  const hubspotToken =
    process.env.HUBSPOT_PRIVATE_APP_TOKEN ||
    (config.hubspot && config.hubspot.privateAppToken);
  if (!hubspotToken) {
    console.error(
      "CRITICAL (HubSpot): HUBSPOT_PRIVATE_APP_TOKEN (ou config.hubspot.privateAppToken) não encontrado!"
    );
    throw new Error("HubSpot Private App Token não fornecido.");
  }
  console.log(
    `INFO (HubSpot): Tentando inicializar cliente HubSpot com token (primeiros 5 chars): ${hubspotToken.substring(
      0,
      5
    )}...`
  );
  // Mude de Hubspot.Client para hubspot.Client
  hubspotClient = new hubspot.Client({ accessToken: hubspotToken });
  console.log("✅ Cliente HubSpot inicializado com sucesso (tentativa).");

  // Verificações de disponibilidade da API HubSpot (indicativo de escopos do token)
  console.log(
    `DEBUG (HubSpot API Check): crm.contacts.basicApi.getPage IS ${
      hubspotClient.crm?.contacts?.basicApi?.getPage ? "" : "NOT "
    }available.`
  );
  console.log(
    `DEBUG (HubSpot API Check): crm.deals.batchApi.read IS ${
      hubspotClient.crm?.deals?.batchApi?.read ? "" : "NOT "
    }available.`
  );
  console.log(
    `DEBUG (HubSpot API Check): marketing.forms.v3.statisticsApi.getById IS ${
      hubspotClient.marketing?.forms?.v3?.statisticsApi?.getById ? "" : "NOT "
    }available.`
  );
  // Adicione mais verificações conforme necessário
} catch (error) {
  console.error(
    "❌ CRITICAL (HubSpot): Erro ao inicializar o cliente HubSpot:",
    error.message
  );
  console.error(
    "DEBUG (HubSpot): Stack de erro da inicialização:",
    error.stack
  );
  hubspotClient = undefined;
}

console.log("--- Inicialização do Google Ads Client ---");

try {
  const adsClientId = process.env.GOOGLE_ADS_CLIENT_ID;
  const adsClientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET;
  const adsDeveloperToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
  const adsRefreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN;
  const adsCustomerId = process.env.GOOGLE_ADS_CUSTOMER_ID;
  const adsLoginCustomerId = process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID; // Optional

  if (
    adsClientId &&
    adsClientSecret &&
    adsDeveloperToken &&
    adsRefreshToken &&
    adsCustomerId
  ) {
    adsApi = new GoogleAdsApi({
      client_id: adsClientId,
      client_secret: adsClientSecret,
      developer_token: adsDeveloperToken,
      // Você PODE remover o refresh_token daqui se ele estiver no Customer.
      // refresh_token: adsRefreshToken
    });
    console.log("✅ GoogleAdsApi instanciada com sucesso.");

    adsCustomer = adsApi.Customer({
      customer_id: adsCustomerId,
      login_customer_id: adsLoginCustomerId, // Descomente e use se for uma conta MCC
      refresh_token: adsRefreshToken, 
    });
    console.log(
      `✅ adsCustomer instanciado com sucesso para o ID: ${adsCustomerId}`
    );
  } else {
    console.warn(
      "WARN: Uma ou mais variáveis de ambiente do Google Ads estão faltando. Cliente não inicializado."
    );
  }
} catch (error) {
  console.error(
    "❌ CRITICAL: Erro ao inicializar o cliente do Google Ads:",
    error.message
  );
  console.error(
    "DEBUG: Stack trace do erro de inicialização do Google Ads:",
    error.stack
  );
}

console.log("-----------------------------------------");

// Helper function for introducing delays (e.g., for rate limiting)
const delay = (ms) => new Promise((res) => setTimeout(res, ms));

/**
 * Retries a function call with exponential backoff for transient errors (e.g., rate limits).
 * Handles 403 Forbidden errors as non-retryable permission issues.
 * @param {Function} fn - The function to call.
 * @param {Array} args - Arguments to pass to the function.
 * @param {number} retries - Maximum number of retries.
 * @param {number} baseBackoffMs - Base delay in milliseconds for exponential backoff.
 * @returns {Promise<any>} - The result of the function call.
 * @throws {Error} - Throws an error if all retries fail or if a 403 is encountered.
 */
async function retryableCall(apiCall, args, retries = 3, initialDelayMs = 1000) {
  for (let i = 0; i <= retries; i++) {
    try {
      const result = await apiCall(...args);
      return result;
    } catch (error) {
      const statusCode = error.statusCode || (error.response && error.response.status);
      const errorMessage = error.message || (error.response && error.response.data && error.response.data.message) || JSON.stringify(error);

      console.error(`ERROR: bound ${apiCall.name || 'API Call'} failed after ${i + 1} attempt(s): Status ${statusCode}, Message: ${errorMessage}`);
      console.error(`DEBUG: Error details:`, error);

      // Only retry on specific status codes (e.g., 429 for rate limits, 5xx for server errors)
      // or if no status code is available (could be network issue).
      if (statusCode && (statusCode === 429 || (statusCode >= 500 && statusCode < 600))) {
        if (i < retries) {
          const waitTime = initialDelayMs * Math.pow(2, i) + Math.random() * 500; // Exponential backoff with jitter
          console.warn(`WARN: Retrying in ${waitTime / 1000} seconds...`);
          await delay(waitTime);
        } else {
          console.error(`ERROR: Max retries (${retries}) exceeded for API call.`);
          throw error; // Re-throw if max retries reached
        }
      } else if (statusCode === 400 || statusCode === 401 || statusCode === 403 || statusCode === 404) {
        // For known client-side errors, don't retry, just throw immediately.
        // A 400 Bad Request usually means the request itself is malformed and won't succeed on retry.
        console.error(`ERROR: Non-retryable API error (Status ${statusCode}). Aborting retries.`);
        throw error;
      } else {
        // For any other unexpected errors, retry up to max retries
        if (i < retries) {
          const waitTime = initialDelayMs * Math.pow(2, i) + Math.random() * 500;
          console.warn(`WARN: Retrying in ${waitTime / 1000} seconds due to unknown error...`);
          await delay(waitTime);
        } else {
          console.error(`ERROR: Max retries (${retries}) exceeded for API call due to unexpected error.`);
          throw error;
        }
      }
    }
  }
}

/**
 * Fetches Google Ads campaigns. Skips if Google Ads client is not initialized.
 * @returns {Promise<Array<Object>>} - An array of Google Ads campaign objects.
 */
// Function to format date to YYYY-MM-DD
function formatDate(date) {
  const d = new Date(date);
  let month = "" + (d.getMonth() + 1);
  let day = "" + d.getDate();
  const year = d.getFullYear();
  if (month.length < 2) month = "0" + month;
  if (day.length < 2) day = "0" + day;
  return [year, month, day].join("-");
}

/**
 * Fetches form analytics (views and submissions) from HubSpot.
 * This function is conditional based on HubSpot plan permissions.
 * @param {Array<string>} formIds - An array of HubSpot Form IDs.
 * @param {Date} startDate - Start date for analytics.
 * @param {Date} endDate - End date for analytics.
 * @returns {Promise<Object>} - A map of formId to its analytics data.
 */
async function fetchFormAnalytics(formIds, startDate, endDate) {
  // Check if HubSpot client and the specific Forms Statistics API method are available
  if (
    !hubspotClient ||
    !formIds.length ||
    !(
      hubspotClient.marketing.forms?.v3?.statisticsApi?.getById &&
      typeof hubspotClient.marketing.forms.v3.statisticsApi.getById ===
        "function"
    )
  ) {
    console.warn(
      "DEBUG: Cliente HubSpot ou Forms API de estatísticas indisponível ou não há formulários para buscar. Retornando mapa vazio."
    );
    return {}; // Return empty if API is not available (due to plan limitations or other issues)
  }

  const formStatsMap = {};
  const startParam = startDate.toISOString();
  const endParam = endDate.toISOString();

  console.log(
    `DEBUG: Buscando estatísticas para ${formIds.length} formulários entre ${startParam} e ${endParam}...`
  );

  // Create a promise for each form to fetch its statistics concurrently
  const formPromises = formIds.map((formId) => {
    return retryableCall(
      hubspotClient.marketing.forms.v3.statisticsApi.getById.bind(
        hubspotClient.marketing.forms.v3.statisticsApi
      ),
      [formId, startParam, endParam]
    )
      .then((stats) => ({ formId, stats })) // On success, return formId and stats
      .catch((error) => {
        // Handle errors for individual form stats fetches
        if (error.message.includes("403")) {
          console.warn(
            `WARN: Acesso negado (403) para estatísticas do formulário ${formId}. Isso pode ser uma limitação do plano ou escopo incorreto.`
          );
        } else {
          console.error(
            `ERRO ao buscar estatísticas para formulário ${formId}: ${error.message}`
          );
        }
        return { formId, stats: null }; // Return null stats on error
      });
  });

  // Wait for all form statistics promises to resolve
  const results = await Promise.all(formPromises);

  // Populate the formStatsMap with fetched data
  results.forEach(({ formId, stats }) => {
    if (stats) {
      formStatsMap[formId] = {
        views: stats.views || 0,
        submissions: stats.submissions || 0,
      };
    } else {
      formStatsMap[formId] = { views: 0, submissions: 0 }; // Default to 0 if stats couldn't be fetched
    }
  });

  return formStatsMap;
}

/**
 * Fetches contacts, associated deals, and engagements from HubSpot.
 * Adapts to plan limitations by conditionally attempting to fetch engagement details.
 * @returns {Promise<Object>} - An object containing various counts and maps of HubSpot data.
 */

// Certifique-se de que delay e retryableCall estão definidos ou importados
// (mantendo a versão atualizada do retryableCall que não tenta novamente em 400 Bad Request)

async function fetchFormSubmissionData(hubspotClient) {
  console.log("DEBUG: Entrou na função fetchFormSubmissionData (filtrando por origem de formulário e data com nomes internos corretos).");
  if (!hubspotClient) {
    console.warn("Cliente HubSpot não inicializado. Pulando busca de dados de formulário.");
    return [];
  }

  const formSubmissionRecords = [];

  console.log(`DEBUG: Iniciando busca de contatos com origem em formulário para coletar GCLIDs e informações de formulário usando os nomes internos: hs_analytics_source e hs_analytics_source_data_1.`);

  let contactsSearchAfter = undefined;
  const CONTACT_SEARCH_BATCH_SIZE = 100;

  // Lista de propriedades atualizada para incluir 'num_associated_deals'
  const contactProperties = [
    "createdate",
    "email",
    "gclid",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_object_source_label",
    "form_name",
    "first_conversion_event_name",
    "num_associated_deals" // <--- Adicionando a nova propriedade aqui
  ];

  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const startDateFilterValue = thirtyDaysAgo.toISOString();

  do {
    try {
      const searchBody = {
        filterGroups: [
          {
            filters: [
              {
                propertyName: "hs_object_source_label",
                operator: "EQ",
                value: "FORM",
              },
              {
                propertyName: "createdate",
                operator: "GTE",
                value: startDateFilterValue,
              },
            ],
          },
        ],
        properties: contactProperties,
        limit: CONTACT_SEARCH_BATCH_SIZE,
        after: contactsSearchAfter,
      };

      console.log("DEBUG: HubSpot Contacts Search Body sendo enviado:", JSON.stringify(searchBody, null, 2));

      const searchResponse = await retryableCall(
        hubspotClient.crm.contacts.searchApi.doSearch.bind(
          hubspotClient.crm.contacts.searchApi
        ),
        [searchBody]
      );

      if (searchResponse && searchResponse.results) {
        for (const contact of searchResponse.results) {
          const originalSource = contact.properties.hs_analytics_source || "N/A";
          const originalSourceData1 = contact.properties.hs_analytics_source_data_1 || "N/A";
          const recordSource = contact.properties.hs_object_source_label || "N/A";

          let formName = contact.properties.form_name || "N/A";
          if (formName === "N/A") {
            formName = contact.properties.first_conversion_event_name || "N/A";
          }

          const gclid = contact.properties.gclid || "N/A";
          const contactId = contact.id;
          const submissionTimestamp = contact.properties.createdate || "N/A";
          const email = contact.properties.email || "N/A";
          const numAssociatedDeals = contact.properties.num_associated_deals || 0; // <--- Capturando a nova propriedade

          formSubmissionRecords.push({
            "Contact ID": contactId,
            Email: email,
            "Original Source": originalSource,
            "Detalhes da Fonte Original": originalSourceData1,
            "Nome do Formulário": formName,
            GCLID: gclid,
            "Timestamp do Envio": submissionTimestamp,
            "Record Source": recordSource,
            "Número de Negócios Associados": numAssociatedDeals // <--- Adicionando à saída
          });
        }
      }

      contactsSearchAfter = searchResponse.paging?.next?.after;

      await delay(500);
    } catch (error) {
      console.error(
        "ERRO ao buscar contatos na Search API para dados de formulário:",
        error.message
      );
      console.error(
        "DEBUG (HubSpot Contacts Search Error): Stack trace:",
        error.stack
      );
      contactsSearchAfter = null;
    }
  } while (contactsSearchAfter);

  console.log(
    `INFO: Total de ${formSubmissionRecords.length} registros de formulário (baseado em contatos) encontrados.`
  );
  return formSubmissionRecords;
}

async function fetchUserConversionData(adsCustomer, overallStartDate, overallEndDate) {
    console.log(`DEBUG: Entrou em fetchUserConversionData para o período GERAL de ${overallStartDate} a ${overallEndDate} (SEM RETRYABLE_CALL).`);
    if (!adsCustomer) {
        console.warn('WARN (fetchUserConversionData): adsCustomer não fornecido.');
        return [];
    }

    const allConversionData = [];
    let currentDate = new Date(overallStartDate);
    const endDateObj = new Date(overallEndDate);

    while (currentDate <= endDateObj) {
        const singleDayFormatted = formatDate(currentDate); // Sua função formatDate
        console.log(`DEBUG (fetchUserConversionData): Buscando dados de conversão para o dia: ${singleDayFormatted}`);

        try {
            // REMOVED: retryableCall wrapper
            const stream = await adsCustomer.reportStream({
                entity: 'click_view', // A entidade
                attributes: [
                    'segments.date',
                    'campaign.id',
                    'click_view.gclid',
                ],
                constraints: [
                    `segments.date = '${singleDayFormatted}'`,
                ],
                // limit: 10 // Descomente para testes rápidos em um dia
            });

            for await (const row of stream) {
                allConversionData.push({
                    date: row.segments.date,
                    campaignId: row.campaign ? row.campaign.id : 'N/A',
                    gclid: row.click_view ? row.click_view.gclid : 'N/A',
                });
            }
            console.log(`INFO (fetchUserConversionData): ${allConversionData.length} registros de conversão acumulados após buscar dia ${singleDayFormatted}.`);

        } catch (error) {
            console.error(`❌ ERROR (fetchUserConversionData): Erro ao buscar dados de conversão para o dia ${singleDayFormatted}:`);
            console.error('DEBUG (fetchUserConversionData): Detalhes do erro (sem retry):', JSON.stringify(error, Object.getOwnPropertyNames(error), 2));
            console.warn(`WARN (fetchUserConversionData): Pulando dia ${singleDayFormatted} devido a erro.`);
        }

        currentDate.setDate(currentDate.getDate() + 1);
    }

    console.log(`INFO (fetchUserConversionData): Retornando ${allConversionData.length} registros de conversão no total.`);
    return allConversionData;
}

/**
 * The main pipeline execution function for the Vercel serverless endpoint.
 * This function orchestrates fetching data from Google Ads and HubSpot,
 * processing it, and writing to Google Sheets.
 * @param {Object} req - The Vercel request object.
 * @param {Object} res - The Vercel response object.
 */

// --- Your fetchCampaigns function (MUST be defined somewhere accessible to executarPipeline) ---
// I'm including a simplified version here for context,
// but use your full, working fetchCampaigns function from earlier.
async function fetchCampaigns(adsCustomer) {
  console.log("DEBUG: Entrou em fetchCampaigns (versão integrada).");
  if (!adsCustomer) {
    console.warn(
      "WARN (fetchCampaigns): adsCustomer não fornecido. Pulando busca de campanhas."
    );
    return [];
  }

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - 30); // Last 30 days
  const formattedStartDate = formatDate(startDate);
  const formattedEndDate = formatDate(endDate);

  console.log(
    `DEBUG (fetchCampaigns): Buscando campanhas para ${formattedStartDate} a ${formattedEndDate}`
  );
  let allCampaigns = [];

  try {
    const stream = adsCustomer.reportStream({
      entity: "campaign",
      attributes: [
        "campaign.name",
        "segments.ad_network_type",
        "campaign.id",
        "metrics.clicks",
        "metrics.impressions",
        "metrics.conversions",
      ],
      metrics: ["metrics.cost_micros"],
      date_ranges: [
        { start_date: formattedStartDate, end_date: formattedEndDate },
      ],
    });

    for await (const row of stream) {
      const cost = Number(row.metrics.cost_micros) / 1e6; // Convert micros to standard currency
      const network = row.segments.ad_network_type || "UNKNOWN";
      allCampaigns.push({
        id: row.campaign.id,
        name: row.campaign.name,
        network: network,
        cost: cost,
        clicks: Number(row.metrics.clicks),
        impressions: Number(row.metrics.impressions),
        conversions: Number(row.metrics.conversions),
      });
    }
    console.log(
      `INFO: fetchCampaigns retornou ${allCampaigns.length} campanhas.`
    );
    return allCampaigns;
  } catch (error) {
    console.error(
      "❌ ERROR (fetchCampaigns): Erro ao buscar campanhas do Google Ads:",
      JSON.stringify(error, null, 2)
    );
    console.warn("WARN (fetchCampaigns): Mensagem de erro:", error.message);
    return [];
  }
}
// --- End of fetchCampaigns function ---




async function executarPipeline() {
  console.log("INFO: Executando pipeline...");

  // --- Definir parâmetros internos ou de variáveis de ambiente ---
  const currentSheetName =
    process.env.DEFAULT_SHEET_NAME || "Google Ads Campaigns";
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - 30); // Last 30 days
  const currentStartDate = formatDate(startDate);
  const currentEndDate = formatDate(endDate);
  const currentDataTypes = process.env.DATA_TYPES
    ? process.env.DATA_TYPES.split(",")
    : ["googleAds", "userConversions", "hubspotForms"]; // Adicionado 'hubspotForms'

  // --- Environment Variable Check ---
  console.log("\n--- Verificação Inicial de Variáveis de Ambiente ---");
  const hubspotPrivateAppToken = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
  const googleAdsClientId = process.env.GOOGLE_ADS_CLIENT_ID;
  const googleAdsClientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET;
  const googleAdsDeveloperToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
  const googleAdsRefreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN;
  const googleAdsCustomerId = process.env.GOOGLE_ADS_CUSTOMER_ID;
  const googleAdsLoginCustomerId = process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID;
  const googleSheetsClientId = process.env.GOOGLE_SHEETS_CLIENT_ID;
  const googleSheetsClientSecret = process.env.GOOGLE_SHEETS_CLIENT_SECRET;
  const googleSheetsRefreshToken = process.env.GOOGLE_SHEETS_REFRESH_TOKEN;
  const googleSheetsId = process.env.GOOGLE_SHEETS_ID;

  console.log(
    `DEBUG: HubSpot Token from config (initial load - first 5 chars): ${
      hubspotPrivateAppToken
        ? hubspotPrivateAppToken.substring(0, 5) + "..."
        : "NOT_LOADED"
    }`
  );
  console.log(`INFO: Google Sheets client ID carregado para autenticação.`);
  console.log(`INFO: Google Sheets client secret carregado para autenticação.`);
  console.log(`INFO: Google Sheets refresh token carregado para autenticação.`);
  console.log(
    `DEBUG: config.ads.refreshToken from loaded config: ${
      googleAdsRefreshToken
        ? googleAdsRefreshToken.substring(0, 10) + "..."
        : "NOT_LOADED"
    }`
  );

  console.log("----------------------------------------------------\n");

  let adsApi;
  let adsCustomer = null;
  let hubspotClient = null; // Inicialize o cliente HubSpot como null aqui

  // --- Google Ads Client Initialization ---
  console.log(
    "DEBUG: Tentando inicializar o cliente Google Ads DENTRO do pipeline..."
  );
  try {
    if (
      googleAdsClientId &&
      googleAdsClientSecret &&
      googleAdsDeveloperToken &&
      googleAdsRefreshToken &&
      googleAdsCustomerId
    ) {
      console.log(
        `DEBUG (Ads Init): googleAdsClientId type: ${typeof googleAdsClientId}, value: '${googleAdsClientId}'`
      );
      console.log(
        `DEBUG (Ads Init): googleAdsClientSecret type: ${typeof googleAdsClientSecret}, value: '${googleAdsClientSecret.substring(
          0,
          10
        )}...'`
      );
      console.log(
        `DEBUG (Ads Init): googleAdsDeveloperToken type: ${typeof googleAdsDeveloperToken}, value: '${googleAdsDeveloperToken}'`
      );
      console.log(
        `DEBUG (Ads Init): googleAdsRefreshToken type: ${typeof googleAdsRefreshToken}, value: '${googleAdsRefreshToken.substring(
          0,
          10
        )}...'`
      );
      console.log(
        `DEBUG (Ads Init): googleAdsCustomerId type: ${typeof googleAdsCustomerId}, value: '${googleAdsCustomerId}'`
      );

      adsApi = new GoogleAdsApi({
        client_id: googleAdsClientId,
        client_secret: googleAdsClientSecret,
        developer_token: googleAdsDeveloperToken,
      });
      console.log("✅ DEBUG (Ads Init): GoogleAdsApi instanciada com sucesso.");

      adsCustomer = adsApi.Customer({
        customer_id: googleAdsCustomerId,
        login_customer_id: googleAdsLoginCustomerId,
        refresh_token: googleAdsRefreshToken,
      });
      console.log(
        `✅ DEBUG (Ads Init): adsCustomer instanciado com sucesso para o ID: ${googleAdsCustomerId}.`
      );

      console.log(
        "DEBUG (Ads Init): Conteúdo de adsCustomer.auth:",
        JSON.stringify(adsCustomer.auth, null, 2)
      );
      if (adsCustomer.auth && adsCustomer.auth.credentials) {
        console.log(
          "DEBUG (Ads Init): adsCustomer.auth.credentials:",
          JSON.stringify(adsCustomer.auth.credentials, null, 2)
        );
      } else {
        console.warn(
          "WARN (Ads Init): Não foi possível logar adsCustomer.auth.credentials. auth ou credentials ausentes."
        );
      }
    } else {
      console.warn(
        "WARN: Uma ou mais variáveis de ambiente do Google Ads estão faltando ou são inválidas. Cliente não será inicializado."
      );
      adsApi = null;
      adsCustomer = null;
    }
  } catch (error) {
    console.error(
      "❌ CRITICAL ERROR (Ads Init): Erro durante a inicialização do cliente Google Ads:",
      error.message
    );
    console.error(
      "DEBUG (Ads Init): Stack trace do erro de inicialização:",
      error.stack
    );
    adsApi = null;
    adsCustomer = null;
  }
  console.log(
    "DEBUG: Finalizou a tentativa de inicialização do cliente Google Ads.\n"
  );

  // --- HubSpot Client Initialization ---
  console.log(
    "DEBUG: Tentando inicializar o cliente HubSpot DENTRO do pipeline..."
  );
  try {
    if (hubspotPrivateAppToken) {
      hubspotClient = new hubspot.Client({
        accessToken: hubspotPrivateAppToken,
      });
      console.log(
        "✅ DEBUG (HubSpot Init): Cliente HubSpot instanciado com sucesso."
      );
    } else {
      console.warn(
        "WARN: Variável de ambiente HUBSPOT_PRIVATE_APP_TOKEN está faltando. Cliente HubSpot não será inicializado."
      );
      hubspotClient = null;
    }
  } catch (error) {
    console.error(
      "❌ CRITICAL ERROR (HubSpot Init): Erro durante a inicialização do cliente HubSpot:",
      error.message
    );
    console.error(
      "DEBUG (HubSpot Init): Stack trace do erro de inicialização:",
      error.stack
    );
    hubspotClient = null;
  }
  console.log(
    "DEBUG: Finalizou a tentativa de inicialização do cliente HubSpot.\n"
  );

  let allCampaignsFromAds = [];

  // --- Fetch Google Ads Campaigns ---
  if (currentDataTypes.includes("googleAds") && adsCustomer) {
    console.log(
      "INFO: Chamando fetchCampaigns para obter dados do Google Ads..."
    );
    try {
      allCampaignsFromAds = await fetchCampaigns(
        adsCustomer,
        currentStartDate,
        currentEndDate
      );
      console.log(
        `INFO: fetchCampaigns retornou ${allCampaignsFromAds.length} campanhas.`
      );

      if (allCampaignsFromAds.length > 0) {
        console.log(
          `INFO: Preparando para escrever dados de campanhas do Google Ads na aba "${currentSheetName}"...`
        );
        const sheetHeaders = [
          "Campaign ID",
          "Campaign Name",
          "Ad Network Type",
          "Cost (BRL)",
          "Clicks",
          "Impressions",
          "Conversions",
        ];
        const sheetData = allCampaignsFromAds.map((campaign) => [
          campaign.id,
          campaign.name,
          campaign.network,
          campaign.cost,
          campaign.clicks,
          campaign.impressions,
          campaign.conversions,
        ]);
        await writeToSheet(sheetData, currentSheetName, sheetHeaders);
        console.log("✅ Google Ads campaign data written to Google Sheets.");
      } else {
        console.log(
          "WARN: Nenhuma campanha Google Ads para escrever na planilha."
        );
      }
    } catch (error) {
      console.error(
        "❌ ERROR (executarPipeline - Google Ads): Erro ao processar dados do Google Ads:",
        error.message
      );
      console.error(
        "DEBUG (executarPipeline - Google Ads): Stack trace:",
        error.stack
      );
    }
  } else if (currentDataTypes.includes("googleAds")) {
    console.warn(
      'WARN: "googleAds" selecionado, mas Google Ads client não inicializado. Pulando a busca e escrita de campanhas.'
    );
  }

  console.log(
    "DEBUG (Ads Auth Check): Conteúdo de adsCustomer.auth APÓS fetchCampaigns:",
    JSON.stringify(adsCustomer.auth, null, 2)
  );

  // --- Fetch Google Ads User Conversion Data ---
  if (currentDataTypes.includes("userConversions") && adsCustomer) {
    console.log(
      "INFO: Chamando fetchUserConversionData para obter dados de conversão de usuários..."
    );
    try {
      const userConversionResults = await fetchUserConversionData(
        adsCustomer,
        currentStartDate,
        currentEndDate
      );
      console.log(
        `INFO: fetchUserConversionData retornou ${userConversionResults.length} registros.`
      );

      if (userConversionResults.length > 0) {
        console.log(
          `DEBUG (executarPipeline): Amostra de userConversionResults (primeiro):`,
          JSON.stringify(userConversionResults[0], null, 2)
        );
        const distinctDatesResults = new Set(
          userConversionResults.map((item) => item.date)
        );
        console.log(
          `DEBUG (executarPipeline): Datas distintas em userConversionResults: ${distinctDatesResults.size}`
        );
      }

      if (userConversionResults.length > 0) {
        console.log(
          `INFO: Preparando para escrever dados de conversão de usuários na aba "User Conversions"...`
        );
        const userConversionHeaders = [
          "Date",
          "Campaign ID",
          // 'Ad Group ID', // Se você removeu do fetchUserConversionData, remova daqui também
          "GCLID",
        ];
        const userConversionSheetData = userConversionResults.map((row) => [
          row.date,
          row.campaignId || "N/A",
          // row.adGroupId || 'N/A', // Se você removeu do fetchUserConversionData, remova daqui também
          row.gclid || "N/A",
        ]);

        console.log(
          `DEBUG (executarPipeline): Amostra de userConversionSheetData (primeira linha de dados):`,
          JSON.stringify(userConversionSheetData[0], null, 2)
        );
        console.log(
          `DEBUG (executarPipeline): Total de linhas de dados para userConversionSheetData: ${userConversionSheetData.length}`
        );

        await writeToSheet(
          userConversionSheetData,
          "User Conversions",
          userConversionHeaders
        );
        console.log(
          "✅ Google Ads user conversion data written to Google Sheets."
        );
      } else {
        console.log(
          "WARN: Nenhum dado de conversão de usuário para escrever na planilha."
        );
      }
    } catch (error) {
      console.error(
        "❌ ERROR (executarPipeline - User Conversions): Erro ao processar dados de conversão do usuário:",
        error.message
      );
      console.error(
        "DEBUG (executarPipeline - User Conversions): Stack trace:",
        error.stack
      );
      throw error;
    }
  } else if (currentDataTypes.includes("userConversions")) {
    console.warn(
      'WARN: "userConversions" selecionado, mas Google Ads client não inicializado. Pulando a busca e escrita de dados de conversão de usuários.'
    );
  }

  // --- NEW SECTION: Fetch HubSpot Form Data ---
if (currentDataTypes.includes("hubspotForms") && hubspotClient) {
  console.log(
    "INFO: Chamando fetchFormSubmissionData para obter dados de formulários específicos do HubSpot..."
  );
  try {
    const formSubmissionResults = await fetchFormSubmissionData(
      hubspotClient
    ); // Chame a nova função

    if (formSubmissionResults.length > 0) {
      console.log(
        'INFO: Preparando para escrever dados de envios de formulários do HubSpot na aba "Form Submissions"...'
      );

      // --- ATUALIZAÇÃO NECESSÁRIA AQUI ---
      // 1. Atualize os cabeçalhos das colunas
      const formHeaders = [
        "Contact ID",
        "Email",
        "Original Source",
        "Detalhes da Fonte Original",
        "Nome do Formulário",
        "GCLID",
        "Timestamp do Envio",
        "Record Source",
        "Número de Negócios Associados"
      ];

      // 2. Mapeie os dados para corresponder aos cabeçalhos
      const formSheetData = formSubmissionResults.map((record) => [
        record["Contact ID"],
        record["Email"],
        record["Original Source"],
        record["Detalhes da Fonte Original"],
        record["Nome do Formulário"],
        record["GCLID"],
        record["Timestamp do Envio"],
        record["Record Source"],
        record["Número de Negócios Associados"]
      ]);
      // --- FIM DA ATUALIZAÇÃO ---

      console.log(
        `DEBUG (executarPipeline): Amostra de formSheetData (primeira linha de dados):`,
        JSON.stringify(formSheetData[0], null, 2)
      );
      console.log(
        `DEBUG (executarPipeline): Total de linhas de dados para formSheetData: ${formSheetData.length}`
      );

      await writeToSheet(formSheetData, "Form Submissions", formHeaders); // Nome da aba pode ser "Form Submissions"
      console.log(
        "✅ Dados de envios de formulários do HubSpot escritos no Google Sheets."
      );
    } else {
      console.log(
        "WARN: Nenhum dado de envio de formulário do HubSpot para escrever na planilha."
      );
    }
  } catch (error) {
    console.error(
      "❌ ERROR (executarPipeline - HubSpot Forms): Erro ao processar dados de formulários do HubSpot:",
      error.message
    );
    console.error(
      "DEBUG (executarPipeline - HubSpot Forms): Stack trace:",
      error.stack
    );
    throw error;
  }
} else if (currentDataTypes.includes("hubspotForms")) {
  console.warn(
    'WARN: "hubspotForms" selecionado, mas HubSpot client não inicializado. Pulando a busca e escrita de dados de formulários.'
  );
}

console.log("INFO: Pipeline concluído.");
}

// Seu module.exports permanece inalterado como entry point para Vercel
// module.exports = async (req, res) => {
//     try {
//         await executarPipeline();
//         res.status(200).send('Pipeline executado com sucesso!');
//     } catch (error) {
//         console.error('❌ ERRO CRÍTICO NA EXECUÇÃO GERAL DO PIPELINE:', error.message, error.stack);
//         res.status(500).send('Erro na execução do pipeline.');
//     }
// };

// Seu module.exports permanece inalterado
module.exports = async (req, res) => {
  try {
    await executarPipeline();
    res.status(200).send("Pipeline executado com sucesso!");
  } catch (error) {
    console.error(
      "❌ ERRO CRÍTICO NA EXECUÇÃO GERAL DO PIPELINE:",
      error.message,
      error.stack
    );
    res.status(500).send("Erro na execução do pipeline.");
  }
};

// --- Vercel Serverless Function Handler ---
// This is the entry point for Vercel to execute your function.
// It wraps the main pipeline logic and sends an HTTP response.
