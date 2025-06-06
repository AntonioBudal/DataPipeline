// index.js - Vercel Serverless Function for HubSpot and Google Ads Data Processing

// Import necessary libraries
const { GoogleAdsApi } = require("google-ads-api");
const hubspot = require("@hubspot/api-client");
const config = require("./config"); // Certifique-se de que config.js está configurado corretamente
const { writeToSheet } = require("./sheetsWriter");

// --- Global Client Initializations (for Warm Starts) ---
// These clients are initialized once when the function starts (cold start)
// and reused for subsequent invocations (warm start).
let hubspotClient = null;
let adsApi = null;
let adsCustomer = null;

// Helper function for introducing delays (used ONLY by retryableCall)
const delay = (ms) => new Promise((res) => setTimeout(res, ms));

/**
 * Retries a function call with exponential backoff for transient errors (e.g., rate limits).
 * Handles 403 Forbidden errors as non-retryable permission issues.
 *
 * @param {Function} apiCall - The function to call.
 * @param {Array} args - Arguments to pass to the function.
 * @param {number} retries - Maximum number of retries.
 * @param {number} initialDelayMs - Base delay in milliseconds for exponential backoff.
 * @returns {Promise<any>} - The result of the function call.
 * @throws {Error} - Throws an error if all retries fail or if a non-retryable error (e.g., 400, 401, 403, 404) is encountered.
 */
async function retryableCall(apiCall, args, retries = 1, initialDelayMs = 50) {
  for (let i = 0; i <= retries; i++) {
    try {
      const result = await apiCall(...args);
      return result;
    } catch (error) {
      const statusCode =
        error.statusCode || (error.response && error.response.status);
      const errorMessage =
        error.message ||
        (error.response &&
          error.response.data &&
          error.response.data.message) ||
        JSON.stringify(error);

      console.error(
        `ERROR: Call to ${apiCall.name || "API Call"} failed after ${
          i + 1
        } attempt(s). Status: ${
          statusCode || "N/A"
        }, Message: ${errorMessage.substring(0, 200)}...`
      ); // Limita o log para evitar sobrecarga
      // console.error(`DEBUG: Full error details:`, error); // Descomente para depuração local

      // Only retry on specific status codes (e.g., 429 for rate limits, 5xx for server errors)
      if (
        statusCode &&
        (statusCode === 429 || (statusCode >= 500 && statusCode < 600))
      ) {
        if (i < retries) {
          const waitTime =
            initialDelayMs * Math.pow(2, i) + Math.random() * 100; // Exponential backoff with jitter
          console.warn(
            `WARN: Retrying in ${waitTime / 1000} seconds... (Attempt ${
              i + 2
            }/${retries + 1})`
          );
          await delay(waitTime);
        } else {
          console.error(
            `ERROR: Max retries (${retries}) exceeded for API call.`
          );
          throw error; // Re-throw if max retries reached
        }
      } else if ([400, 401, 403, 404].includes(statusCode)) {
        // For known client-side errors, don't retry, just throw immediately.
        console.error(
          `ERROR: Non-retryable API error (Status ${statusCode}). Aborting retries.`
        );
        throw error;
      } else {
        // For any other unexpected errors, retry up to max retries
        if (i < retries) {
          const waitTime =
            initialDelayMs * Math.pow(2, i) + Math.random() * 100;
          console.warn(
            `WARN: Retrying in ${
              waitTime / 1000
            } seconds due to unknown error. (Attempt ${i + 2}/${retries + 1})`
          );
          await delay(waitTime);
        } else {
          console.error(
            `ERROR: Max retries (${retries}) exceeded for API call due to unexpected error.`
          );
          throw error;
        }
      }
    }
  }
}

/**
 * Function to format date to YYYY-MM-DD
 */
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
 * Fetches contacts, associated deals, and engagements from HubSpot.
 * Adapts to plan limitations by conditionally attempting to fetch engagement details.
 * @param {hubspot.Client} hubspotClient - The initialized HubSpot API client.
 * @returns {Promise<Array<Object>>} - An array of formatted form submission records.
 */
async function fetchFormSubmissionData(hubspotClient) {
  if (!hubspotClient) {
    console.warn(
      "WARN: Cliente HubSpot não inicializado. Pulando busca de dados de formulário."
    );
    return [];
  }

  // Verificação crucial para a API V4 de associações
  if (!hubspotClient.crm?.associations?.v4?.basicApi?.getPage) {
    console.error(
      "CRITICAL ERROR: crm.associations.v4.basicApi.getPage IS NOT available. Check client initialization and SDK version."
    );
    return [];
  }

  const formSubmissionRecords = [];
  const CONTACT_SEARCH_BATCH_SIZE = 100;
  const DEAL_BATCH_READ_SIZE = 100;

  // PROPRIEDADES DE CONTATO REDUZIDAS PARA O ESSENCIAL
  const contactProperties = [
    "createdate",
    "email",
    "gclid",
    "hs_object_source_label", // Para verificar se é 'FORM'
    "num_associated_deals",
    "form_name", // Mantendo o nome do formulário
  ];

  // Filtra contatos criados nos últimos 30 dias
  // Get the current date
    const today = new Date();

    // Get the current year
    const currentYear = today.getFullYear();

    // Set the start date to January 1st of the current year
    const firstDayOfCurrentYear = new Date(currentYear, 0, 1); // Month is 0-indexed (0 for January)

    // Format the date as "YYYY-MM-DD"
    const startDateFilterValue = firstDayOfCurrentYear.toISOString().split("T")[0];

    // The endDateFilterValue will typically be today's date for "up to now" data
    const endDateFilterValue = today.toISOString().split("T")[0];

    console.log(`Filtering data from: ${startDateFilterValue} to ${endDateFilterValue}`);

  let contactsSearchAfter = undefined; // Para paginação de contatos

  // --- DECLARAÇÕES ÚNICAS: Variáveis que acumulam dados ao longo das fases ---
  const allFetchedContacts = [];
  const allDealIdsToFetchDetails = new Set();
  const contactToDealIdsMap = new Map();
  const dealDetailsMap = new Map(); // <--- DECLARAÇÃO ÚNICA E CORRETA AQUI!

  // --- FASE 1: BUSCAR TODOS OS CONTATOS COM DEALS E SUAS ASSOCIAÇÕES ---
  console.log("INFO: Fase 1: Iniciando busca de contatos com deals...");
  do {
    try {
      const searchBody = {
        filterGroups: [
          {
            filters: [
              {
                propertyName: "hs_object_source_label", // Fonte do objeto, 'FORM' para formulários
                operator: "EQ",
                value: "FORM",
              },
              {
                propertyName: "createdate", // Contatos criados a partir da data
                operator: "GTE",
                value: startDateFilterValue,
              },
              {
                propertyName: "num_associated_deals", // Contatos com pelo menos 1 negócio associado
                operator: "GT",
                value: "0",
              },
            ],
          },
        ],
        properties: contactProperties, // Usando propriedades reduzidas
        limit: CONTACT_SEARCH_BATCH_SIZE,
        after: contactsSearchAfter,
        sorts: [{ propertyName: "createdate", direction: "ASCENDING" }], // Ordena por data de criação
      };

      // Chamada direta à API do HubSpot
      const searchResponse = await hubspotClient.crm.contacts.searchApi.doSearch(searchBody);

      if (searchResponse?.results?.length > 0) {
        allFetchedContacts.push(...searchResponse.results); // Adiciona contatos ao array global

        // Coleta associações e IDs de deals para o batch atual (em paralelo para eficiência)
        await Promise.all(
          searchResponse.results.map(async (contact) => {
            try {
              // Chamada direta à API do HubSpot
              const associationsResponse = await hubspotClient.crm.associations.v4.basicApi.getPage(
                "contacts", contact.id, "deals" // Busca associações de contatos para deals
              );

              const dealIdsForCurrentContact = [];
              if (associationsResponse?.results) {
                associationsResponse.results.forEach((assoc) => {
                  dealIdsForCurrentContact.push(assoc.toObjectId);
                  allDealIdsToFetchDetails.add(assoc.toObjectId); // Adiciona ID do deal ao SET global
                });
              }
              // Mapeia IDs de deals para o contato atual
              contactToDealIdsMap.set(contact.id, dealIdsForCurrentContact);
            } catch (assocError) {
              console.error(
                `ERROR: Falha ao buscar associações de negócio (V4 API) para contato ${
                  contact.id
                }: ${assocError.message.substring(0, 100)}...`
              );
              contactToDealIdsMap.set(contact.id, []); // Garante que o mapa tenha uma entrada, mesmo que vazia
            }
          })
        );
      }
      contactsSearchAfter = searchResponse.paging?.next?.after; // Atualiza o cursor para a próxima página
    } catch (error) {
      console.error(
        `ERROR: Erro geral ao buscar contatos e dados de deals (Fase 1): ${error.message.substring(
          0,
          100
        )}...`
      );
      contactsSearchAfter = null; // Interrompe o loop em caso de erro crítico
    }
  } while (contactsSearchAfter);

//   console.log(
//     `INFO: Fase 1 completa. Total de contatos com deals: ${allFetchedContacts.length}`
//   );
//   console.log(
//     `INFO: Total de Deal IDs únicos para buscar detalhes: ${allDealIdsToFetchDetails.size}`
//   );

  // --- FASE 2: BUSCAR DETALHES DE TODOS OS DEALS ÚNICOS ---
  console.log("INFO: Fase 2: Iniciando busca de detalhes dos deals únicos...");
  const uniqueDealIdsArray = Array.from(allDealIdsToFetchDetails);

  if (uniqueDealIdsArray.length > 0) {
    for (let i = 0; i < uniqueDealIdsArray.length; i += DEAL_BATCH_READ_SIZE) {
      const batchOfDealIds = uniqueDealIdsArray.slice(
        i,
        i + DEAL_BATCH_READ_SIZE
      );
      const batchReadRequest = {
        inputs: batchOfDealIds.map((id) => ({ id })),
        properties: ["dealname", "hs_is_open_count", "hs_is_closed_won"], // Ainda precisamos de 'dealname' para o DEBUG no log
      };
      try {
        // Chamada direta à API do HubSpot
        const dealBatchResponse = await hubspotClient.crm.deals.batchApi.read(batchReadRequest);
        if (dealBatchResponse?.results) {
          dealBatchResponse.results.forEach((deal) => {
            dealDetailsMap.set(deal.id, {
              dealname: deal.properties.dealname || "Nome Indisponível", // Mantenha o nome para DEBUG se precisar
              isOpen: deal.properties.hs_is_open_count === "1", // HubSpot retorna '1' para true
              isClosedWon: deal.properties.hs_is_closed_won === "true", // HubSpot retorna 'true' para true
            });
          });
        }
      } catch (dealBatchError) {
        console.error(
          `ERROR: Falha ao buscar nomes/status de deals em lote (Fase 2): ${dealBatchError.message.substring(
            0,
            100
          )}...`
        );
        // Em caso de erro no lote, armazena um status de erro para cada deal do lote
        batchOfDealIds.forEach((id) =>
          dealDetailsMap.set(id, {
            dealname: "Erro ao Buscar Nome", // Mantém para consistência interna do objeto
            isOpen: false,
            isClosedWon: false,
          })
        );
      }
    }
  }
//   console.log(
//     `INFO: Fase 2 completa. Detalhes de ${dealDetailsMap.size} deals coletados.`
//   );

  // --- FASE 3: CONSTRUIR OS REGISTROS FINAIS COM TODOS OS DADOS JÁ COLETADOS ---
//   console.log("INFO: Fase 3: Construindo registros finais...");
  for (const contact of allFetchedContacts) {
    const contactId = contact.id;
    const email = contact.properties.email || "N/A";
    const formName = contact.properties.form_name || "N/A";
    const submissionTimestamp = contact.properties.createdate || "N/A";
    const numAssociatedDeals = parseInt(
      contact.properties.num_associated_deals || "0",
      10
    );

    let associatedDealStatusStr = "Nenhum Negócio Associado";
    const dealIdsForContact = contactToDealIdsMap.get(contact.id) || [];

    if (dealIdsForContact.length > 0) {
      const statuses = [];
      for (const dealId of dealIdsForContact) {
        const dealInfo = dealDetailsMap.get(dealId);
        let statusText = "";

        if (dealInfo) {
          // Lógica de Prioridade para Status: Fechado -> Aberto -> Indisponível
          if (!dealInfo.isOpen) { // Se o negócio NÃO está aberto, ele está fechado (ganho ou outro)
            if (dealInfo.isClosedWon) {
              statusText = `(Fechado Ganho)`; // REMOVIDO: ${dealInfo.dealname}
            } else {
              statusText = `(Fechado Outro)`; // REMOVIDO: ${dealInfo.dealname}
            }
          } else if (dealInfo.isOpen) { // Se não é fechado, e está aberto
            statusText = `(Aberto)`; // REMOVIDO: ${dealInfo.dealname}
          } else { // Fallback, embora raro se isOpen for true ou false
            statusText = `(Status Desconhecido)`; // REMOVIDO: ${dealInfo.dealname}
          }
        } else { // Se o dealInfo NÃO foi encontrado no mapa (problema de escopo ou deal excluído)
          statusText = `Deal ${dealId} (Status Indisponível)`; // Mantém o ID do deal para referência
        }
        statuses.push(statusText);
      }

      if (statuses.length > 0) {
        associatedDealStatusStr = statuses.join(" | ");
      } else {
        associatedDealStatusStr = "Status dos Negócios não puderam ser carregados";
      }
    }

    // Adiciona o registro formatado ao array final com colunas reduzidas
    formSubmissionRecords.push({
      "Contact ID": contactId,
      Email: email,
      gclid: GCLID,
      "Nome do Formulário": formName,
      "Timestamp do Envio": submissionTimestamp,
      "Número de Negócios Associados": numAssociatedDeals,
      "Nomes dos Negócios Associados": associatedDealStatusStr,
    });
  }

//   console.log(
//     `INFO: Total de ${formSubmissionRecords.length} form records (with >0 deals) encontrados.`
//   );
  return formSubmissionRecords;
}

module.exports = { fetchFormSubmissionData };
/**
 * Fetches Google Ads campaigns. Skips if Google Ads client is not initialized.
 * @param {GoogleAdsApi.Customer} adsCustomer - The initialized Google Ads customer client.
 * @param {string} startDate - Start date in YYYY-MM-DD format.
 * @param {string} endDate - End date in YYYY-MM-DD format.
 * @returns {Promise<Array<Object>>} - An array of Google Ads campaign objects.
 */
async function fetchCampaigns(adsCustomer, startDate, endDate) {
  if (!adsCustomer) {
    console.warn("WARN: adsCustomer not provided. Skipping campaign fetch.");
    return [];
  }

  


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
        "campaign.status", // <--- Add campaign status to attributes
      ],
      metrics: ["metrics.cost_micros"],
      date_ranges: [{ start_date: startDate, end_date: endDate }],
      // --- ADD THIS WHERE CLAUSE ---
      where: "campaign.status = 'ENABLED'", // Filters for active campaigns
      // -----------------------------
    });

    for await (const row of stream) {
      const cost = Number(row.metrics.cost_micros) / 1e6;
      const AD_NETWORK_TYPE_MAP = {
    1: "UNSPECIFIED", // Não especificado
    2: "Google Search", // Rede de Pesquisa do Google
    3: "Search Partners", // Parceiros de Pesquisa
    4: "Google Display Network (GDN)", // Rede de Display do Google
    5: "Youtube", // Pesquisa no YouTube
    6: "YouTube Videos", // Vídeos do YouTube
    7: "Cross-network (e.g., Performance Max)", // Rede Mista (ex: Performance Max)
    8: "UNKNOWN", // Desconhecido
    };

  const networkNumeric = row.segments.ad_network_type;
  const network = AD_NETWORK_TYPE_MAP[networkNumeric] || "UNKNOWN_VALUE_" + networkNumeric; // Fallback se o número não estiver no mapa
      allCampaigns.push({
        id: row.campaign.id,
        name: row.campaign.name,
        network: network,
        cost: Number(cost.toFixed(1)),
        clicks: Number(row.metrics.clicks),
        impressions: Number(row.metrics.impressions),
        conversions: Number(row.metrics.conversions),
        status: row.campaign.status, // <--- Add status to the pushed object
      });
    }
    // console.log(
    //   `INFO: fetchCampaigns returned ${allCampaigns.length} campaigns.`
    // );
    return allCampaigns;
  } catch (error) {
    console.error(
      `ERROR: Failed to fetch Google Ads campaigns: ${error.message.substring(
        0,
        100
      )}...`
    );
    // console.error(`DEBUG: Full error details (fetchCampaigns):`, JSON.stringify(error, null, 2)); // Uncomment for debugging
    return [];
  }
}

/**
 * Fetches Google Ads user conversion data.
 * @param {GoogleAdsApi.Customer} adsCustomer - The initialized Google Ads customer client.
 * @param {string} overallStartDate - Start date in YYYY-MM-DD format.
 * @param {string} overallEndDate - End date in YYYY-MM-DD format.
 * @returns {Promise<Array<Object>>} - An array of Google Ads user conversion records.
 */
async function fetchUserConversionData(
  adsCustomer,
  overallStartDate,
  overallEndDate
) {
  if (!adsCustomer) {
    console.warn("WARN: adsCustomer not provided for fetchUserConversionData.");
    return [];
  }

  const allConversionData = [];
  let currentDate = new Date(overallStartDate);
  const endDateObj = new Date(overallEndDate);

   currentDate.setHours(0, 0, 0, 0); 
  endDateObj.setHours(0, 0, 0, 0);

  while (currentDate <= endDateObj) {
    const singleDayFormatted = formatDate(currentDate);
    try {
      // No retryableCall wrapper here as per original code's explicit removal
      const stream = await adsCustomer.reportStream({
        entity: "click_view",
        attributes: ["segments.date", "campaign.id", "click_view.gclid"],
        constraints: [`segments.date = '${singleDayFormatted}'`],
      });

      for await (const row of stream) {
        allConversionData.push({
          date: row.segments.date,
          campaignId: row.campaign?.id || "N/A",
          gclid: row.click_view?.gclid || "N/A",
        });
      }
    //   console.log(
    //     `INFO: Accumulated ${allConversionData.length} conversion records after fetching day ${singleDayFormatted}.`
    //   );
    } catch (error) {
      console.error(
        `ERROR: Failed to fetch conversion data for day ${singleDayFormatted}: ${error.message.substring(
          0,
          100
        )}...`
      );
      // console.error('DEBUG: Full error details (fetchUserConversionData):', JSON.stringify(error, Object.getOwnPropertyNames(error), 2)); // Descomente para depuração
    }
    currentDate.setDate(currentDate.getDate() + 1);
  }

//   console.log(
//     `INFO: Total ${allConversionData.length} conversion records returned.`
//   );
  return allConversionData;
}

/**
 * The main pipeline execution function for the Vercel serverless endpoint.
 * This function orchestrates fetching data from Google Ads and HubSpot,
 * processing it, and writing to Google Sheets.
 */
async function executarPipeline() {
  console.log("INFO: Executing pipeline...");

    const currentSheetName =
    process.env.DEFAULT_SHEET_NAME || "Google Ads Campaigns";
    const endDate = new Date(); // e.g., Jun 6, 2025

    const currentYear = endDate.getFullYear(); // 2025

    const startDate = new Date(currentYear, 0, 1); // January 1st, 2025 (month 0 is January)

    const currentStartDate = formatDate(startDate); // Will be '2025-01-01'
    const currentEndDate = formatDate(endDate);     // Will be today's date, e.g., '2025-06-06'


    const currentDataTypes = process.env.DATA_TYPES
        ? process.env.DATA_TYPES.split(",")
        : ["googleAds", "userConversions", "hubspotForms"];

  // --- Client Initialization (Ensuring Warm Starts) ---
  // Only initialize if not already initialized from a previous warm invocation
  if (!hubspotClient) {
    try {
      const hubspotToken =
        process.env.HUBSPOT_PRIVATE_APP_TOKEN ||
        (config.hubspot && config.hubspot.privateAppToken);
      if (hubspotToken) {
        hubspotClient = new hubspot.Client({ accessToken: hubspotToken });
        console.log("✅ HubSpot Client initialized/reused.");
      } else {
        console.error("CRITICAL: HUBSPOT_PRIVATE_APP_TOKEN not found.");
      }
    } catch (error) {
      console.error(
        "❌ CRITICAL: Error initializing HubSpot client:",
        error.message
      );
    }
  }

  if (!adsCustomer) {
    try {
      const adsClientId = process.env.GOOGLE_ADS_CLIENT_ID;
      const adsClientSecret = process.env.GOOGLE_ADS_CLIENT_SECRET;
      const adsDeveloperToken = process.env.GOOGLE_ADS_DEVELOPER_TOKEN;
      const adsRefreshToken = process.env.GOOGLE_ADS_REFRESH_TOKEN;
      const adsCustomerId = process.env.GOOGLE_ADS_CUSTOMER_ID;
      const adsLoginCustomerId = process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID;

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
        });
        adsCustomer = adsApi.Customer({
          customer_id: adsCustomerId,
          login_customer_id: adsLoginCustomerId,
          refresh_token: adsRefreshToken,
        });
        // console.log(
        //   `✅ Google Ads Customer client initialized/reused for ID: ${adsCustomerId}`
        // );
      } else {
        console.warn(
          "WARN: One or more Google Ads environment variables are missing. Client not initialized."
        );
      }
    } catch (error) {
      console.error(
        "❌ CRITICAL: Error initializing Google Ads client:",
        error.message
      );
    }
  }

  // --- Parallel Data Fetching ---
  const dataFetchPromises = [];
  let allCampaignsFromAds = [];
  let userConversionResults = [];
  let formSubmissionRecords = [];

  if (currentDataTypes.includes("googleAds") && adsCustomer) {
    dataFetchPromises.push(
      fetchCampaigns(adsCustomer, currentStartDate, currentEndDate)
        .then((campaigns) => {
          allCampaignsFromAds = campaigns;
        })
        .catch((error) => {
          console.error(
            "❌ ERROR: Google Ads Campaigns fetch failed:",
            error.message
          );
          allCampaignsFromAds = [];
        })
    );
  } else if (currentDataTypes.includes("googleAds")) {
    console.warn(
      '"googleAds" selected, but Google Ads client not initialized. Skipping campaigns.'
    );
  }

  if (currentDataTypes.includes("userConversions") && adsCustomer) {
    dataFetchPromises.push(
      fetchUserConversionData(adsCustomer, currentStartDate, currentEndDate)
        .then((conversions) => {
          userConversionResults = conversions;
        })
        .catch((error) => {
          console.error(
            "❌ ERROR: Google Ads User Conversions fetch failed:",
            error.message
          );
          userConversionResults = [];
        })
    );
  } else if (currentDataTypes.includes("userConversions")) {
    console.warn(
      '"userConversions" selected, but Google Ads client not initialized. Skipping conversions.'
    );
  }

  if (currentDataTypes.includes("hubspotForms") && hubspotClient) {
    dataFetchPromises.push(
      fetchFormSubmissionData(hubspotClient)
        .then((forms) => {
          formSubmissionRecords = forms;
        })
        .catch((error) => {
          console.error("❌ ERROR: HubSpot Forms fetch failed:", error.message);
          formSubmissionRecords = [];
        })
    );
  } else if (currentDataTypes.includes("hubspotForms")) {
    console.warn(
      '"hubspotForms" selected, but HubSpot client not initialized. Skipping form data.'
    );
  }

  // Wait for all data fetches to complete (or settle)
  await Promise.allSettled(dataFetchPromises);

  // --- Write to Google Sheets (sequentially, as writing is a shared resource) ---

  // Google Ads Campaigns
  if (allCampaignsFromAds.length > 0) {
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
    try {
      await writeToSheet(sheetData, currentSheetName, sheetHeaders);
      console.log("✅ Google Ads campaign data written to Google Sheets.");
    } catch (error) {
      console.error(
        "❌ ERROR: Failed to write Google Ads campaign data to Sheets:",
        error.message
      );
    }
  } else {
    console.log("WARN: No Google Ads campaigns to write to sheet.");
  }

  // Google Ads User Conversions
  if (userConversionResults.length > 0) {
    const userConversionHeaders = ["Date", "Campaign ID", "GCLID"];
    const userConversionSheetData = userConversionResults.map((row) => [
      row.date,
      row.campaignId || "N/A",
      row.gclid || "N/A",
    ]);
    try {
      await writeToSheet(
        userConversionSheetData,
        "User Conversions",
        userConversionHeaders
      );
      console.log(
        "✅ Google Ads user conversion data written to Google Sheets."
      );
    } catch (error) {
      console.error(
        "❌ ERROR: Failed to write Google Ads user conversion data to Sheets:",
        error.message
      );
    }
  } else {
    console.log("WARN: No user conversion data to write to sheet.");
  }

  // HubSpot Form Submissions
  if (formSubmissionRecords.length > 0) {
    const formHeaders = [
      "Contact ID",
      "Email",
      "Nome do Formulário",
      "GCLID",
      "Número de Negócios Associados",
    ];
    const formSheetData = formSubmissionRecords.map((record) => [
      record["Contact ID"],
      record["Email"],
      record["Nome do Formulário"],
      record["GCLID"],
      record["Número de Negócios Associados"],
    ]);
    try {
      await writeToSheet(formSheetData, "Form Submissions", formHeaders);
      console.log("✅ HubSpot form submission data written to Google Sheets.");
    } catch (error) {
      console.error(
        "❌ ERROR: Failed to write HubSpot form data to Sheets:",
        error.message
      );
    }
  } else {
    console.log("WARN: No HubSpot form submission data to write to sheet.");
  }

  console.log("INFO: Pipeline completed.");
}

// --- Vercel Serverless Function Handler ---
// This is the entry point for Vercel to execute your function.
// It wraps the main pipeline logic and sends an HTTP response.
module.exports = async (req, res) => {
  try {
    await executarPipeline();
    res.status(200).send("Pipeline executed successfully!");
  } catch (error) {
    // Catch-all for any unhandled errors that propagate up
    console.error(
      "❌ CRITICAL ERROR IN MAIN PIPELINE EXECUTION:",
      error.message
    );
    console.error("DEBUG: Stack trace of critical error:", error.stack);
    res.status(500).send("Error during pipeline execution.");
  }
};
