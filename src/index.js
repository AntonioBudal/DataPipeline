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

      // console.error(
      //   `ERROR: Call to ${apiCall.name || "API Call"} failed after ${
      //     i + 1
      //   } attempt(s). Status: ${
      //     statusCode || "N/A"
      //   }, Message: ${errorMessage.substring(0, 200)}...`
      // ); // Limita o log para evitar sobrecarga
      // console.error(`DEBUG: Full error details:`, error); // Descomente para depuração local

      // Only retry on specific status codes (e.g., 429 for rate limits, 5xx for server errors)
      if (
        statusCode &&
        (statusCode === 429 || (statusCode >= 500 && statusCode < 600))
      ) {
        if (i < retries) {
          const waitTime =
            initialDelayMs * Math.pow(2, i) + Math.random() * 100; // Exponential backoff with jitter
          // console.warn(
          //   `WARN: Retrying in ${waitTime / 1000} seconds... (Attempt ${
          //   i + 2
          //   }/${retries + 1})`
          // );
          await delay(waitTime);
        } else {
          // console.error(
          //   `ERROR: Max retries (${retries}) exceeded for API call.`
          // );
          throw error; // Re-throw if max retries reached
        }
      } else if ([400, 401, 403, 404].includes(statusCode)) {
        // For known client-side errors, don't retry, just throw immediately.
        // console.error(
        //   `ERROR: Non-retryable API error (Status ${statusCode}). Aborting retries.`
        // );
        throw error;
      } else {
        // For any other unexpected errors, retry up to max retries
        if (i < retries) {
          const waitTime =
            initialDelayMs * Math.pow(2, i) + Math.random() * 100;
          // console.warn(
          //   `WARN: Retrying in ${
          //   waitTime / 1000
          //   } seconds due to unknown error. (Attempt ${i + 2}/${retries + 1})`
          // );
          await delay(waitTime);
        } else {
          // console.error(
          //   `ERROR: Max retries (${retries}) exceeded for API call due to unexpected error.`
          // );
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
 * Fetches contacts properties from HubSpot.
 * This version is simplified to only fetch contact properties
 * and not deal associations, as requested.
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

  const formSubmissionRecords = [];
  const CONTACT_SEARCH_BATCH_SIZE = 100;

  // PROPRIEDADES DE CONTATO ESSENCIAIS (SEM AS PROPRIEDADES DE DEAL)
  const contactProperties = [
    "createdate",
    "email",
    "gclid",
    "hs_object_source_label", // Para verificar se é 'FORM'
    "num_associated_deals",
    "form_name", // Mantendo o nome do formulário
  ];

  // Define a data de início para HubSpot como 1º de janeiro do ano atual
  const today = new Date();
  const currentYear = today.getFullYear();
  const firstDayOfCurrentYear = new Date(currentYear, 0, 1); // Month is 0-indexed (0 for January)

  const startDateFilterValue = firstDayOfCurrentYear.toISOString().split("T")[0];
  // const endDateFilterValue = today.toISOString().split("T")[0]; // Não necessário se você só busca a partir do ano atual

  console.log(`INFO: HubSpot Data Range (from ${startDateFilterValue} to today).`);

  let contactsSearchAfter = undefined; // Para paginação de contatos
  const allFetchedContacts = [];

  // --- FASE 1: BUSCAR TODOS OS CONTATOS COM AS PROPRIEDADES DIRETAS ---
  console.log("INFO: Iniciando busca de contatos com propriedades...");
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
              // Removendo o filtro de 'num_associated_deals > 0' se você quer todos os envios de formulário
              // Se você *só* quer envios de formulário que *têm* deals, mantenha este filtro.
              // Para alinhamento com "apenas propriedades", vou assumir que você ainda quer apenas os que têm deals.
              {
                propertyName: "num_associated_deals", 
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

      const searchResponse = await hubspotClient.crm.contacts.searchApi.doSearch(searchBody);

      if (searchResponse?.results?.length > 0) {
        allFetchedContacts.push(...searchResponse.results);
      }
      contactsSearchAfter = searchResponse.paging?.next?.after; // Atualiza o cursor para a próxima página
    } catch (error) {
      console.error(
        `ERROR: Erro geral ao buscar contatos (HubSpot): ${error.message.substring(0, 100)}...`
      );
      contactsSearchAfter = null; // Interrompe o loop em caso de erro crítico
    }
  } while (contactsSearchAfter);

  console.log(`INFO: Busca de contatos completa. Total de contatos: ${allFetchedContacts.length}`);

  // --- FASE 2: CONSTRUIR OS REGISTROS FINAIS COM OS DADOS COLETADOS ---
  console.log("INFO: Construindo registros finais para formulários...");
  for (const contact of allFetchedContacts) {
    const contactId = contact.id;
    const email = contact.properties.email || "N/A";
    const GCLID = contact.properties.gclid || "N/A";
    const formName = contact.properties.form_name || "N/A";
    const numAssociatedDeals = parseInt(
      contact.properties.num_associated_deals || "0",
      10
    );

    // Adiciona o registro formatado ao array final com as colunas solicitadas
    formSubmissionRecords.push({
      "Contact ID": contactId,
      Email: email,
      "Nome do Formulário": formName,
      GCLID: GCLID,
      "Número de Negócios Associados": numAssociatedDeals,
    });
  }

  console.log(
    `INFO: Total de ${formSubmissionRecords.length} form records (com >0 deals) encontrados.`
  );
  return formSubmissionRecords;
}

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
      where: "campaign.status = 'ENABLED'", // Filters for active campaigns
    });

    for await (const row of stream) {
      const cost = Number(row.metrics.cost_micros) / (1e6 * 100);
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
    //   `INFO: fetchCampaigns returned ${allCampaigns.length} campaigns.`
    // );
    return allCampaigns;
  } catch (error) {
    console.error(
      `ERROR: Failed to fetch Google Ads campaigns: ${error.message?.substring(0, 100)}...`
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

  while (currentDate <= endDateObj) {
    const singleDayFormatted = formatDate(currentDate);
    try {
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
      // console.log(
      //   `INFO: Accumulated ${allConversionData.length} conversion records after fetching day ${singleDayFormatted}.`
      // );
    } catch (error) {
      console.error(
        `ERROR: Failed to fetch conversion data for day ${singleDayFormatted}: ${error.message?.substring(0, 100)}...`
      );
      // console.error('DEBUG: Full error details (fetchUserConversionData):', JSON.stringify(error, Object.getOwnPropertyNames(error), 2)); // Descomente para depuração
    }
    currentDate.setDate(currentDate.getDate() + 1);
  }

  // console.log(
  //   `INFO: Total ${allConversionData.length} conversion records returned.`
  // );
  return allConversionData;
}

/**
 * The main pipeline execution function for the Vercel serverless endpoint.
 * This function orchestrates fetching data from Google Ads and HubSpot,
 * processing it, and writing to Google Sheets.
 */
async function executarPipeline() {
  console.log("INFO: Executing pipeline...");

  const currentSheetName = process.env.DEFAULT_SHEET_NAME || "Google Ads Campaigns";
  const endDate = new Date(); // e.g., Jun 6, 2025
  const currentEndDate = formatDate(endDate); // Will be today's date, e.g., '2025-06-06'

  // --- GLOBAL START DATE FOR GOOGLE ADS (MAX 90 DAYS BACK) ---
  const googleAdsStartDate = new Date();
  googleAdsStartDate.setDate(googleAdsStartDate.getDate() - 89); // 89 days back for safety
  const formattedGoogleAdsStartDate = formatDate(googleAdsStartDate);

  console.log(`INFO: Google Ads Data Range: ${formattedGoogleAdsStartDate} to ${currentEndDate}`);

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
      // Usando a data de início global para Google Ads
      fetchCampaigns(adsCustomer, formattedGoogleAdsStartDate, currentEndDate)
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
      // Usando a data de início global para Google Ads
      fetchUserConversionData(adsCustomer, formattedGoogleAdsStartDate, currentEndDate)
        .then((conversions) => {
          userConversionResults = conversions;
        })
        .catch((error) => {
          console.error(
            "❌ ERROR: Google Ads User Conversions fetch failed:",
            error.message?.substring(0, 100) ?? JSON.stringify(error).substring(0, 100) // Mais robusto para erro.message
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
      // fetchFormSubmissionData já tem sua própria lógica de datas interna (início do ano)
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
      await writeToSheet(sheetData, currentSheetName, sheetHeaders, ["Campaign ID"]);
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
        userConversionHeaders,
        ["Date", "Campaign ID", "GCLID"] // Combinar para unicidade
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
      await writeToSheet(formSheetData, "Form Submissions", formHeaders, ["Contact ID"]);
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