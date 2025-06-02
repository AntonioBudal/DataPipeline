// index.js - Vercel Serverless Function for HubSpot and Google Ads Data Processing

// Load environment variables from .env file


// Import necessary libraries
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');

// THIS IS THE CRITICAL LOG TO ADD/MOVE:

// Add similar logs for other Google Ads env vars

// Import local configuration and utility for writing to Google Sheets
const config = require('./config');
const { writeToSheet } = require('./sheetsWriter');
let adsCustomer,adsApi;

// --- Global Client Initializations ---
// These clients are initialized once when the function starts (cold start)
// and reused for subsequent invocations (warm start).

// Log initial HubSpot token status for debugging
console.log(`DEBUG: config.ads.refreshToken from loaded config: ${config.ads.refreshToken ? config.ads.refreshToken.substring(0, 10) + '...' : 'NOT LOADED'}`);
console.log(`DEBUG: HubSpot Token from config (initial load - first 5 chars): ${config.hubspot && config.hubspot.privateAppToken ? config.hubspot.privateAppToken.substring(0, 5) + '...' : 'NOT FOUND or config.hubspot is undefined'}`);

// --- Log de Variáveis de Ambiente Iniciais (Verificação) ---


// HubSpot Client Initialization
let hubspotClient;
try {
  const hubspotToken = process.env.HUBSPOT_PRIVATE_APP_TOKEN || (config.hubspot && config.hubspot.privateAppToken);
  if (!hubspotToken) {
    console.error('CRITICAL (HubSpot): HUBSPOT_PRIVATE_APP_TOKEN (ou config.hubspot.privateAppToken) não encontrado!');
    throw new Error('HubSpot Private App Token não fornecido.');
  }
  console.log(`INFO (HubSpot): Tentando inicializar cliente HubSpot com token (primeiros 5 chars): ${hubspotToken.substring(0, 5)}...`);
  hubspotClient = new Hubspot.Client({ accessToken: hubspotToken });
  console.log('✅ Cliente HubSpot inicializado com sucesso (tentativa).');

  // Verificações de disponibilidade da API HubSpot (indicativo de escopos do token)
  console.log(`DEBUG (HubSpot API Check): crm.contacts.basicApi.getPage IS ${hubspotClient.crm?.contacts?.basicApi?.getPage ? '' : 'NOT '}available.`);
  console.log(`DEBUG (HubSpot API Check): crm.deals.batchApi.read IS ${hubspotClient.crm?.deals?.batchApi?.read ? '' : 'NOT '}available.`);
  console.log(`DEBUG (HubSpot API Check): marketing.forms.v3.statisticsApi.getById IS ${hubspotClient.marketing?.forms?.v3?.statisticsApi?.getById ? '' : 'NOT '}available.`);
  // Adicione mais verificações conforme necessário
} catch (error) {
  console.error('❌ CRITICAL (HubSpot): Erro ao inicializar o cliente HubSpot:', error.message);
  console.error('DEBUG (HubSpot): Stack de erro da inicialização:', error.stack);
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

    if (adsClientId && adsClientSecret && adsDeveloperToken && adsRefreshToken && adsCustomerId) {
        adsApi = new GoogleAdsApi({
            client_id: adsClientId,
            client_secret: adsClientSecret,
            developer_token: adsDeveloperToken,
            refresh_token: adsRefreshToken
        });
        console.log('✅ GoogleAdsApi instanciada com sucesso.');

        // Initialize adsCustomer using adsApi
        adsCustomer = adsApi.Customer({
            customer_id: adsCustomerId,
            // login_customer_id: adsLoginCustomerId // Uncomment if using MCC
        });
        console.log(`✅ adsCustomer instanciado com sucesso para o ID: ${adsCustomerId}`);
    } else {
        console.warn('WARN: Uma ou mais variáveis de ambiente do Google Ads estão faltando. Cliente não inicializado.');
    }
} catch (error) {
    console.error('❌ CRITICAL: Erro ao inicializar o cliente do Google Ads:', error.message);
    console.error('DEBUG: Stack trace do erro de inicialização do Google Ads:', error.stack);
}

console.log("-----------------------------------------");

// Helper function for introducing delays (e.g., for rate limiting)
const delay = ms => new Promise(res => setTimeout(res, ms));

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
async function retryableCall(fn, args = [], retries = 3, baseBackoffMs = 2000) {
    for (let i = 0; i <= retries; i++) {
        try {
            return await fn(...args);
        } catch (e) {
            const code = e.code || (e.response && e.response.status);
            // Specific handling for 403 (Forbidden) indicating permission issues
            if (code === 403) {
                console.warn(`WARN: ${fn.name || 'Função'} sem escopo necessário ou permissão (403): ${e.message}`);
                // For 403, it's typically not retryable; it's a configuration error.
                throw new Error(`Permission denied (403) for ${fn.name || 'Function'}: ${e.message}`);
            }
            // Handle 429 (Too Many Requests) with exponential backoff
            if (code === 429 && i < retries) {
                const wait = baseBackoffMs * Math.pow(2, i);
                console.warn(`WARN: Rate limit em ${fn.name || 'Função'}, retry em ${wait}ms (tentativa ${i + 1}/${retries + 1})`);
                await delay(wait);
                continue; // Continue to the next retry
            }
            // Log and re-throw for other errors or after all retries
            console.error(`ERROR: ${fn.name || 'Função'} falhou após ${i + 1} tentativas: Status ${code || 'N/A'}, Mensagem: ${e.message}`);
            if (e.response && e.response.body) {
                console.error("Detalhes do erro (body):", typeof e.response.body === 'string' ? e.response.body : JSON.stringify(e.response.body, null, 2));
            }
            throw e; // Re-throw if not a 429 or after all retries
        }
    }
    return null; // Should ideally not be reached if an error is always thrown on failure
}

/**
 * Fetches Google Ads campaigns. Skips if Google Ads client is not initialized.
 * @returns {Promise<Array<Object>>} - An array of Google Ads campaign objects.
 */
// Function to format date to YYYY-MM-DD
function formatDate(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
}



async function fetchCampaigns() {
    console.log('DEBUG: Entrou em fetchCampaigns.');
    if (!adsCustomer) {
        console.warn('WARN (fetchCampaigns): Cliente Google Ads (adsCustomer) não configurado ou inicializado corretamente. Pulando busca de campanhas.');
        return [];
    }
    console.log('INFO (fetchCampaigns): adsCustomer está definido, prosseguindo.');
    // Log adicional para verificar o customer ID que a biblioteca PODE estar usando
    // A forma de acessar isso depende da implementação da biblioteca 'google-ads-api'
    if (adsCustomer.options) { // Exemplo, pode não ser 'options'
        console.log(`DEBUG (fetchCampaigns): adsCustomer.options (pode conter IDs): ${JSON.stringify(adsCustomer.options, null, 2)}`);
    } else {
        console.log(`DEBUG (fetchCampaigns): Não foi possível logar adsCustomer.options.`);
    }
    // Para ter certeza, vamos logar as variáveis de ambiente novamente aqui
    console.log(`DEBUG (fetchCampaigns immediate env): GOOGLE_ADS_CUSTOMER_ID: ${process.env.GOOGLE_ADS_CUSTOMER_ID}`);
    console.log(`DEBUG (fetchCampaigns immediate env): GOOGLE_ADS_LOGIN_CUSTOMER_ID: ${process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID}`);


    console.time('ads-fetch');
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - 30); // Últimos 30 dias
    const formattedStartDate = formatDate(startDate);
    const formattedEndDate = formatDate(endDate);

    console.log(`DEBUG (fetchCampaigns): Buscando campanhas Google Ads para o período: ${formattedStartDate} a ${formattedEndDate}`);
    let allCampaigns = [];

    try {
        console.log('DEBUG (fetchCampaigns): Tentando criar report stream para campanhas...');
        const stream = adsCustomer.reportStream({
            entity: 'campaign',
            attributes: ['campaign.name', 'segments.ad_network_type', 'campaign.id'], // Adicionado campaign.id para depuração
            metrics: ['metrics.cost_micros'],
            date_ranges: [{ start_date: formattedStartDate, end_date: formattedEndDate }],
            // Se você estiver usando um ID de MCC para `adsCustomer` e a biblioteca não lida com `login-customer-id` automaticamente
            // no construtor de Customer, você PODE precisar de um parâmetro aqui, mas é menos comum para reportStream.
            // Ex: login_customer_id: process.env.GOOGLE_ADS_LOGIN_CUSTOMER_ID (verifique a documentação da lib)
        });

        console.log('DEBUG (fetchCampaigns): Stream criado. Iniciando iteração...');
        for await (const row of stream) {
            const cost = Number(row.metrics.cost_micros) / 1e6;
            const network = row.segments.ad_network_type || 'Desconhecida';
            // console.log(`DEBUG (fetchCampaigns): Linha do stream: Campanha ${row.campaign.name} (ID: ${row.campaign.id}), Custo: ${cost}, Rede: ${network}`);
            allCampaigns.push({ name: row.campaign.name, id: row.campaign.id, network, cost });
        }
        console.log(`DEBUG (fetchCampaigns): Iteração do stream finalizada. ${allCampaigns.length} campanhas encontradas.`);
        return allCampaigns;

    } catch (error) {
        console.error('❌ ERROR (fetchCampaigns): Erro detalhado ao buscar campanhas do Google Ads:', JSON.stringify(error, null, 2));
        // O erro original era: { "errors": [ { "error_code": { "request_error": "INVALID_CUSTOMER_ID" }, "message": "Invalid customer ID 'undefined'." } ] ... }
        // Este log acima deve capturar essa estrutura se ela ocorrer.
        console.warn('WARN (fetchCampaigns): Houve um erro ao buscar campanhas do Google Ads. Mensagem:', error.message);
        if (error.errors && Array.isArray(error.errors)) {
            error.errors.forEach(errDetail => {
                if (errDetail.error_code && errDetail.error_code.request_error === "INVALID_CUSTOMER_ID") {
                    console.error("CRITICAL (fetchCampaigns): A API do Google Ads retornou 'INVALID_CUSTOMER_ID'. Verifique se o ID do cliente usado pela biblioteca é válido e não 'undefined'.");
                }
            });
        }
        return []; // Retorna array vazio em caso de erro
    } finally {
        console.log('DEBUG (fetchCampaigns): Bloco finally executado.');
        console.timeEnd('ads-fetch');
    }
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
    if (!hubspotClient || !formIds.length || !(hubspotClient.marketing.forms?.v3?.statisticsApi?.getById && typeof hubspotClient.marketing.forms.v3.statisticsApi.getById === 'function')) {
        console.warn('DEBUG: Cliente HubSpot ou Forms API de estatísticas indisponível ou não há formulários para buscar. Retornando mapa vazio.');
        return {}; // Return empty if API is not available (due to plan limitations or other issues)
    }

    const formStatsMap = {};
    const startParam = startDate.toISOString();
    const endParam = endDate.toISOString();

    console.log(`DEBUG: Buscando estatísticas para ${formIds.length} formulários entre ${startParam} e ${endParam}...`);

    // Create a promise for each form to fetch its statistics concurrently
    const formPromises = formIds.map(formId => {
        return retryableCall(
            hubspotClient.marketing.forms.v3.statisticsApi.getById.bind(hubspotClient.marketing.forms.v3.statisticsApi),
            [formId, startParam, endParam]
        )
        .then(stats => ({ formId, stats })) // On success, return formId and stats
        .catch(error => {
            // Handle errors for individual form stats fetches
            if (error.message.includes('403')) {
                console.warn(`WARN: Acesso negado (403) para estatísticas do formulário ${formId}. Isso pode ser uma limitação do plano ou escopo incorreto.`);
            } else {
                console.error(`ERRO ao buscar estatísticas para formulário ${formId}: ${error.message}`);
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
async function countContactsAndAssociatedDeals() {
    console.log('DEBUG: Entrou na função countContactsAndAssociatedDeals.');
    if (!hubspotClient) {
        console.warn('Cliente HubSpot não inicializado. Pulando busca de contatos e negócios.');
        // Return default empty data if HubSpot client is not available
        return {
            totalOpenHubSpotDeals: 0, totalClosedWonHubSpotDeals: 0, totalLostHubspotDeals: 0,
            contactsWithDeals: 0,
            contactsWithoutDeals: 0,
            dealCampaigns: {}, contactFormSubmissions: {}, contactToDealIdsMap: {}, dealDetailsMap: {}
        };
    }

    let totalOpenHubSpotDeals = 0;
    let totalClosedWonHubSpotDeals = 0;
    let totalLostHubspotDeals = 0;
    let contactsWithDeals = 0;
    let contactsWithoutDeals = 0;
    const dealCampaignsData = {}; // Map of dealId -> { campaignNames, dealstage }
    const contactFormSubmissions = {}; // Map of contactId -> { formId, formName }
    const contactToDealIdsMap = {}; // Map of contactId -> array of associated deal IDs
    const allDealIds = new Set(); // Set to collect all unique deal IDs
    const allEngagementIds = new Set(); // Set to collect all unique engagement IDs
    const allContactsFetched = []; // Array to store full contact objects for later processing

    let contactsAfter = undefined; // Cursor for pagination
    let processedContactsCount = 0;
    const CONTACT_BATCH_SIZE = 100; // HubSpot API max limit for contacts basicApi.getPage
    const MAX_CONTACTS_TO_PROCESS = 10000; // Safety limit to prevent runaway loops

    // Properties to fetch for contacts, including associations to deals and engagements
    // ADDED: 'original_source', 'original_source_data_1', 'original_source_data_2' for inferring forms
    const contactProperties = ['createdate', 'email', 'firstname', 'lastname', 'original_source', 'original_source_data_1', 'original_source_data_2'];
    const contactAssociations = ['deals', 'engagements'];

    let contactResponse = null; // Initialize contactResponse to null

    // Loop to fetch all contacts with pagination
    do {
        try {
            contactResponse = await retryableCall(
                hubspotClient.crm.contacts.basicApi.getPage.bind(hubspotClient.crm.contacts.basicApi),
                [
                    CONTACT_BATCH_SIZE, // limit
                    contactsAfter,      // after (pagination cursor)
                    contactProperties,  // properties to fetch
                    [],                 // propertiesWithHistory (not needed)
                    contactAssociations, // associations to fetch (deals and engagements)
                    false               // archived
                ]
            );

            // Log response details for debugging
            if (contactResponse && contactResponse.results && contactResponse.results.length > 0) {
                console.log(`DEBUG: Resposta da API de contatos (primeiro contato): ${JSON.stringify(contactResponse.results[0], null, 2).substring(0, 500)}...`);
            } else {
                console.log('DEBUG: Resposta da API de contatos vazia ou sem resultados.');
            }

        } catch (err) {
            // Log and handle definitive failure to fetch contacts
            console.error('DEBUG: Falha definitiva ao buscar contatos após retries. Configurando contactResponse para null e finalizando busca de contatos:', err.message);
            contactResponse = null; // Explicitly set to null on error to ensure subsequent checks work
        }

        // Exit loop if no response, no results, or empty results
        if (!contactResponse || !contactResponse.results || contactResponse.results.length === 0) {
            console.warn('WARN: Nenhuma resposta ou resultados da API de contatos do HubSpot na iteração atual. Finalizando busca de contatos.');
            contactsAfter = null; // Force loop exit
            break; // Explicitly break out of the do...while loop
        }

        // Process each fetched contact
        for (const contact of contactResponse.results) {
            processedContactsCount++;
            allContactsFetched.push(contact); // Store the full contact object for later processing

            const contactId = contact.id;

            // --- ADDED DEBUG LOG FOR ORIGINAL SOURCE PROPERTIES ---
            console.log(`DEBUG: Contact ${contactId} original_source: '${contact.properties.original_source}', original_source_data_1: '${contact.properties.original_source_data_1}', original_source_data_2: '${contact.properties.original_source_data_2}'`);
            // --- END ADDED DEBUG LOG ---

            // Process associated deals for the contact
            const associatedDealIds = contact.associations?.deals?.results?.map(d => d.id) || [];
            if (associatedDealIds.length > 0) {
                contactsWithDeals++; // Increment count of contacts with deals
                contactToDealIdsMap[contactId] = associatedDealIds; // Map contact to its deal IDs
                associatedDealIds.forEach(id => allDealIds.add(id)); // Add to global set of unique deal IDs
            } else {
                contactsWithoutDeals++; // Increment count of contacts without deals
                contactToDealIdsMap[contactId] = [];
            }

            // Collect all engagement IDs for batch fetching later
            const associatedEngagementIds = contact.associations?.engagements?.results?.map(e => e.id) || [];
            if (associatedEngagementIds.length > 0) {
                associatedEngagementIds.forEach(id => allEngagementIds.add(id));
            }
        }

        contactsAfter = contactResponse.paging?.next?.after; // Get cursor for the next page
        // Implement a safety limit to prevent infinite loops in case of unexpected pagination issues
        if (processedContactsCount >= MAX_CONTACTS_TO_PROCESS && contactsAfter) {
            console.warn(`WARN: Limite de ${MAX_CONTACTS_TO_PROCESS} contatos processados atingido. Parando iteração.`);
            contactsAfter = null; // Force loop exit
        }
        console.log(`DEBUG: Processados ${processedContactsCount} contatos. Próxima página de contatos: ${!!contactsAfter}`);
        await delay(500); // Small delay between pages to prevent rate limits
    } while (contactsAfter); // Continue looping while there are more pages

    console.log(`INFO: Total de ${processedContactsCount} contatos processados. Total de ${allDealIds.size} negócios únicos associados.`);
    console.log(`--- Contagem de Contatos ---`);
    console.log(`Contatos com negócios associados: ${contactsWithDeals}`);
    console.log(`Contatos sem negócios associados: ${contactsWithoutDeals}`);
    console.log(`--------------------------`);


    // --- Fetch Deal Details in Batches ---
    const uniqueDealIds = Array.from(allDealIds);
    const dealDetailsMap = {}; // Stores dealId -> { deal_object }
    if (uniqueDealIds.length > 0) {
        console.log(`DEBUG: Buscando detalhes de ${uniqueDealIds.length} negócios únicos em lotes...`);
        const BATCH_DEAL_READ_SIZE = 100;
        const dealProperties = ['dealstage', 'dealname', 'amount']; // Add any other deal properties you need
        const dealAssociations = ['marketing_campaign']; // Request associations to marketing campaigns

        for (let i = 0; i < uniqueDealIds.length; i += BATCH_DEAL_READ_SIZE) {
            const batchOfDealIds = uniqueDealIds.slice(i, i + BATCH_DEAL_READ_SIZE);
            const dealBatchRequest = {
                inputs: batchOfDealIds.map(id => ({ id })),
                properties: dealProperties,
                associations: dealAssociations,
                archived: false
            };
            try {
                // Fetch deal details in batches
                const dealDetailsResponse = await retryableCall(
                    hubspotClient.crm.deals.batchApi.read.bind(hubspotClient.crm.deals.batchApi),
                    [dealBatchRequest]
                );

                if (dealDetailsResponse && dealDetailsResponse.results) {
                    dealDetailsResponse.results.forEach(deal => {
                        dealDetailsMap[deal.id] = deal; // Store fetched deal details
                    });
                }
            } catch (error) {
                console.error(`ERRO ao ler lote de detalhes de negócios:`, error.message);
                // Continue processing other batches even if one fails
            }
        }
    }

    // --- Fetch Engagement Details in Batches (specifically for Form Submissions) ---
    const uniqueEngagementIds = Array.from(allEngagementIds);
    const engagementDetailsMap = {}; // Stores engagementId -> { engagement_object }

    // Conditionally attempt to fetch form submission details based on API availability
    if (uniqueEngagementIds.length > 0 && hubspotClient.crm.objects.form_submissions?.batchApi?.read && typeof hubspotClient.crm.objects.form_submissions.batchApi.read === 'function') {
        console.log(`DEBUG: Buscando detalhes de ${uniqueEngagementIds.length} engajamentos únicos em lotes...`);
        const BATCH_ENGAGEMENT_READ_SIZE = 100;
        // Properties relevant for forms on engagements: hs_engagement_type, hs_createdate, hs_form_id, hs_form_title
        const engagementProperties = ['hs_engagement_type', 'hs_createdate', 'hs_form_id', 'hs_form_title'];

        for (let i = 0; i < uniqueEngagementIds.length; i += BATCH_ENGAGEMENT_READ_SIZE) {
            const batchOfEngagementIds = uniqueEngagementIds.slice(i, i + BATCH_ENGAGEMENT_READ_SIZE);
            const engagementBatchRequest = {
                inputs: batchOfEngagementIds.map(id => ({ id })),
                properties: engagementProperties,
                archived: false
            };
            try {
                // Attempt to read form submission details in batches
                const engagementDetailsResponse = await retryableCall(
                    hubspotClient.crm.objects.form_submissions.batchApi.read.bind(hubspotClient.crm.objects.form_submissions.batchApi),
                    [engagementBatchRequest]
                );
                if (engagementDetailsResponse && engagementDetailsResponse.results) {
                    engagementDetailsResponse.results.forEach(eng => {
                        engagementDetailsMap[eng.id] = eng; // Store fetched engagement details
                    });
                }
            } catch (error) {
                console.error(`ERRO ao ler lote de detalhes de engajamentos (form_submissions):`, error.message);
                // Continue processing other batches even if one fails
            }
        }
    } else {
        // Log a warning if the API is not available (due to plan limitations)
        console.warn('WARN: HubSpot CRM Forms Submissions API (crm.objects.form_submissions.batchApi.read) não está disponível ou não há engajamentos para buscar. Não será possível coletar detalhes de envios de formulário.');
    }

    // --- Process Contacts to find their Latest Form Submission (prioritizing engagements, then original_source) ---
    // Iterate through all the contacts fetched and use the detailed engagement map
    for (const contact of allContactsFetched) {
        const contactId = contact.id;
        const associatedEngagementIds = contact.associations?.engagements?.results?.map(e => e.id) || [];
        let latestFormSubmission = null;

        // FIRST ATTEMPT: Try to get form submissions from engagements (most accurate)
        // This block only runs if hubspotClient.crm.objects.form_submissions.batchApi.read is available
        if (hubspotClient.crm.objects.form_submissions?.batchApi?.read && typeof hubspotClient.crm.objects.form_submissions.batchApi.read === 'function') {
            for (const engagementId of associatedEngagementIds) {
                const engagement = engagementDetailsMap[engagementId];
                if (engagement && engagement.properties.hs_engagement_type === 'FORM_SUBMISSION') {
                    const formId = engagement.properties.hs_form_id;
                    const formTitle = engagement.properties.hs_form_title;
                    const engagementCreateDate = new Date(engagement.properties.hs_createdate);

                    if (formId && formTitle) {
                        if (!latestFormSubmission || engagementCreateDate > new Date(latestFormSubmission.properties.hs_createdate)) {
                            latestFormSubmission = engagement;
                        }
                    }
                }
            }
        }

        // FALLBACK: If no form submission found via engagements OR if the API was unavailable,
        // try to infer from original_source properties
        if (!latestFormSubmission) {
            const originalSource = contact.properties.original_source;
            const originalSourceData1 = contact.properties.original_source_data_1;
            const originalSourceData2 = contact.properties.original_source_data_2;

            // Refined logic for inferring form name based on provided example
            if (originalSource === 'FORM' || originalSource === 'OFFLINE_FORM' || (originalSource === 'EMAIL' && originalSourceData1 && originalSourceData1.includes('form')) || originalSource === 'PAID_SEARCH') {
                let inferredFormName = 'Formulário (Origem Desconhecida)';
                let inferredFormId = null; // We might not get a direct ID from original_source

                if (originalSource === 'PAID_SEARCH' && originalSourceData1) {
                    // Example: "Internacional Ground Wheels Envio de Formulário #Form PRODUCT PAGE"
                    const match = originalSourceData1.match(/Envio de Formulário\s*(.*)/i); // Case-insensitive match for "Envio de Formulário"
                    if (match && match[1]) {
                        inferredFormName = match[1].trim(); // Extract and trim the form name
                    } else {
                        inferredFormName = `Formulário (Pesquisa Paga: ${originalSourceData1})`;
                    }
                } else if (originalSourceData1) {
                    const match = originalSourceData1.match(/Form Submission: (.*)/);
                    if (match && match[1]) {
                        inferredFormName = match[1];
                    } else {
                        inferredFormName = `Formulário (Origem: ${originalSourceData1})`;
                    }
                } else if (originalSourceData2) {
                    inferredFormName = `Formulário (Origem: ${originalSourceData2})`;
                }

                latestFormSubmission = {
                    properties: {
                        hs_engagement_type: 'FORM_SUBMISSION',
                        hs_form_id: inferredFormId, // Might be null
                        hs_form_title: inferredFormName,
                        hs_createdate: contact.properties.createdate // Use contact creation date as proxy
                    }
                };
                console.log(`DEBUG: Contato ${contactId} (${contact.properties.email || 'N/A'}) - Formulário inferido da origem: ${inferredFormName}`);
            }
        }

        if (latestFormSubmission) {
            contactFormSubmissions[contactId] = {
                formId: String(latestFormSubmission.properties.hs_form_id || 'inferred'), // Use 'inferred' if no direct ID
                formName: String(latestFormSubmission.properties.hs_form_title)
            };
        } else {
            contactFormSubmissions[contactId] = { formId: null, formName: 'Formulário Não Encontrado' };
        }
    }
    // DEBUG LOG: Show the populated contactFormSubmissions map
    console.log('DEBUG: Conteúdo de contactFormSubmissions após processamento:', JSON.stringify(contactFormSubmissions, null, 2).substring(0, 1000) + '...'); // Log first 1000 chars

    // --- Process Deals to count and identify associated campaigns ---
    for (const dealId in dealDetailsMap) {
        const deal = dealDetailsMap[dealId];
        const currentDealStage = deal.properties.dealstage;
        const associatedCampaignNames = [];

        // Check for associations to marketing campaigns
        if (deal.associations?.marketing_campaign?.results?.length) {
            for (const assoc of deal.associations.marketing_campaign.results) {
                try {
                    // Fetch campaign details only if the marketing campaigns API is accessible
                    if (hubspotClient.marketing.campaigns?.campaignsApi?.getById && typeof hubspotClient.marketing.campaigns.campaignsApi.getById === 'function') {
                        const camp = await retryableCall(
                            hubspotClient.marketing.campaigns.campaignsApi.getById.bind(hubspotClient.marketing.campaigns.campaignsApi),
                            [assoc.id]
                        );
                        if (camp && camp.name) {
                            associatedCampaignNames.push(camp.name);
                        }
                    } else {
                        // Log a warning if the API is not available due to plan limitations
                        console.warn(`WARN: HubSpot Marketing Campaigns API (campaignsApi.getById) is NOT accessible to fetch campaign name for ID ${assoc.id}. This is expected if your plan does not permit it.`);
                        associatedCampaignNames.push(`ID: ${assoc.id} (Nome Indisponível devido ao plano)`);
                    }
                } catch (campErr) {
                    // Log errors during campaign fetching (e.g., specific campaign not found)
                    console.warn(`WARN: Erro ao buscar campanha ${assoc.id}: ${campErr.message}. Este erro pode ser devido a restrições do plano.`);
                    associatedCampaignNames.push(`ID: ${assoc.id} (Erro ou Acesso Negado)`);
                }
            }
        }
        dealCampaignsData[dealId] = { campaignNames: associatedCampaignNames, dealstage: currentDealStage };

        // Count deals by stage using config for stage IDs
        if (currentDealStage === 'closedwon' || currentDealStage === config.hubspot.dealStageIdForClosedWon) {
            totalClosedWonHubSpotDeals++;
        } else if (currentDealStage === 'closedlost' || currentDealStage === config.hubspot.dealStageIdForClosedLost) {
            totalLostHubspotDeals++;
        } else {
            totalOpenHubSpotDeals++;
        }
    }

    return {
        totalOpenHubSpotDeals,
        totalClosedWonHubSpotDeals,
        totalLostHubspotDeals,
        contactsWithDeals,
        contactsWithoutDeals,
        dealCampaigns: dealCampaignsData,
        contactFormSubmissions: contactFormSubmissions,
        contactToDealIdsMap: contactToDealIdsMap,
        dealDetailsMap: dealDetailsMap
    };
}

/**
 * The main pipeline execution function for the Vercel serverless endpoint.
 * This function orchestrates fetching data from Google Ads and HubSpot,
 * processing it, and writing to Google Sheets.
 * @param {Object} req - The Vercel request object.
 * @param {Object} res - The Vercel response object.
 */
async function executarPipeline() {
    console.log('INFO: Executando pipeline...');

    // --- Environment Variable Check (your existing code) ---
    console.log('\n--- Verificação Inicial de Variáveis de Ambiente ---');
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

    // Log environment variables (your existing ENV_CHECK logic)
    console.log(`ENV_CHECK: HUBSPOT_PRIVATE_APP_TOKEN: ${hubspotPrivateAppToken ? 'LOADED (first 5: ' + hubspotPrivateAppToken.substring(0,5) + '...)' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_ADS_CLIENT_ID: ${googleAdsClientId ? 'LOADED' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_ADS_CLIENT_SECRET: ${googleAdsClientSecret ? 'LOADED (first 5: ' + googleAdsClientSecret.substring(0,5) + '...)' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_ADS_DEVELOPER_TOKEN: ${googleAdsDeveloperToken ? 'LOADED' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_ADS_REFRESH_TOKEN: ${googleAdsRefreshToken ? 'LOADED (first 5: ' + googleAdsRefreshToken.substring(0,5) + '...)' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_ADS_CUSTOMER_ID: ${googleAdsCustomerId || 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_ADS_LOGIN_CUSTOMER_ID: ${googleAdsLoginCustomerId || 'NOT SET'} (opcional, usado se GOOGLE_ADS_CUSTOMER_ID for uma MCC)`);
    console.log(`ENV_CHECK: GOOGLE_SHEETS_CLIENT_ID: ${googleSheetsClientId ? 'LOADED' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_SHEETS_CLIENT_SECRET: ${googleSheetsClientSecret ? 'LOADED (first 5: ' + googleSheetsClientSecret.substring(0,5) + '...)' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_SHEETS_REFRESH_TOKEN: ${googleSheetsRefreshToken ? 'LOADED (first 5: ' + googleSheetsRefreshToken.substring(0,5) + '...)' : 'NOT SET'}`);
    console.log(`ENV_CHECK: GOOGLE_SHEETS_ID: ${googleSheetsId ? 'LOADED (first 5: ' + googleSheetsId.substring(0,5) + '...)' : 'NOT SET'}`);
    console.log('----------------------------------------------------\n');


    // --- NEW/UPDATED GOOGLE ADS CLIENT INITIALIZATION WITH MORE DEBUGGING ---
    console.log('DEBUG: Tentando inicializar o cliente Google Ads...');
    try {
        if (googleAdsClientId && googleAdsClientSecret && googleAdsDeveloperToken && googleAdsRefreshToken && googleAdsCustomerId) {

            // --- EXTREMELY DETAILED LOGGING OF GOOGLE ADS VARIABLES ---
            console.log(`DEBUG (Ads Init): googleAdsClientId type: ${typeof googleAdsClientId}, value: '${googleAdsClientId}'`);
            console.log(`DEBUG (Ads Init): googleAdsClientSecret type: ${typeof googleAdsClientSecret}, value: '${googleAdsClientSecret.substring(0, 10)}...'`); // Partial log for security
            console.log(`DEBUG (Ads Init): googleAdsDeveloperToken type: ${typeof googleAdsDeveloperToken}, value: '${googleAdsDeveloperToken}'`);
            console.log(`DEBUG (Ads Init): googleAdsRefreshToken type: ${typeof googleAdsRefreshToken}, value: '${googleAdsRefreshToken.substring(0, 10)}...'`); // Partial log for security
            // console.log(`DEBUG (Ads Init): googleAdsRefreshToken FULL VALUE: '${googleAdsRefreshToken}'`); // UNCOMMENT TEMPORARILY FOR EXTREME DEBUG, REMOVE IMMEDIATELY AFTERWARDS
            console.log(`DEBUG (Ads Init): googleAdsCustomerId type: ${typeof googleAdsCustomerId}, value: '${googleAdsCustomerId}'`);
            // --- END EXTREMELY DETAILED LOGGING ---

            adsApi = new GoogleAdsApi({
                client_id: googleAdsClientId,
                client_secret: googleAdsClientSecret,
                developer_token: googleAdsDeveloperToken,
                refresh_token: googleAdsRefreshToken
            });
            console.log('✅ DEBUG (Ads Init): GoogleAdsApi instanciada com sucesso.');

            adsCustomer = adsApi.Customer({
                customer_id: googleAdsCustomerId,
                login_customer_id: googleAdsLoginCustomerId // Will be undefined if not set, which is fine
            });
            console.log(`✅ DEBUG (Ads Init): adsCustomer instanciado com sucesso para o ID: ${googleAdsCustomerId}.`);
            // Try to log adsCustomer.options if possible, though previous logs said it failed.
            try {
                console.log('DEBUG (Ads Init): adsCustomer options (if available):', JSON.stringify(adsCustomer.options, null, 2));
            } catch (optErr) {
                console.warn('WARN (Ads Init): Não foi possível logar adsCustomer.options detalhadamente:', optErr.message);
            }

        } else {
            console.warn('WARN: Uma ou mais variáveis de ambiente do Google Ads estão faltando ou são inválidas. Cliente não será inicializado.');
            // Ensure adsApi and adsCustomer are explicitly null if initialization fails
            adsApi = null;
            adsCustomer = null;
        }
    } catch (error) {
        console.error('❌ CRITICAL ERROR (Ads Init): Erro durante a inicialização do cliente Google Ads:', error.message);
        console.error('DEBUG (Ads Init): Stack trace do erro de inicialização:', error.stack);
        adsApi = null;
        adsCustomer = null; // Ensure they are null if there's an error during instantiation
    }
    console.log('DEBUG: Finalizou a tentativa de inicialização do cliente Google Ads.\n');

    // --- Rest of your pipeline (fetchCampaigns, countContactsAndAssociatedDeals, etc.) ---
    // Make sure fetchCampaigns (and any other function using adsApi/adsCustomer)
    // has a check like `if (adsCustomer)` before proceeding.

    if (adsCustomer) {
        console.log('DEBUG: Entrou em fetchCampaigns.');
        console.log('INFO (fetchCampaigns): adsCustomer está definido, prosseguindo.');

        // Your existing fetchCampaigns logic starts here
        const startDate = '2025-05-03'; // Example, replace with your actual dates
        const endDate = '2025-06-02';   // Example, replace with your actual dates

        console.log(`DEBUG (fetchCampaigns): Buscando campanhas Google Ads para o período: ${startDate} a ${endDate}`);
        try {
            console.log('DEBUG (fetchCampaigns): Tentando criar report stream para campanhas...');
            const campaignIterator = adsCustomer.reportStream({
                entity: 'campaign',
                attributes: ['campaign.id', 'campaign.name', 'metrics.cost_micros'],
                constraints: [`segments.date BETWEEN '${startDate}' AND '${endDate}'`],
            });
            console.log('DEBUG (fetchCampaigns): Stream criado. Iniciando iteração...');

            let totalCostMicros = 0;
            let campaignCount = 0;

            for await (const row of campaignIterator) {
                totalCostMicros += parseFloat(row.metrics.cost_micros);
                campaignCount++;
            }

            console.log(`INFO: Google Ads: ${campaignCount} campanhas com custos totais recuperadas.`);
            console.log(`INFO: Custo total Google Ads: ${totalCostMicros / 1000000} (em BRL)`); // Convert micros to currency

        } catch (error) {
            console.error(`❌ ERROR (fetchCampaigns): Erro detalhado ao buscar campanhas do Google Ads: ${JSON.stringify(error, null, 2)}`);
            console.warn(`WARN (fetchCampaigns): Houve um erro ao buscar campanhas do Google Ads. Mensagem: ${error.message}`);
            console.log('DEBUG (fetchCampaigns): Stack trace do erro Google Ads:', error.stack); // More detailed stack trace
        } finally {
            console.log('DEBUG (fetchCampaigns): Bloco finally executado.');
            // Optionally, log how long ads-fetch took, if you have a timing mechanism
            // console.log('ads-fetch: Xms'); // Placeholder
        }
    } else {
        console.warn('WARN: Google Ads client não inicializado. Pulando a busca de campanhas.');
    }

    // --- Continue with HubSpot and Google Sheets logic ---
    console.log('\nDEBUG: Entrou na função countContactsAndAssociatedDeals.');
    // ... (your existing countContactsAndAssociatedDeals, writeToSheet, etc. calls) ...

    // Example placeholder for calling the next function
    // await countContactsAndAssociatedDeals(hubspotClient, startDate, endDate);
    // await writeToSheet(googleSheetsClient, data);
}

// If your function is a Vercel serverless function handler:
module.exports = async (req, res) => {
    // You might want to wrap the entire pipeline in a try-catch for the handler
    try {
        await executarPipeline();
        res.status(200).send('Pipeline executado com sucesso!');
    } catch (error) {
        console.error('❌ ERRO CRÍTICO NA EXECUÇÃO DO PIPELINE:', error.message);
        res.status(500).send('Erro na execução do pipeline.');
    }
};
// --- Vercel Serverless Function Handler ---
// This is the entry point for Vercel to execute your function.
// It wraps the main pipeline logic and sends an HTTP response.
