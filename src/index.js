// index.js - Vercel Serverless Function for HubSpot and Google Ads Data Processing

// Load environment variables from .env file
require('dotenv').config();

// Import necessary libraries
const { GoogleAdsApi } = require('google-ads-api');
const Hubspot = require('@hubspot/api-client');

// Import local configuration and utility for writing to Google Sheets
const config = require('./config'); // Assumes a config.js file exists with API keys/tokens
const { writeToSheet } = require('./sheetsWriter'); // Assumes sheetsWriter.js handles Google Sheets API interaction

// --- Global Client Initializations ---
// These clients are initialized once when the function starts (cold start)
// and reused for subsequent invocations (warm start).

// Log initial HubSpot token status for debugging
console.log(`DEBUG: HubSpot Token from config (initial load - first 5 chars): ${config.hubspot && config.hubspot.privateAppToken ? config.hubspot.privateAppToken.substring(0, 5) + '...' : 'NOT FOUND or config.hubspot is undefined'}`);

// Google Ads Client Initialization
let adsApi, adsCustomer;
try {
    // Attempt to initialize Google Ads API client with credentials from config
    adsApi = new GoogleAdsApi({
        client_id: config.ads.clientId,
        client_secret: config.ads.clientSecret,
        developer_token: config.ads.developerToken,
    });
    // Attempt to create a customer instance for specific ad account operations
    adsCustomer = adsApi.Customer({
        customer_id: config.ads.customerId,
        refresh_token: config.ads.refreshToken,
    });
    console.log('Cliente Google Ads inicializado com sucesso (mas a permissão do token pode ser um problema).');
} catch (error) {
    // Log critical error if Google Ads client initialization fails
    console.error('ERRO CRÍTICO ao inicializar o cliente Google Ads. Isso pode ser devido a um token de desenvolvedor não aprovado para contas reais, ou outras configurações inválidas.');
    console.error('DEBUG: Google Ads Initialization Error (detailed):', error.message);
    adsApi = null; // Ensure clients are null on error to prevent further use
    adsCustomer = null;
}

// HubSpot Client Initialization
let hubspotClient;
try {
    const tokenForHubspot = config.hubspot && config.hubspot.privateAppToken;
    console.log(`DEBUG: Attempting to initialize HubSpot client. Token available (first 5 chars): ${tokenForHubspot ? tokenForHubspot.substring(0, 5) + '...' : 'NO TOKEN'}`);
    if (!tokenForHubspot) {
        // Throw error if HubSpot token is missing, preventing client initialization
        console.error('CRITICAL: HubSpot Private App Token is MISSING in config before client initialization!');
        throw new Error('HubSpot Private App Token is missing.');
    }
    // Initialize HubSpot API client with the provided access token
    hubspotClient = new Hubspot.Client({ accessToken: tokenForHubspot });
    console.log('Cliente HubSpot inicializado (tentativa).');

    // --- CRITICAL API AVAILABILITY CHECKS ---
    // These checks confirm if specific API methods are available on the client instance.
    // If a method logs 'NOT available', it indicates a missing scope/permission for your Private App Token.
    console.log(`DEBUG: hubspotClient.crm.contacts.basicApi.getPage IS ${hubspotClient.crm.contacts?.basicApi?.getPage && typeof hubspotClient.crm.contacts.basicApi.getPage === 'function' ? '' : 'NOT '}available.`);
    console.log(`DEBUG: hubspotClient.crm.deals.batchApi.read IS ${hubspotClient.crm.deals?.batchApi?.read && typeof hubspotClient.crm.deals.batchApi.read === 'function' ? '' : 'NOT '}available.`);
    console.log(`DEBUG: hubspotClient.marketing.forms.v3.statisticsApi.getById IS ${hubspotClient.marketing.forms?.v3?.statisticsApi?.getById && typeof hubspotClient.marketing.forms.v3.statisticsApi.getById === 'function' ? '' : 'NOT '}available.`);
    console.log(`DEBUG: hubspotClient.crm.objects.form_submissions.batchApi.read IS ${hubspotClient.crm.objects.form_submissions?.batchApi?.read && typeof hubspotClient.crm.objects.form_submissions.batchApi.read === 'function' ? '' : 'NOT '}available.`);
    console.log(`DEBUG: hubspotClient.marketing.campaigns.campaignsApi.getById IS ${hubspotClient.marketing.campaigns?.campaignsApi?.getById && typeof hubspotClient.marketing.campaigns.campaignsApi.getById === 'function' ? '' : 'NOT '}available.`);

} catch (error) {
    // Log critical error if HubSpot client initialization fails
    console.error('Erro CRÍTICO ao inicializar o cliente HubSpot:', error.message);
    console.error('DEBUG: HubSpot Initialization Error Stack:', error.stack);
    hubspotClient = undefined; // Ensure client is undefined on error to prevent further use
}

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
async function fetchCampaigns() {
    if (!adsCustomer) {
        console.log('INFO: Cliente Google Ads não configurado ou inicializado corretamente. Pulando busca de campanhas.');
        return [];
    }
    console.time('ads-fetch'); // Start timer for Google Ads fetch operation
    try {
        // Stream report for campaigns, attributes, and metrics
        const stream = adsCustomer.reportStream({
            entity: 'campaign',
            attributes: ['campaign.name', 'segments.ad_network_type'],
            metrics: ['metrics.cost_micros'],
            constraints: { 'campaign.status': ['ENABLED', 'PAUSED'], 'segments.date': 'DURING LAST_30_DAYS' },
        });
        const campaigns = [];
        // Iterate through the stream to collect campaign data
        for await (const row of stream) {
            const cost = Number(row.metrics.cost_micros) / 1e6; // Convert cost from micros to actual currency
            const network = row.segments.ad_network_type || 'Desconhecida';
            campaigns.push({ name: row.campaign.name, network, cost });
        }
        return campaigns;
    } catch (error) {
        // Warn if there's an error fetching Google Ads campaigns (e.g., due to token permissions)
        console.warn('WARN: Houve um erro ao buscar campanhas do Google Ads. Isso pode ser esperado se o token de desenvolvedor não tiver acesso total a contas reais.', error.message);
        return [];
    } finally {
        console.timeEnd('ads-fetch'); // End timer for Google Ads fetch operation
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
 * Main pipeline execution function. Orchestrates data fetching and processing.
 * This function is designed to be called by the Vercel serverless handler.
 */
async function executarPipeline() {
    console.log('🚀 Pipeline iniciado');
    try {
        const googleAdsCampaigns = await fetchCampaigns();

        const {
            totalOpenHubSpotDeals,
            totalClosedWonHubSpotDeals,
            totalLostHubspotDeals,
            contactsWithDeals,
            contactsWithoutDeals,
            dealCampaigns,
            contactFormSubmissions,
            contactToDealIdsMap,
            dealDetailsMap
        } = await countContactsAndAssociatedDeals();

        // --- ENHANCED DEBUG AND VALIDATION ---
        console.log(`DEBUG: After countContactsAndAssociatedDeals() call.`);
        console.log(`DEBUG: Type of contactToDealIdsMap: ${typeof contactToDealIdsMap}`);
        console.log(`DEBUG: Is contactToDealIdsMap null? ${contactToDealIdsMap === null}`);
        console.log(`DEBUG: Is contactToDealIdsMap undefined? ${contactToDealIdsMap === undefined}`);

        // Critical validation for contactToDealIdsMap
        if (typeof contactToDealIdsMap !== 'object' || contactToDealIdsMap === null) {
            console.error("CRITICAL ERROR: contactToDealIdsMap is NOT a valid object after countContactsAndAssociatedDeals() call. Value:", contactToDealIdsMap);
            throw new Error(`Pipeline initialization failed: contactToDealIdsMap is of unexpected type (${typeof contactToDealIdsMap}) or null.`);
        }
        console.log(`DEBUG: contactToDealIdsMap has ${Object.keys(contactToDealIdsMap).length} entries.`);
        // --- END ENHANCED DEBUG ---

        // Log new contact counts
        console.log(`📊 Contagem de Contatos HubSpot:`);
        console.log(`Contatos com Negócios Associados: ${contactsWithDeals}`);
        console.log(`Contatos sem Negócios Associados: ${contactsWithoutDeals}`);
        console.log(`---------------------------------`);

        console.log('📊 Contagem de negócios HubSpot (Totais):', { totalOpenHubSpotDeals, totalClosedWonHubSpotDeals, totalLostHubspotDeals });

        const formsAssociatedWithClosedDeals = new Set();
        const formNameToCounts = {}; // formName -> { open: X, closed: Y, id: formId }

        // Aggregate form data and associate with deals
        for (const contactId in contactToDealIdsMap) {
            const formInfo = contactFormSubmissions[contactId];
            // Skip if no valid form info (e.g., if form_submissions API was unavailable)
            if (!formInfo || !formInfo.formId) {
                continue;
            }

            const formName = formInfo.formName || 'Formulário Sem Nome'; // Provide a fallback name
            const formId = formInfo.formId; // Corrected: Use formInfo.formId

            if (!formNameToCounts[formName]) {
                formNameToCounts[formName] = { open: 0, closed: 0, id: formId };
            }

            const contactDeals = contactToDealIdsMap[contactId] || [];
            for (const dealId of contactDeals) {
                const deal = dealDetailsMap[dealId];
                if (deal) {
                    const currentDealStage = deal.properties.dealstage;
                    if (currentDealStage === 'closedwon' || currentDealStage === config.hubspot.dealStageIdForClosedWon) {
                        formNameToCounts[formName].closed++;
                        // Only add real form IDs to this set, as it's for fetching statistics
                        if (formId !== 'inferred' && formId !== null) {
                            formsAssociatedWithClosedDeals.add(formId);
                        }
                    } else if (currentDealStage !== 'closedlost' && currentDealStage !== config.hubspot.dealStageIdForClosedLost) {
                        formNameToCounts[formName].open++;
                    }
                }
            }
        }
        // DEBUG LOG: Show the populated formNameToCounts map
        console.log('DEBUG: Conteúdo de formNameToCounts antes de popular a planilha:', JSON.stringify(formNameToCounts, null, 2).substring(0, 1000) + '...'); // Log first 1000 chars


        console.log(`DEBUG: Total de ${formsAssociatedWithClosedDeals.size} formulários associados a negócios fechados para buscar estatísticas.`);

        const endDate = new Date();
        const startDate = new Date();
        startDate.setDate(endDate.getDate() - 30); // Last 30 days

        // Conditionally call fetchFormAnalytics based on API availability
        let formAnalytics = {};
        // Only attempt to fetch form analytics if the API is available AND there are actual form IDs to query
        if (hubspotClient.marketing.forms?.v3?.statisticsApi?.getById && typeof hubspotClient.marketing.forms.v3.statisticsApi.getById === 'function' && formsAssociatedWithClosedDeals.size > 0) {
            formAnalytics = await fetchFormAnalytics(Array.from(formsAssociatedWithClosedDeals), startDate, endDate);
            console.log(`DEBUG: Estatísticas de formulários obtidas para ${Object.keys(formAnalytics).length} formulários.`);
        } else {
            console.warn('WARN: HubSpot Forms Statistics API (marketing.forms.v3.statisticsApi.getById) não está disponível ou não há formulários reais para buscar estatísticas. As visualizações e envios dos formulários não serão coletados.');
        }

        // --- Prepare data for "Campanhas Ads" sheet ---
        const resultsForAdsSheet = [];
        const adsHeaders = ['Nome da Campanha', 'Rede', 'Custo Total no Período', 'Negócios Abertos', 'Negócios Fechados'];

        if (googleAdsCampaigns && googleAdsCampaigns.length > 0) {
            console.log(`ℹ️ Processando ${googleAdsCampaigns.length} campanhas do Google Ads...`);
            for (const adCampaign of googleAdsCampaigns) {
                let openCountForAdCampaign = 0;
                let closedWonCountForAdCampaign = 0;

                // Iterate over all deals to check their associated campaigns
                for (const dealId in dealDetailsMap) {
                    const deal = dealDetailsMap[dealId];
                    const currentDealStage = deal.properties.dealstage;

                    // Check if this deal was influenced by the current ad campaign
                    // This relies on the deal having 'marketing_campaign' associations
                    const associatedCampaignsOfDeal = dealCampaigns[dealId]?.campaignNames || [];
                    const isAssociatedToCurrentAdCampaign = associatedCampaignsOfDeal.includes(adCampaign.name);

                    if (isAssociatedToCurrentAdCampaign) {
                        if (currentDealStage === 'closedwon' || currentDealStage === config.hubspot.dealStageIdForClosedWon) {
                            closedWonCountForAdCampaign++;
                        } else if (currentDealStage !== 'closedlost' && currentDealStage !== config.hubspot.dealStageIdForClosedLost) {
                            openCountForAdCampaign++;
                        }
                    }
                }

                // Only add campaigns that had associated HubSpot deals (open or closed-won)
                if (openCountForAdCampaign > 0 || closedWonCountForAdCampaign > 0) {
                    resultsForAdsSheet.push({
                        'Nome da Campanha': adCampaign.name,
                        'Rede': adCampaign.network,
                        'Custo Total no Período': adCampaign.cost.toFixed(2),
                        'Negócios Abertos': openCountForAdCampaign,
                        'Negócios Fechados': closedWonCountForAdCampaign,
                    });
                } else {
                    console.debug(`DEBUG: Campanha Ads "${adCampaign.name}" não possui negócios HubSpot associados (abertos/ganhos). Não será incluída na aba "Campanhas Ads".`);
                }
            }
        }

        if (resultsForAdsSheet.length > 0) {
            await writeToSheet(resultsForAdsSheet, 'Campanhas Ads', adsHeaders);
            console.log(`✅ ${resultsForAdsSheet.length} campanha(s) Ads com negócios enviada(s) para "Campanhas Ads".`);
        } else {
            console.log('INFO: Nenhuma campanha Ads com negócios associados. Aba "Campanhas Ads" será limpa com cabeçalhos.');
            await writeToSheet([], 'Campanhas Ads', adsHeaders);
        }

        // --- Prepare data for "Dados de Formulários" sheet ---
        const formDataForSheet = [];
        const formHeaders = ['Nome do Formulário', 'Visualizações', 'Envios', 'Negócios Abertos', 'Negócios Fechados'];

        // Filter and prepare data for the sheet
        for (const formName in formNameToCounts) {
            const counts = formNameToCounts[formName];
            const formId = counts.id; // This will be the formId (real or 'inferred')

            // Include forms that have at least one associated open or closed deal
            // And exclude the generic "Formulário Não Encontrado" from being a separate row
            if (counts.closed > 0 || counts.open > 0) {
                if (formName === 'Formulário Não Encontrado') {
                    console.log(`INFO: O formulário genérico "Formulário Não Encontrado" possui negócios associados, mas não será incluído na aba "Dados de Formulários" como uma entrada separada.`);
                    continue; // Skip this entry for the sheet
                }

                const stats = formId && formId !== 'inferred' ? formAnalytics[formId] : null; // Get view/submission stats only for real form IDs

                formDataForSheet.push({
                    'Nome do Formulário': formName,
                    'Visualizações': stats?.views !== undefined ? stats.views : 'N/A (Plano)', // Indicate N/A if stats couldn't be fetched
                    'Envios': stats?.submissions !== undefined ? stats.submissions : 'N/A (Plano)',
                    'Negócios Abertos': counts.open,
                    'Negócios Fechados': counts.closed,
                });
            } else {
                console.log(`INFO: Formulário "${formName}" não possui negócios associados (abertos/ganhos). Não será incluído na aba "Dados de Formulários".`);
            }
        }

        if (formDataForSheet.length > 0) {
            await writeToSheet(formDataForSheet, 'Dados de Formulários', formHeaders);
            console.log(`✅ ${formDataForSheet.length} formulário(s) com negócios associados enviado(s) para "Dados de Formulários".`);
        } else {
            console.log('INFO: Nenhum formulário com negócios associados. Aba "Dados de Formulários" será limpa com cabeçalhos.');
            await writeToSheet([], 'Dados de Formulários', formHeaders);
        }

        console.log('✅ Pipeline concluído com sucesso!');
        return { success: true, message: 'Pipeline executed successfully.' }; // Return a success object
    } catch (error) {
        console.error('❌ Erro no pipeline:', error.message);
        console.error('Stack do erro:', error.stack);
        // Return an error object for the Vercel handler to process
        return { success: false, error: error.message || 'Unknown error during pipeline execution.' };
    }
}

// --- Vercel Serverless Function Handler ---
// This is the entry point for Vercel to execute your function.
// It wraps the main pipeline logic and sends an HTTP response.
export default async function handler(req, res) {
    console.log('Vercel Function invoked via HTTP request.');

    // You might want to add basic security checks here, e.g.,
    // if (req.method !== 'GET') {
    //     return res.status(405).json({ error: 'Method Not Allowed' });
    // }
    // if (req.headers.authorization !== `Bearer ${process.env.YOUR_API_KEY}`) {
    //     return res.status(401).json({ error: 'Unauthorized' });
    // }

    try {
        const result = await executarPipeline(); // Execute your main pipeline logic

        if (result.success) {
            // If the pipeline ran successfully, send a 200 OK response
            res.status(200).json({ status: 'success', message: result.message, data: result });
        } else {
            // If the pipeline reported an error, send a 500 Internal Server Error
            res.status(500).json({ status: 'error', message: 'Pipeline execution failed', details: result.error });
        }
    } catch (handlerError) {
        // Catch any unexpected errors that might occur outside of the pipeline's try/catch
        console.error('Unhandled error in Vercel handler:', handlerError);
        res.status(500).json({ status: 'error', message: 'Internal Server Error', details: handlerError.message || 'An unexpected error occurred.' });
    }
}
