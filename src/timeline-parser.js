// timeline-parser.js - Vercel Serverless Function para extração de dados de formulários HubSpot e campanhas UTM

// Carrega variáveis de ambiente do arquivo .env (necessário para ambiente de desenvolvimento local)
require('dotenv').config();

// Importa a biblioteca oficial do cliente HubSpot
const Hubspot = require('@hubspot/api-client');

// --- CONFIGURAÇÃO DOS FORMULÁRIOS WIX A SEREM PROCESSADOS ---
// Substitua os placeholders 'SEU_FORM_GUID_...' pelos GUIDs reais dos seus formulários no HubSpot.
// O nome do formulário aqui será usado no resultado final.
const WIX_FORMS_TO_PROCESS = [
    { name: "#Form CONTACT US", guid: "87e92239-e21f-42fc-9295-29dac4d358b3" }, // Ex: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    { name: "#Form CATALOG", guid: "24aca4be-29d7-4aee-b30f-8d6e726204a6" },
    { name: "#Form STAY CONNECTED", guid: "93faa820-0d4e-48e7-985a-8d0761806fd9" },
    { name: "#Form DISTRIBUITOR (CONTACT PAGE)", guid: "5b6329b7-0a73-4158-b7f6-27342e7ef43f" },
    { name: "Aeroladder Ads", guid: "ac3acf3c-0592-46ea-bde5-12e396dace78" }, // Verifique se o nome e GUID estão corretos
    { name: "Forms Ads", guid: "5cd3c16d-6c4c-494e-a203-847f061436f2" },
    { name: "GET A QUOTE", guid: "b6cc344f-5020-437a-b5e8-b890833bede7" }, // Este é o que você recriou, pode já ser nativo.
    { name: "Forms Ads Home 2", guid: "cb22690-cf5a-4213-b6fd-a4053ac66a99" },
    { name: "HOME ADS", guid: "5cb22690-cf5a-4213-b6fd-a4053ac66a99" },
    { name: "#Form PEDINDO SER REVENDEDOR", guid: "213aa848-0bf8-45f0-a3e2-bab88e20cd30" },
    // Para os formulários com nomes longos como "#comp-m6jpyefp3...", use um nome amigável se desejar.
    // Se o FormsId na imagem (ex: "FormsId=a1b2c3d4-...") for o GUID, use-o.
    // Exemplo:
    // { name: "Wix Form Custom 1 (comp-m6jpyefp3)", guid: "a1b2c3d4-xxxx-xxxx-xxxx-xxxxxxxxxxxx" },
    // { name: "Wix Form Custom 2 (comp-m00z1va9)", guid: "e5f6g7h8-xxxx-xxxx-xxxx-xxxxxxxxxxxx" },
    { name: "#Form PRODUCT PAGE", guid: "29d269ac-c8fd-43fc-8fb8-79a150176722" }
];

// --- Inicialização do Cliente HubSpot ---
let hubspotClient;
try {
    const tokenForHubspot = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
    console.log(`DEBUG: HubSpot Token lido (primeiros 5 chars): ${tokenForHubspot ? tokenForHubspot.substring(0, 5) + '...' : 'NÃO ENCONTRADO'}`);
    if (!tokenForHubspot) {
        console.error('CRITICAL: HUBSPOT_PRIVATE_APP_TOKEN está FALTANDO. Verifique suas variáveis de ambiente.');
        throw new Error('HubSpot Private App Token está faltando.');
    }
    hubspotClient = new Hubspot.Client({ accessToken: tokenForHubspot });
    console.log('Cliente HubSpot inicializado com sucesso.');
} catch (error) {
    console.error('ERRO CRÍTICO ao inicializar o cliente HubSpot:', error.message);
    hubspotClient = undefined;
}

const delay = ms => new Promise(res => setTimeout(res, ms));

async function retryableCall(fn, args = [], retries = 3, baseBackoffMs = 2000) {
    for (let i = 0; i <= retries; i++) {
        try {
            return await fn(...args);
        } catch (e) {
            const code = e.code || (e.response && e.response.status);
            if (code === 403) {
                console.warn(`WARN: ${fn.name || 'Função'} sem escopo necessário ou permissão (403): ${e.message}`);
                throw new Error(`Permissão negada (403) para ${fn.name || 'Função'}: ${e.message}`);
            }
            if (code === 429 && i < retries) {
                const wait = baseBackoffMs * Math.pow(2, i);
                console.warn(`WARN: Limite de taxa em ${fn.name || 'Função'}, retry em ${wait}ms (tentativa ${i + 1}/${retries + 1})`);
                await delay(wait);
                continue;
            }
            console.error(`ERROR: ${fn.name || 'Função'} falhou após ${i + 1} tentativas: Status ${code || 'N/A'}, Mensagem: ${e.message}`);
            if (e.response && e.response.body) {
                console.error("Detalhes do erro (body):", typeof e.response.body === 'string' ? e.response.body : JSON.stringify(e.response.body, null, 2));
            }
            throw e;
        }
    }
    return null;
}

/**
 * Lista todos os formulários HubSpot NATIVOS existentes.
 * @returns {Promise<Array<Object>>} - Um array de objetos de formulário nativos.
 */
async function listNativeForms() {
    console.log('DEBUG: Listando formulários NATIVOS HubSpot...');
    const path = '/forms/v2/forms';
    try {
        const response = await retryableCall(
            hubspotClient.apiRequest.bind(hubspotClient),
            [{ method: 'GET', path: path }]
        );
        const jsonResponse = await response.json();
        if (Array.isArray(jsonResponse)) {
            console.log(`DEBUG: Encontrados ${jsonResponse.length} formulários NATIVOS.`);
            return jsonResponse;
        } else {
            console.warn(`WARN: Resposta inesperada ao listar formulários NATIVOS (não é um array):`, jsonResponse);
            return [];
        }
    } catch (error) {
        console.error(`ERRO ao listar formulários NATIVOS: ${error.message}`);
        return [];
    }
}

/**
 * Obtém todas as submissões para um formulário específico, com paginação.
 * @param {string} formGuid - O GUID do formulário.
 * @returns {Promise<Array<Object>>} - Um array de objetos de submissão.
 */
async function getFormSubmissions(formGuid) {
    console.log(`DEBUG: Buscando submissões para o formulário ${formGuid}...`);
    let allSubmissions = [];
    let offset = 0;
    const limit = 50;
    let hasMore = true;

    while (hasMore) {
        const path = `/form-integrations/v1/submissions/forms/${formGuid}`;
        const qs = { limit: limit, offset: offset };
        try {
            const response = await retryableCall(
                hubspotClient.apiRequest.bind(hubspotClient),
                [{ method: 'GET', path: path, qs: qs }]
            );
            const jsonResponse = await response.json();

            if (jsonResponse && jsonResponse.results && Array.isArray(jsonResponse.results)) {
                allSubmissions = allSubmissions.concat(jsonResponse.results);
                hasMore = (jsonResponse.hasMore !== undefined) ? jsonResponse.hasMore : (jsonResponse.results.length === limit);
                offset = jsonResponse.offset;
                if (jsonResponse.results.length === 0) hasMore = false;
                
                console.log(`DEBUG: Formulário ${formGuid}: Processadas ${allSubmissions.length} submissões. Mais páginas: ${hasMore}`);
                jsonResponse.results.forEach((sub, index) => {
                    // console.log(`DEBUG: Submissão ${index + 1} do formulário ${formGuid}: hs_context = ${JSON.stringify(sub.hs_context || {})}`);
                });
            } else {
                console.warn(`WARN: Resposta inesperada para submissões do formulário ${formGuid} (offset: ${offset}):`, jsonResponse);
                hasMore = false;
            }
        } catch (error) {
            console.error(`ERRO ao buscar submissões para o formulário ${formGuid} (offset: ${offset}): ${error.message}`);
            if (error.response && error.response.status === 404) {
                console.warn(`WARN: Formulário ${formGuid} não encontrado (404) ou sem submissões.`);
            }
            hasMore = false;
        }
        if (hasMore) await delay(200);
    }
    console.log(`DEBUG: Total de ${allSubmissions.length} submissões encontradas para o formulário ${formGuid}.`);
    return allSubmissions;
}

async function getContactByEmail(email) {
    if (!email) {
        console.warn('WARN: E-mail vazio fornecido para getContactByEmail.');
        return null;
    }
    const path = `/contacts/v1/contact/email/${encodeURIComponent(email)}/profile`;
    try {
        const response = await retryableCall(
            hubspotClient.apiRequest.bind(hubspotClient),
            [{ method: 'GET', path: path }]
        );
        const jsonResponse = await response.json();
        return jsonResponse && jsonResponse.vid ? jsonResponse.vid : null;
    } catch (error) {
        if (error.response && error.response.status === 404) {
            // console.log(`DEBUG: Contato não encontrado para o e-mail (404): ${email}`);
        } else {
            console.error(`ERRO ao buscar contato por e-mail ${email}: ${error.message}`);
        }
        return null;
    }
}

/**
 * Orquestra a busca e processamento de contatos para encontrar o primeiro formulário e campanha UTM.
 * @returns {Promise<Array<Object>>} - Um array de objetos de dados de contato estruturados.
 */
async function processContactsForFormAndCampaigns() {
    console.log('Iniciando processamento de contatos para formulários e campanhas.');
    if (!hubspotClient) {
        console.error('ERRO: Cliente HubSpot não está inicializado.');
        return [];
    }

    const allSubmissions = [];
    const uniqueSubmissionEmails = new Set();

    // 1. Processar os formulários Wix especificados
    console.log('--- Iniciando processamento de formulários WIX especificados ---');
    for (const wixForm of WIX_FORMS_TO_PROCESS) {
        if (!wixForm.guid || wixForm.guid === `SEU_FORM_GUID_${wixForm.name.toUpperCase().replace(/ /g, '_')}` || !wixForm.guid.includes('-')) {
             console.warn(`WARN: GUID inválido ou placeholder para o formulário Wix '${wixForm.name}'. Pulando... Substitua pelo GUID correto.`);
             continue;
        }
        console.log(`Processando formulário Wix: ${wixForm.name} (GUID: ${wixForm.guid})`);
        const formSubmissions = await getFormSubmissions(wixForm.guid);
        for (const sub of formSubmissions) {
            allSubmissions.push({ ...sub, formName: wixForm.name, formGuid: wixForm.guid }); // Adiciona nome amigável e GUID
            const email = sub.values.find(val => val.name === 'email')?.value;
            if (email) uniqueSubmissionEmails.add(email.toLowerCase());
        }
        await delay(500); // Atraso entre o processamento de formulários diferentes
    }
    console.log(`--- Processamento de formulários WIX especificados concluído. ${allSubmissions.length} submissões coletadas até agora. ---`);

    // 2. Opcional: Processar formulários nativos do HubSpot (se houver outros que você queira incluir)
    // Se você SÓ quer os formulários Wix da lista, pode comentar/remover este bloco.
    /*
    console.log('--- Iniciando processamento de formulários NATIVOS HubSpot ---');
    const nativeForms = await listNativeForms();
    for (const nativeForm of nativeForms) {
        // Evitar reprocessar se um GUID nativo coincidir com um da lista Wix (improvável, mas seguro)
        if (WIX_FORMS_TO_PROCESS.some(wf => wf.guid === nativeForm.guid)) {
            console.log(`DEBUG: Formulário nativo ${nativeForm.name} (GUID: ${nativeForm.guid}) já processado na lista Wix. Pulando.`);
            continue;
        }
        console.log(`Processando formulário nativo: ${nativeForm.name} (GUID: ${nativeForm.guid})`);
        const formSubmissions = await getFormSubmissions(nativeForm.guid);
        for (const sub of formSubmissions) {
            allSubmissions.push({ ...sub, formName: nativeForm.name, formGuid: nativeForm.guid });
            const email = sub.values.find(val => val.name === 'email')?.value;
            if (email) uniqueSubmissionEmails.add(email.toLowerCase());
        }
        await delay(500);
    }
    console.log(`--- Processamento de formulários NATIVOS concluído. Total de ${allSubmissions.length} submissões coletadas. ---`);
    */
    
    console.log(`DEBUG: Total final de ${allSubmissions.length} submissões coletadas de todos os formulários processados.`);
    console.log(`DEBUG: Total de ${uniqueSubmissionEmails.size} e-mails únicos de submissões.`);

    // Mapear e-mails para contactIds
    const emailToContactIdMap = new Map();
    let emailCounter = 0;
    for (const email of uniqueSubmissionEmails) {
        const contactId = await getContactByEmail(email);
        if (contactId) emailToContactIdMap.set(email.toLowerCase(), contactId);
        emailCounter++;
        if (emailCounter % 20 === 0) { // Loga a cada 20 e-mails
             console.log(`DEBUG: Mapeados ${emailToContactIdMap.size} e-mails para contactIds (${emailCounter}/${uniqueSubmissionEmails.size} e-mails verificados).`);
        }
        await delay(150); // Aumentar um pouco o delay aqui pode ser bom para muitos emails
    }
    console.log(`DEBUG: Mapeamento final de ${emailToContactIdMap.size} e-mails para contactIds.`);

    const processedContactsData = [];
    let contactsAfter = undefined;
    const CONTACT_BATCH_SIZE = 100;

    do {
        console.log(`DEBUG: Buscando lote de contatos (após: ${contactsAfter || 'início'})...`);
        let contactsResponse;
        try {
            contactsResponse = await retryableCall(
                hubspotClient.crm.contacts.basicApi.getPage.bind(hubspotClient.crm.contacts.basicApi),
                [
                    CONTACT_BATCH_SIZE,
                    contactsAfter,
                    ['email', 'firstname', 'lastname', 'createdate', 'hs_analytics_source', 'hs_analytics_source_data_1', 'hs_analytics_source_data_2'],
                    [],
                    [], // Removido 'engagements', 'deals' para simplificar e focar no objetivo primário
                    false
                ]
            );
        } catch (error) {
            console.error('ERRO ao buscar lote de contatos:', error.message);
            break;
        }

        if (!contactsResponse || !contactsResponse.results || contactsResponse.results.length === 0) {
            console.log('INFO: Nenhuma resposta ou resultados de contatos. Finalizando busca de contatos.');
            break;
        }

        for (const contact of contactsResponse.results) {
            const contactId = contact.id;
            const email = contact.properties.email;
            let firstFormName = null;
            let utmCampaign = null; // Será null se não houver UTMs históricos
            let earliestSubmittedAt = Infinity;

            // console.log(`INFO: Processando contato ${contactId} (${email || 'N/A'})...`);

            if (email) {
                const contactSubmissions = allSubmissions.filter(sub =>
                    sub.values.find(val => val.name === 'email')?.value?.toLowerCase() === email.toLowerCase()
                );

                if (contactSubmissions.length > 0) {
                    for (const submission of contactSubmissions) {
                        const submittedAtTimestamp = new Date(submission.submittedAt).getTime();
                        if (submittedAtTimestamp < earliestSubmittedAt) {
                            earliestSubmittedAt = submittedAtTimestamp;
                            firstFormName = submission.formName; // Nome amigável definido anteriormente
                            const hsContext = submission.hs_context || {};
                            utmCampaign = hsContext.utmCampaign || hsContext.utm_campaign || null; // Tenta ambos os casings
                            // Log para utmCampaign
                            // if (utmCampaign) {
                            //     console.log(`DEBUG: Contato ${contactId}, Email ${email}, Form ${firstFormName} (GUID ${submission.formGuid}), Subm ${new Date(earliestSubmittedAt).toISOString()}: utmCampaign='${utmCampaign}' encontrado.`);
                            // } else if (Object.keys(hsContext).length > 0) {
                            //      console.log(`DEBUG: Contato ${contactId}, Email ${email}, Form ${firstFormName} (GUID ${submission.formGuid}), Subm ${new Date(earliestSubmittedAt).toISOString()}: hs_context presente mas sem utmCampaign. Contexto: ${JSON.stringify(hsContext)}`);
                            // }
                        }
                    }
                }
            }
            
            // Se firstFormName ainda for null, mas o contato tem email e não achamos submissão, logar.
            // Ou se tem submissões mas nenhuma com email correspondente (pouco provável se uniqueSubmissionEmails veio de lá)
            // if (!firstFormName && email) {
            //     // console.log(`WARN: Contato ${contactId} (${email}) não teve formulário inicial encontrado nas submissões processadas.`);
            // }


            processedContactsData.push({
                contactId: Number(contactId),
                email: email || null,
                firstFormName: firstFormName || null,
                utmCampaign: utmCampaign || null, // Será null se não encontrado
                firstSubmittedAt: earliestSubmittedAt !== Infinity ? new Date(earliestSubmittedAt).toISOString() : null
            });
        }

        contactsAfter = contactsResponse.paging?.next?.after;
        await delay(500);
    } while (contactsAfter);

    console.log(`Processamento concluído. Total de ${processedContactsData.length} contatos processados e retornados.`);
    return processedContactsData;
}


// --- Vercel Serverless Function Handler ---
export default async function handler(req, res) {
    console.log('Vercel Function timeline-parser invocada.');
    // Adicionar verificação de segurança básica se necessário (ex: um token secreto no header)
    // if (req.headers['x-secret-token'] !== process.env.MY_VERCEL_PROTECTION_TOKEN) {
    //    console.warn('WARN: Tentativa de acesso não autorizado.');
    //    return res.status(401).json({ success: false, error: 'Unauthorized' });
    // }

    try {
        const resultData = await processContactsForFormAndCampaigns();
        res.status(200).json({ success: true, count: resultData.length, data: resultData });
    } catch (error) {
        console.error('❌ Erro no handler da Vercel Function:', error.message);
        console.error('Stack do erro:', error.stack);
        res.status(500).json({ success: false, error: error.message || 'Erro desconhecido.' });
    }
}