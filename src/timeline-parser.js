// timeline-parser.js - Vercel Serverless Function para diagnosticar o acesso aos dados da timeline do HubSpot

// Carrega variáveis de ambiente do arquivo .env (se estiver em ambiente de desenvolvimento local)
require('dotenv').config();

// Importa a biblioteca do cliente HubSpot
const Hubspot = require('@hubspot/api-client');

// --- Inicialização do Cliente HubSpot ---
// O cliente é inicializado uma vez quando a função inicia (cold start)
// e reutilizado para invocações subsequentes (warm start).
let hubspotClient;
try {
    // Tenta obter o token da Private App das variáveis de ambiente
    const tokenForHubspot = process.env.HUBSPOT_PRIVATE_APP_TOKEN;
    if (!tokenForHubspot) {
        // Erro crítico se o token estiver faltando
        console.error('CRITICAL: HubSpot Private App Token está FALTANDO para timeline-parser!');
        throw new Error('HubSpot Private App Token está faltando.');
    }
    // Inicializa o cliente HubSpot com o token de acesso
    hubspotClient = new Hubspot.Client({ accessToken: tokenForHubspot });
    console.log('Cliente HubSpot inicializado para timeline-parser.');

    // --- Verificações de Disponibilidade de API (para diagnóstico) ---
    // Estas verificações confirmam se métodos de API específicos estão disponíveis na instância do cliente.
    console.log(`DEBUG: hubspotClient.crm.contacts.basicApi.getPage IS ${hubspotClient.crm.contacts?.basicApi?.getPage && typeof hubspotClient.crm.contacts.basicApi.getPage === 'function' ? '' : 'NOT '}available.`);
    console.log(`DEBUG: hubspotClient.crm.engagements.engagementsApi.get IS ${hubspotClient.crm.engagements?.engagementsApi?.get && typeof hubspotClient.crm.engagements.engagementsApi.get === 'function' ? '' : 'NOT '}available.`);
    console.log(`DEBUG: hubspotClient.crm.objects.form_submissions.batchApi.read IS ${hubspotClient.crm.objects.form_submissions?.batchApi?.read && typeof hubspotClient.crm.objects.form_submissions.batchApi.read === 'function' ? '' : 'NOT '}available.`);

} catch (error) {
    // Loga erro crítico se a inicialização do cliente HubSpot falhar
    console.error('Erro CRÍTICO ao inicializar o cliente HubSpot para timeline-parser:', error.message);
    hubspotClient = undefined; // Garante que o cliente seja undefined em caso de erro
}

// Função auxiliar para introduzir atrasos (ex: para limites de taxa)
const delay = ms => new Promise(res => setTimeout(res, ms));

/**
 * Tenta chamar uma função com retries e backoff exponencial para erros transitórios.
 * Lida com erros 403 Forbidden como problemas de permissão não-retryable.
 * @param {Function} fn - A função a ser chamada.
 * @param {Array} args - Argumentos a serem passados para a função.
 * @param {number} retries - Número máximo de retries.
 * @param {number} baseBackoffMs - Atraso base em milissegundos para o backoff exponencial.
 * @returns {Promise<any>} - O resultado da chamada da função.
 * @throws {Error} - Lança um erro se todos os retries falharem ou se um 403 for encontrado.
 */
async function retryableCall(fn, args = [], retries = 3, baseBackoffMs = 2000) {
    for (let i = 0; i <= retries; i++) {
        try {
            return await fn(...args);
        } catch (e) {
            const code = e.code || (e.response && e.response.status);
            // Tratamento específico para 403 (Forbidden) indicando problemas de permissão
            if (code === 403) {
                console.warn(`WARN: ${fn.name || 'Função'} sem escopo necessário ou permissão (403): ${e.message}`);
                // Para 403, geralmente não é retryable; é um erro de configuração.
                throw new Error(`Permissão negada (403) para ${fn.name || 'Função'}: ${e.message}`);
            }
            // Lida com 429 (Too Many Requests) com backoff exponencial
            if (code === 429 && i < retries) {
                const wait = baseBackoffMs * Math.pow(2, i);
                console.warn(`WARN: Limite de taxa em ${fn.name || 'Função'}, retry em ${wait}ms (tentativa ${i + 1}/${retries + 1})`);
                await delay(wait);
                continue; // Continua para a próxima tentativa
            }
            // Loga e relança para outros erros ou após todas as tentativas
            console.error(`ERROR: ${fn.name || 'Função'} falhou após ${i + 1} tentativas: Status ${code || 'N/A'}, Mensagem: ${e.message}`);
            if (e.response && e.response.body) {
                console.error("Detalhes do erro (body):", typeof e.response.body === 'string' ? e.response.body : JSON.stringify(e.response.body, null, 2));
            }
            throw e; // Relança se não for um 429 ou após todas as tentativas
        }
    }
    return null; // Não deve ser alcançado se um erro for sempre lançado em caso de falha
}

/**
 * Função principal para diagnosticar o acesso aos dados da timeline.
 * Busca contatos e tenta obter detalhes de seus engajamentos para ver o conteúdo da timeline.
 * @returns {Promise<Object>} - Um objeto com o resultado do diagnóstico.
 */
async function diagnoseTimelineData() {
    console.log('Iniciando diagnóstico da timeline do HubSpot.');
    if (!hubspotClient) {
        console.warn('Cliente HubSpot não inicializado. Pulando diagnóstico da timeline.');
        return { message: 'Cliente HubSpot não inicializado.' };
    }

    const contactsToFetch = 5; // Busca um pequeno número de contatos para diagnóstico
    // Propriedades básicas do contato, incluindo associações a engajamentos
    const contactProperties = ['createdate', 'email', 'firstname', 'lastname'];
    const contactAssociations = ['engagements'];

    let contactsResponse;
    try {
        // Tenta buscar uma página de contatos com suas associações de engajamento
        contactsResponse = await retryableCall(
            hubspotClient.crm.contacts.basicApi.getPage.bind(hubspotClient.crm.contacts.basicApi),
            [
                contactsToFetch,
                undefined, // Sem cursor 'after' para a primeira página
                contactProperties,
                [],
                contactAssociations,
                false // Não arquivados
            ]
        );
    } catch (error) {
        console.error('ERRO ao buscar contatos para diagnóstico da timeline:', error.message);
        return { message: 'Falha ao buscar contatos para diagnóstico da timeline.', error: error.message };
    }

    if (!contactsResponse || !contactsResponse.results || contactsResponse.results.length === 0) {
        console.log('Nenhum contato encontrado para diagnóstico da timeline.');
        return { message: 'Nenhum contato encontrado para diagnóstico da timeline.' };
    }

    const timelineData = [];

    // Itera sobre cada contato encontrado
    for (const contact of contactsResponse.results) {
        const contactId = contact.id;
        const contactEmail = contact.properties.email || 'N/A';
        console.log(`DEBUG: Processando contato ${contactId} (${contactEmail}) para dados da timeline.`);

        // Obtém os IDs dos engajamentos associados a este contato
        const associatedEngagementIds = contact.associations?.engagements?.results?.map(e => e.id) || [];

        if (associatedEngagementIds.length === 0) {
            console.log(`DEBUG: Contato ${contactId} não possui engajamentos associados.`);
            timelineData.push({ contactId, email: contactEmail, engagements: [] });
            continue;
        }

        const engagementsDetails = [];
        // Para cada ID de engajamento, tenta buscar os detalhes completos
        for (const engagementId of associatedEngagementIds) {
            try {
                // Esta é a parte chave: tentar obter os detalhes completos do engajamento.
                // A propriedade 'body' ou outras propriedades podem conter o texto da timeline.
                const engagement = await retryableCall(
                    hubspotClient.crm.engagements.engagementsApi.get.bind(hubspotClient.crm.engagements.engagementsApi),
                    [engagementId]
                );

                if (engagement) {
                    console.log(`DEBUG: Detalhes do engajamento ${engagementId} para contato ${contactId}:`);
                    console.log(`DEBUG:   Type: ${engagement.engagement.type}`); // Ex: CALL, EMAIL, FORM_SUBMISSION

                    // Loga o corpo do engajamento, se disponível
                    console.log(`DEBUG:   Body (se disponível): ${engagement.engagement.body ? engagement.engagement.body.substring(0, 500) + '...' : 'N/A'}`);
                    // Loga todas as propriedades do engajamento, se disponíveis
                    console.log(`DEBUG:   Properties (se disponível): ${engagement.properties ? JSON.stringify(engagement.properties).substring(0, 500) + '...' : 'N/A'}`);

                    let extractedFormName = null;
                    let extractedFormId = null;

                    // Tenta extrair informações de formulário se for uma submissão de formulário
                    if (engagement.engagement.type === 'FORM_SUBMISSION') {
                        // Prioriza propriedades diretas se disponíveis
                        extractedFormId = engagement.properties?.hs_form_id;
                        extractedFormName = engagement.properties?.hs_form_title;

                        // Se as propriedades diretas não estiverem disponíveis, tenta parsear do body
                        if (!extractedFormName && engagement.engagement.body) {
                            // Exemplo de parsing baseado no formato que você mencionou: "Envio de Formulário #Form PRODUCT PAGE"
                            const formBodyMatch = engagement.engagement.body.match(/Envio de Formulário\s*(.*)/i);
                            if (formBodyMatch && formBodyMatch[1]) {
                                extractedFormName = formBodyMatch[1].trim();
                                console.log(`DEBUG:     Formulário extraído do body: ${extractedFormName}`);
                            }
                        }
                        console.log(`DEBUG:     Form ID (extraído/disponível): ${extractedFormId || 'N/A'}`);
                        console.log(`DEBUG:     Form Title (extraído/disponível): ${extractedFormName || 'N/A'}`);
                    }

                    engagementsDetails.push({
                        id: engagement.engagement.id,
                        type: engagement.engagement.type,
                        body: engagement.engagement.body,
                        properties: engagement.properties,
                        inferredFormName: extractedFormName, // Adiciona o nome do formulário inferido
                        inferredFormId: extractedFormId // Adiciona o ID do formulário inferido
                    });
                } else {
                    console.warn(`WARN: Engajamento ${engagementId} para contato ${contactId} não encontrado ou acesso negado.`);
                }
            } catch (engError) {
                console.error(`ERRO ao buscar detalhes do engajamento ${engagementId} para contato ${contactId}: ${engError.message}`);
            }
            await delay(200); // Pequeno atraso para evitar limites de taxa
        }
        timelineData.push({ contactId, email: contactEmail, engagements: engagementsDetails });
    }

    console.log('Diagnóstico da timeline concluído.');
    // Retorna os dados para análise
    return { success: true, data: timelineData, message: 'Diagnóstico da timeline concluído. Verifique os logs para detalhes dos engajamentos.' };
}

// --- Handler da Função Serverless Vercel ---
// Este é o ponto de entrada para o Vercel executar sua função.
// Ele envolve a lógica principal do diagnóstico e envia uma resposta HTTP.
export default async function handler(req, res) {
    console.log('Vercel Function timeline-parser invocada via HTTP request.');
    try {
        const result = await diagnoseTimelineData(); // Executa a lógica de diagnóstico
        if (result.success) {
            // Se o diagnóstico foi bem-sucedido, envia uma resposta 200 OK
            res.status(200).json(result);
        } else {
            // Se o diagnóstico reportou um erro, envia um 500 Internal Server Error
            res.status(500).json(result);
        }
    } catch (error) {
        // Captura quaisquer erros inesperados que possam ocorrer fora do try/catch do diagnóstico
        console.error('Erro inesperado no handler do timeline-parser:', error);
        res.status(500).json({ success: false, message: 'Internal Server Error', error: error.message });
    }
}
