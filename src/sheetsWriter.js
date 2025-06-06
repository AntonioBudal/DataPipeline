const { google } = require('googleapis');

// Inicialização da autenticação do Google Sheets
const auth = new google.auth.OAuth2(
    process.env.GOOGLE_SHEETS_CLIENT_ID,
    process.env.GOOGLE_SHEETS_CLIENT_SECRET,
    'https://developers.google.com/oauthplayground' // Ou a sua URI de redirecionamento configurada
);

// Define as credenciais (especificamente o refresh token)
// É crucial que GOOGLE_SHEETS_REFRESH_TOKEN esteja definido e seja válido.
if (process.env.GOOGLE_SHEETS_REFRESH_TOKEN) {
    auth.setCredentials({ refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN });
    console.log('INFO: Google Sheets refresh token carregado para autenticação.');
} else {
    console.error('CRITICAL ERROR: GOOGLE_SHEETS_REFRESH_TOKEN não está definido. A escrita na planilha falhará.');
}

/**
 * Escreve dados em uma aba específica de uma Planilha Google.
 * @param {Array<Array<any>>} data - Dados a serem escritos (array de linhas, onde cada linha é um array de células).
 * @param {string} sheetName - Nome da aba da planilha (ex: 'Sheet1').
 * @param {Array<string>} headers - Array opcional de cabeçalhos. Se fornecido, será a primeira linha.
 */
async function writeToSheet(data, sheetName = 'Sheet1', headers = []) {
    console.log(`DEBUG: writeToSheet chamado para aba: "${sheetName}"`);
    console.log(`DEBUG: writeToSheet - GOOGLE_SHEETS_CLIENT_ID: ${process.env.GOOGLE_SHEETS_CLIENT_ID ? 'LOADED' : 'MISSING'}`);
    console.log(`DEBUG: writeToSheet - GOOGLE_SHEETS_CLIENT_SECRET: ${process.env.GOOGLE_SHEETS_CLIENT_SECRET ? 'LOADED (first 5: ' + String(process.env.GOOGLE_SHEETS_CLIENT_SECRET).substring(0,5) + '...)' : 'MISSING'}`);
    console.log(`DEBUG: writeToSheet - GOOGLE_SHEETS_REFRESH_TOKEN: ${process.env.GOOGLE_SHEETS_REFRESH_TOKEN ? 'LOADED (first 5: ' + String(process.env.GOOGLE_SHEETS_REFRESH_TOKEN).substring(0,5) + '...)' : 'MISSING'}`);
    console.log(`DEBUG: writeToSheet - GOOGLE_SHEETS_ID: ${process.env.GOOGLE_SHEETS_ID ? 'LOADED (first 5: ' + String(process.env.GOOGLE_SHEETS_ID).substring(0,5) + '...)' : 'MISSING'}`);

    const spreadsheetId = process.env.GOOGLE_SHEETS_ID;
    if (!spreadsheetId) {
        console.error('❌ Erro Crítico: GOOGLE_SHEETS_ID não está definido. Não é possível escrever na planilha.');
        return;
    }

    if (!process.env.GOOGLE_SHEETS_REFRESH_TOKEN) {
        console.error('❌ Erro Crítico: GOOGLE_SHEETS_REFRESH_TOKEN não definido. Autenticação falhará.');
        return;
    }

    const validData = Array.isArray(data) ? data : [];
    let effectiveHeaders = Array.isArray(headers) ? [...headers] : [];

    try {
        const sheets = google.sheets({ version: 'v4', auth });
        console.log(`DEBUG: Instância google.sheets API V4 criada.`);

        // Tenta obter um novo token de acesso para verificar a validade do refresh token
        try {
            const tokenResponse = await auth.getAccessToken();
            if (tokenResponse && tokenResponse.token) {
                console.log('INFO: Google Sheets Access Token obtido/renovado com sucesso.');
            } else {
                console.warn('WARN: Falha ao obter/renovar o Google Sheets Access Token, mas tentando prosseguir. Resposta:', tokenResponse);
            }
        } catch (tokenError) {
            console.error('CRITICAL ERROR: Falha ao obter/renovar Google Sheets Access Token. Verifique GOOGLE_SHEETS_REFRESH_TOKEN e configuração OAuth. Erro:', tokenError.message);
            // throw tokenError; // Descomente esta linha para interromper em caso de falha na autenticação
        }

        const rangeToClear = `${sheetName}!A:ZZ`;
        console.log(`INFO: Limpando o range "${rangeToClear}" na aba "${sheetName}"...`);
        try {
            await sheets.spreadsheets.values.clear({
                spreadsheetId,
                range: rangeToClear,
            });
            console.log(`INFO: Range "${rangeToClear}" limpo com sucesso.`);
        } catch (clearError) {
            console.warn(`WARN: Falha ao limpar o range "${rangeToClear}". Erro: ${clearError.message}. Tentando prosseguir...`);
        }

        let valuesToWrite = [];

        if (effectiveHeaders.length > 0) {
            valuesToWrite.push(effectiveHeaders); // Adiciona os cabeçalhos primeiro
        }

        // Processa os dados, garantindo que o custo seja tratado como número
        validData.forEach(rowArray => {
            if (Array.isArray(rowArray)) {
                const processedRow = rowArray.map((value, index) => {
                    // Se for a coluna "Cost (BRL)" (índice 3), garanta que é um número com 2 casas decimais
                    // Os índices são baseados na ordem em sheetHeaders:
                    // "Campaign ID" (0), "Campaign Name" (1), "Ad Network Type" (2), "Cost (BRL)" (3), ...
                    if (index === 3) { // Coluna 'Cost (BRL)'
                        // Converte para número e arredonda para 2 casas decimais para evitar problemas de formatação
                        if (typeof value === 'number') {
                            return parseFloat(value.toFixed(2));
                        }
                        // Se não for um número, tenta parsear ou retorna 0
                        const parsedValue = parseFloat(value);
                        return isNaN(parsedValue) ? 0 : parseFloat(parsedValue.toFixed(2));
                    }
                    // Para outras colunas, ou se não for um número na coluna de custo, envie o valor como está
                    // ou uma string vazia se for null/undefined
                    return value !== undefined && value !== null ? value : '';
                });
                valuesToWrite.push(processedRow);
            } else if (typeof rowArray === 'object' && rowArray !== null) {
                // Este é um fallback para arrays de OBJETOS e headers foram dados, mantido do seu código original
                const row = effectiveHeaders.map(header => {
                    const value = rowArray[header];
                    // Para a coluna 'Cost (BRL)' (se o cabeçalho for "Cost (BRL)")
                    if (header === "Cost (BRL)") {
                        if (typeof value === 'number') {
                            return parseFloat(value.toFixed(2));
                        }
                        const parsedValue = parseFloat(value);
                        return isNaN(parsedValue) ? 0 : parseFloat(parsedValue.toFixed(2));
                    }
                    return value !== undefined && value !== null ? String(value) : '';
                });
                valuesToWrite.push(row);
            } else {
                console.warn(`WARN (sheetsWriter): Linha de dados inesperada pulada:`, rowArray);
            }
        });

        console.log(`DEBUG: Total de linhas a serem escritas (incluindo cabeçalho, se houver): ${valuesToWrite.length}`);
        if (valuesToWrite.length === 0) {
            console.log(`INFO: Nenhum dado para escrever na aba "${sheetName}" após processamento.`);
            return;
        }

        const rangeToWrite = `${sheetName}!A1`;
        const resource = { values: valuesToWrite };
        console.log(`INFO: Tentando escrever ${valuesToWrite.length} linhas na aba "${sheetName}" no range "${rangeToWrite}"...`);

        await sheets.spreadsheets.values.update({
            spreadsheetId,
            range: rangeToWrite,
            valueInputOption: 'RAW', // Importante: Envia os valores brutos sem interpretação automática
            resource,
        });

        console.log(`✅ Planilha Google Sheets atualizada com sucesso na aba "${sheetName}".`);

    } catch (error) {
        console.error(`❌ Erro ao atualizar a planilha Google Sheets na aba "${sheetName}":`, error.message);
        if (error.code === 403) {
            console.error('CRITICAL AUTH ERROR (Sheets): Código 403 - Verifique as permissões da API do Google Sheets para a conta de serviço/token OAuth. Pode não ter permissão de escrita na planilha ou o token de atualização é inválido/revogado.');
        }
        if (error.response && error.response.data && error.response.data.error) {
            console.error('DEBUG (Sheets): Detalhes do erro da API do Google (objeto de erro):', JSON.stringify(error.response.data.error, null, 2));
        } else if (error.errors && Array.isArray(error.errors)) {
            console.error('DEBUG (Sheets): Detalhes do erro da API do Google (array de erros):', JSON.stringify(error.errors, null, 2));
        }
        if (typeof error.stack !== 'undefined') {
            console.error('DEBUG (Sheets): Stacktrace do erro:', error.stack);
        }
    }
}

// Exporta a função para que possa ser importada em outros arquivos
module.exports = { writeToSheet };