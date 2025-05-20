// sheetsWriter.js
const { google } = require('googleapis');

// Configuração da autenticação OAuth2
// Certifique-se de que as variáveis de ambiente estão definidas no seu ambiente Vercel
const auth = new google.auth.OAuth2(
  process.env.GOOGLE_SHEETS_CLIENT_ID,
  process.env.GOOGLE_SHEETS_CLIENT_SECRET,
  'https://developers.google.com/oauthplayground' // Este é um URI de redirecionamento comum para testes.
                                                 // Para produção, você pode ter um diferente ou nenhum se usar apenas o refresh token.
);

auth.setCredentials({ refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN });

/**
 * Escreve dados em uma aba específica de uma Planilha Google.
 * A aba é limpa (colunas A-ZZ) antes da escrita. Os dados são mapeados para os cabeçalhos fornecidos.
 * @param {Array<Object>} data Array de objetos, onde cada objeto representa uma linha.
 * @param {string} sheetName O nome da aba onde os dados serão escritos (ex: 'Sheet1').
 * @param {Array<string>} headers Array de strings representando os cabeçalhos das colunas. A ordem aqui define a ordem das colunas.
 */
async function writeToSheet(data, sheetName = 'Sheet1', headers = []) {
  // Garante que 'data' seja um array, mesmo que seja null ou undefined.
  const validData = Array.isArray(data) ? data : [];
  // Cria uma cópia dos cabeçalhos para evitar modificação do array original, se aplicável.
  let effectiveHeaders = Array.isArray(headers) ? [...headers] : [];

  try {
    const sheets = google.sheets({ version: 'v4', auth });
    const spreadsheetId = process.env.GOOGLE_SHEETS_ID;

    if (!spreadsheetId) {
        console.error('❌ Erro: GOOGLE_SHEETS_ID não está definido nas variáveis de ambiente.');
        return; // Interrompe a execução se o ID da planilha não estiver disponível
    }

    // 1. Limpar um range amplo da aba antes de escrever novos dados.
    // Isso garante que dados antigos sejam removidos. `${sheetName}!A:ZZ` limpa todas as linhas das colunas A a ZZ.
    // Ajuste 'ZZ' se você espera usar mais de 702 colunas.
    const rangeToClear = `${sheetName}!A:ZZ`;
    console.log(`INFO: Limpando o range "${rangeToClear}" na aba "${sheetName}" antes de escrever...`);
    try {
      await sheets.spreadsheets.values.clear({
        spreadsheetId,
        range: rangeToClear,
      });
      console.log(`INFO: Range "${rangeToClear}" na aba "${sheetName}" limpo.`);
    } catch (clearError) {
      // Loga o erro de limpeza, mas continua para tentar escrever os dados.
      // A aba pode estar vazia, ou pode haver outras razões para falha na limpeza.
      console.warn(`WARN: Falha ao limpar o range "${rangeToClear}" na aba "${sheetName}". Erro: ${clearError.message}. Tentando prosseguir com a escrita...`);
    }

    // Se não houver cabeçalhos explícitos E nem dados, não há nada para escrever.
    if (effectiveHeaders.length === 0 && validData.length === 0) {
      console.log(`INFO: Nenhum cabeçalho ou dado fornecido para a aba "${sheetName}". A aba permanecerá como está após a tentativa de limpeza.`);
      return;
    }

    // 2. Preparar os dados para escrita (valuesToWrite)
    let valuesToWrite = [];

    // Adicionar cabeçalhos se fornecidos explicitamente.
    // Se não houver cabeçalhos explícitos, mas houver dados, usar as chaves do primeiro objeto de dados.
    if (effectiveHeaders.length === 0 && validData.length > 0) {
      console.warn(`WARN: Escrevendo dados na aba "${sheetName}" sem cabeçalhos explícitos. Usando chaves do primeiro item de dados como cabeçalhos.`);
      effectiveHeaders = Object.keys(validData[0]);
    }

    // Adiciona a linha de cabeçalhos (se houver effectiveHeaders)
    if (effectiveHeaders.length > 0) {
      valuesToWrite.push(effectiveHeaders);
    }

    // Mapear cada item de 'validData' para uma linha,
    // usando os 'effectiveHeaders' para garantir a ordem correta das colunas.
    if (effectiveHeaders.length > 0) { // Prosseguir somente se tivermos cabeçalhos para mapear
      validData.forEach(item => {
        const row = effectiveHeaders.map(header => {
          const value = item[header];
          // Usar string vazia para valores undefined ou null para evitar erros na API
          return value !== undefined && value !== null ? String(value) : ''; // Garante que seja string
        });
        valuesToWrite.push(row);
      });
    } else if (validData.length > 0) {
        // Caso não haja cabeçalhos, mas haja dados (não objetos, talvez arrays de arrays?)
        // Esta lógica assume que 'data' é um array de objetos. Se for array de arrays, precisaria de ajuste.
        // Por ora, se não há effectiveHeaders, apenas os dados que já são arrays seriam adicionados.
        // No entanto, o código acima já tenta derivar effectiveHeaders se não forem passados.
        console.warn(`WARN: Tentando processar dados sem cabeçalhos efetivos para a aba "${sheetName}". Os dados podem não ser formatados corretamente.`);
        validData.forEach(rowArray => { // Assumindo que validData pode ser um array de arrays neste caso
            if(Array.isArray(rowArray)) {
                valuesToWrite.push(rowArray.map(value => value !== undefined && value !== null ? String(value) : ''));
            }
        });
    }


    // Se valuesToWrite estiver vazio ou contiver apenas uma linha de cabeçalhos vazia,
    // não há dados significativos para atualizar.
    if (valuesToWrite.length === 0) {
      console.log(`INFO: Nenhum dado para escrever na aba "${sheetName}" após o processamento.`);
      return;
    }
    if (valuesToWrite.length === 1 && effectiveHeaders.length > 0 && valuesToWrite[0].every(h => h === '')) {
      console.log(`INFO: Cabeçalhos processados estão vazios. Nada será escrito na aba "${sheetName}".`);
      return;
    }


    // 3. Escrever os dados na planilha
    const rangeToWrite = `${sheetName}!A1`; // Começando na célula A1
    const valueInputOption = 'USER_ENTERED';
    const resource = {
      values: valuesToWrite,
    };

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: rangeToWrite,
      valueInputOption,
      resource,
    });

    const numDataRows = validData.length;
    const numHeaderRows = (effectiveHeaders.length > 0 && valuesToWrite.length > 0) ? 1 : 0;
    console.log(`✅ Planilha Google Sheets atualizada com sucesso na aba "${sheetName}" com ${numHeaderRows} linha(s) de cabeçalho e ${numDataRows} linha(s) de dados.`);

  } catch (error) {
    console.error(`❌ Erro ao atualizar a planilha Google Sheets na aba "${sheetName}":`, error.message);
    if (error.response && error.response.data && error.response.data.error) {
      // Erro estruturado da API do Google
      console.error('DEBUG: Detalhes do erro da API do Google (objeto de erro):', JSON.stringify(error.response.data.error, null, 2));
    } else if (error.errors && Array.isArray(error.errors)) {
      // Às vezes, a API retorna um array de erros
      console.error('DEBUG: Detalhes do erro da API do Google (array de erros):', JSON.stringify(error.errors, null, 2));
    } else if (typeof error.stack !== 'undefined') {
        // Fallback para stacktrace se outros detalhes não estiverem disponíveis
        console.error('DEBUG: Stacktrace do erro:', error.stack);
    }
    // Comentado por padrão para não interromper um pipeline maior se for apenas uma falha de escrita.
    // throw error; 
  }
}

module.exports = { writeToSheet };