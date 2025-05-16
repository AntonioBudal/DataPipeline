// sheetsWriter.js
const { google } = require('googleapis');

const auth = new google.auth.OAuth2(
  process.env.GOOGLE_SHEETS_CLIENT_ID,
  process.env.GOOGLE_SHEETS_CLIENT_SECRET,
  'https://developers.google.com/oauthplayground' // URI de redirecionamento do OAuth Playground
);

auth.setCredentials({ refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN });

/**
 * Escreve dados em uma aba específica de uma Planilha Google.
 * A aba é limpa antes da escrita. Os dados são mapeados para os cabeçalhos fornecidos.
 * @param {Array<Object>} data Array de objetos, onde cada objeto representa uma linha.
 * @param {string} sheetName O nome da aba onde os dados serão escritos (ex: 'Sheet1').
 * @param {Array<string>} headers Array de strings representando os cabeçalhos das colunas. A ordem aqui define a ordem das colunas.
 */
async function writeToSheet(data, sheetName = 'Sheet1', headers = []) {
  // Verifica se 'data' é um array. Se for null ou undefined, trata como array vazio para consistência.
  const validData = Array.isArray(data) ? data : [];

  try {
    const sheets = google.sheets({ version: 'v4', auth });
    const spreadsheetId = process.env.GOOGLE_SHEETS_ID;

    // 1. Limpar a aba inteira antes de escrever novos dados
    // Isso garante que dados antigos sejam removidos, especialmente se o novo conjunto de dados for menor.
    console.log(`INFO: Limpando a aba "${sheetName}" antes de escrever...`);
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: sheetName, // Limpa a aba inteira
    });
    console.log(`INFO: Aba "${sheetName}" limpa.`);

    // Se não houver cabeçalhos e nem dados, não há nada para escrever.
    if (headers.length === 0 && validData.length === 0) {
      console.log(`INFO: Nenhum cabeçalho ou dado fornecido para a aba "${sheetName}". A aba permanecerá vazia.`);
      return;
    }

    // 2. Preparar os dados para escrita (valuesToWrite)
    let valuesToWrite = [];

    // Adicionar cabeçalhos se fornecidos
    if (headers.length > 0) {
      valuesToWrite.push(headers);
    } else if (validData.length > 0) {
      // Se não houver cabeçalhos explícitos, mas houver dados,
      // usar as chaves do primeiro objeto de dados como cabeçalhos.
      console.warn(`WARN: Escrevendo dados na aba "${sheetName}" sem cabeçalhos explícitos. Usando chaves do primeiro item de dados como cabeçalhos.`);
      const firstItemHeaders = Object.keys(validData[0]);
      valuesToWrite.push(firstItemHeaders);
      // Atualizar 'headers' para usar na lógica de mapeamento de linhas abaixo
      headers = firstItemHeaders;
    }

    // Mapear cada item de 'validData' para uma linha,
    // usando os 'headers' (explícitos ou derivados) para garantir a ordem correta das colunas.
    if (headers.length > 0) { // Prosseguir somente se tivermos cabeçalhos para mapear
      validData.forEach(item => {
        const row = headers.map(header => {
          const value = item[header];
          // Usar string vazia para valores undefined ou null para evitar erros na API
          return value !== undefined && value !== null ? value : '';
        });
        valuesToWrite.push(row);
      });
    }

    // Se valuesToWrite estiver vazio (ex: headers vazios e validData vazio)
    // ou contiver apenas uma linha de cabeçalhos vazia, não há dados significativos para atualizar.
    if (valuesToWrite.length === 0 || (valuesToWrite.length === 1 && valuesToWrite[0].length === 0)) {
      console.log(`INFO: Nenhum dado significativo para escrever na aba "${sheetName}" após o processamento. A aba pode estar vazia ou apenas com cabeçalhos (se fornecidos e não vazios).`);
      // Se valuesToWrite tiver apenas cabeçalhos (e eles não são vazios), o update abaixo tratará disso.
      // Se valuesToWrite está completamente vazio, não faz nada.
      if (valuesToWrite.length === 0) return;
    }

    // 3. Escrever os dados na planilha
    const range = `${sheetName}!A1`; // Começando na célula A1
    const valueInputOption = 'USER_ENTERED';
    const resource = {
      values: valuesToWrite,
    };

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption,
      resource,
    });

    const numDataRows = validData.length;
    const numHeaderRows = (valuesToWrite.length > 0 && valuesToWrite[0].length > 0 && headers.length > 0) ? 1 : 0;
    console.log(`✅ Planilha Google Sheets atualizada com sucesso na aba "${sheetName}" com ${numHeaderRows} linha(s) de cabeçalho e ${numDataRows} linha(s) de dados.`);

  } catch (error) {
    console.error(`❌ Erro ao atualizar a planilha Google Sheets na aba "${sheetName}":`, error.message);
    if (error.response && error.response.data && error.response.data.error) {
      console.error('DEBUG: Detalhes do erro da API do Google:', JSON.stringify(error.response.data.error, null, 2));
    } else if (error.errors) {
      console.error('DEBUG: Detalhes do erro da API do Google (array de erros):', JSON.stringify(error.errors, null, 2));
    }
    // Considerar se deve relançar o erro. Se esta função for usada em um pipeline maior,
    // pode ser útil não parar todo o pipeline por uma falha de escrita em uma aba.
    // throw error; // Comentado por padrão
  }
}

module.exports = { writeToSheet };