// sheetsWriter.js
const { google } = require('googleapis');

const auth = new google.auth.OAuth2(
  process.env.GOOGLE_SHEETS_CLIENT_ID,
  process.env.GOOGLE_SHEETS_CLIENT_SECRET,
  'https://developers.google.com/oauthplayground' // URI de redirecionamento do OAuth Playground
);

auth.setCredentials({ refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN });

async function writeToSheet(data, sheetName = 'Sheet1', headers = []) {
  try {
    const sheets = google.sheets('v4');
    const spreadsheetId = process.env.GOOGLE_SHEETS_ID;
    const range = `${sheetName}!A1`; // Especificando a aba e começando na célula A1
    const valueInputOption = 'USER_ENTERED';
    let values = [];

    if (headers && headers.length > 0) {
      values.push(headers); // Adiciona os cabeçalhos se fornecidos
      values.push(
        ...data.map(item => Object.values(item)) // Assume que a ordem das propriedades no objeto corresponde aos cabeçalhos
      );
    } else {
      values = data.map(item => Object.values(item)); // Escreve os dados sem cabeçalho
    }

    const resource = {
      values,
    };

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption,
      resource,
      auth,
    });
    console.log(`✅ Planilha do Google Sheets atualizada com sucesso na aba "${sheetName}".`);
  } catch (error) {
    console.error(`❌ Erro ao atualizar a planilha do Google Sheets na aba "${sheetName}":`, error);
    throw error; // Rejoga o erro para ser capturado no pipeline principal
  }
}

module.exports = { writeToSheet };