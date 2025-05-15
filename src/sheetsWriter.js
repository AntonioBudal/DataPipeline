// sheetsWriter.js
const { google } = require('googleapis');

const auth = new google.auth.OAuth2(
  process.env.GOOGLE_SHEETS_CLIENT_ID,
  process.env.GOOGLE_SHEETS_CLIENT_SECRET,
  'https://developers.google.com/oauthplayground' // URI de redirecionamento do OAuth Playground
);

auth.setCredentials({ refresh_token: process.env.GOOGLE_SHEETS_REFRESH_TOKEN });

async function writeToSheet(data) {
  try {
    const sheets = google.sheets('v4');
    const spreadsheetId = process.env.GOOGLE_SHEETS_ID;
    const range = 'A1'; // Começar a escrever da célula A1
    const valueInputOption = 'USER_ENTERED';
    const values = [
      ['Nome da Campanha', 'Rede', 'Custo Total no Período', 'Negócios Abertos', 'Negócios Fechados'], // Cabeçalho
      ...data.map(item => [
        item.name,
        item.network,
        item.cost.toFixed(2),
        item.open,
        item.closed,
      ]),
    ];
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
    console.log('✅ Planilha do Google Sheets atualizada com sucesso.');
  } catch (error) {
    console.error('❌ Erro ao atualizar a planilha do Google Sheets:', error);
    throw error; // Rejoga o erro para ser capturado no pipeline principal
  }
}

module.exports = { writeToSheet };