// src/sheetsWriter.js
const config = require('./config');
const { google } = require('googleapis');

// Inicializa o cliente do Sheets **aqui**:
const sheetsAuth = new google.auth.OAuth2(
  config.sheets.clientId,
  config.sheets.clientSecret,
  'https://developers.google.com/oauthplayground'
);
sheetsAuth.setCredentials({ refresh_token: config.sheets.refreshToken });
const sheetsApi = google.sheets({ version: 'v4', auth: sheetsAuth });

// Agora a função só recebe o array de dados:
async function writeToSheet(data) {
  const header = ['Campanha','Rede','Custo','Negócios Abertos','Negócios Fechados'];
  const rows = [header, ...data.map(d => [d.name,d.network,d.cost,d.open,d.closed])];

  // sheetsApi é fechado sobre escopo, não precisa vir por parâmetro
  await sheetsApi.spreadsheets.values.update({
    spreadsheetId: config.sheets.spreadsheetId,
    range: 'A1',
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: rows },
  });
}

module.exports = { writeToSheet };
