// src/sheetsWriter.js
async function writeToSheet(data) {
  const header = ['Campanha', 'Rede', 'Custo', 'Negócios Abertos', 'Negócios Fechados'];
  const rows = [header, ...data.map(d => [d.name, d.network, d.cost, d.open, d.closed])];

  await sheetsApi.spreadsheets.values.update({
    spreadsheetId: config.sheets.spreadsheetId,
    range: 'A1',
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: rows },
  });
}

module.exports = { writeToSheet };
