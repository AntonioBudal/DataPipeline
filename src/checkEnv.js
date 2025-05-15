const requiredVars = [
  'GOOGLE_ADS_DEVELOPER_TOKEN',
  'GOOGLE_ADS_CLIENT_ID',
  'GOOGLE_ADS_CLIENT_SECRET',
  'GOOGLE_ADS_REFRESH_TOKEN',
  'GOOGLE_ADS_CUSTOMER_ID',
  'HUBSPOT_PRIVATE_APP_TOKEN',
  'GOOGLE_SHEETS_CLIENT_ID',
  'GOOGLE_SHEETS_CLIENT_SECRET',
  'GOOGLE_SHEETS_REFRESH_TOKEN',
  'GOOGLE_SHEETS_ID',
  'NODE_ENV',
];

function checkEnv() {
  const missing = requiredVars.filter(key => !process.env[key]);
  if (missing.length) {
    console.error('❌ Variáveis de ambiente ausentes:', missing.join(', '));
    process.exit(1);
  }
  console.log('✅ Todas as variáveis de ambiente carregadas.');
}

module.exports = { checkEnv };
