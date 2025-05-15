"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum = exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum = exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions = void 0;
class HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions {
    static getAttributeTypeMap() {
        return HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions.attributeTypeMap;
    }
    constructor() {
    }
}
exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions = HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions;
HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions.discriminator = undefined;
HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions.attributeTypeMap = [
    {
        "name": "type",
        "baseName": "type",
        "type": "HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum",
        "format": ""
    },
    {
        "name": "subscriptionTypeIds",
        "baseName": "subscriptionTypeIds",
        "type": "Array<number>",
        "format": "int64"
    },
    {
        "name": "lawfulBasis",
        "baseName": "lawfulBasis",
        "type": "HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum",
        "format": ""
    },
    {
        "name": "privacyText",
        "baseName": "privacyText",
        "type": "string",
        "format": ""
    },
    {
        "name": "communicationsCheckboxes",
        "baseName": "communicationsCheckboxes",
        "type": "Array<LegalConsentCheckbox>",
        "format": ""
    },
    {
        "name": "communicationConsentText",
        "baseName": "communicationConsentText",
        "type": "string",
        "format": ""
    },
    {
        "name": "consentToProcessCheckboxLabel",
        "baseName": "consentToProcessCheckboxLabel",
        "type": "string",
        "format": ""
    },
    {
        "name": "consentToProcessFooterText",
        "baseName": "consentToProcessFooterText",
        "type": "string",
        "format": ""
    },
    {
        "name": "consentToProcessText",
        "baseName": "consentToProcessText",
        "type": "string",
        "format": ""
    }
];
var HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum;
(function (HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum) {
    HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum["None"] = "none";
    HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum["LegitimateInterest"] = "legitimate_interest";
    HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum["ExplicitConsentToProcess"] = "explicit_consent_to_process";
    HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum["ImplicitConsentToProcess"] = "implicit_consent_to_process";
})(HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum = exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum || (exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum = {}));
var HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum;
(function (HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum) {
    HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum["Lead"] = "lead";
    HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum["Client"] = "client";
    HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum["Other"] = "other";
})(HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum = exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum || (exports.HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum = {}));
//# sourceMappingURL=HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions.js.map