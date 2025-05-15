"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum = exports.HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum = exports.HubSpotFormDefinitionAllOfLegalConsentOptions = void 0;
class HubSpotFormDefinitionAllOfLegalConsentOptions {
    static getAttributeTypeMap() {
        return HubSpotFormDefinitionAllOfLegalConsentOptions.attributeTypeMap;
    }
    constructor() {
    }
}
exports.HubSpotFormDefinitionAllOfLegalConsentOptions = HubSpotFormDefinitionAllOfLegalConsentOptions;
HubSpotFormDefinitionAllOfLegalConsentOptions.discriminator = undefined;
HubSpotFormDefinitionAllOfLegalConsentOptions.attributeTypeMap = [
    {
        "name": "type",
        "baseName": "type",
        "type": "HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum",
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
        "type": "HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum",
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
var HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum;
(function (HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum) {
    HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum["None"] = "none";
    HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum["LegitimateInterest"] = "legitimate_interest";
    HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum["ExplicitConsentToProcess"] = "explicit_consent_to_process";
    HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum["ImplicitConsentToProcess"] = "implicit_consent_to_process";
})(HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum = exports.HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum || (exports.HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum = {}));
var HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum;
(function (HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum) {
    HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum["Lead"] = "lead";
    HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum["Client"] = "client";
    HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum["Other"] = "other";
})(HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum = exports.HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum || (exports.HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum = {}));
//# sourceMappingURL=HubSpotFormDefinitionAllOfLegalConsentOptions.js.map