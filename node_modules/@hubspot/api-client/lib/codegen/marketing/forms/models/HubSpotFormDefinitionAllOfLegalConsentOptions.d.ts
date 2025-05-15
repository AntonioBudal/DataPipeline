import { LegalConsentCheckbox } from '../models/LegalConsentCheckbox';
export declare class HubSpotFormDefinitionAllOfLegalConsentOptions {
    'type': HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum;
    'subscriptionTypeIds': Array<number>;
    'lawfulBasis': HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum;
    'privacyText': string;
    'communicationsCheckboxes': Array<LegalConsentCheckbox>;
    'communicationConsentText'?: string;
    'consentToProcessCheckboxLabel'?: string;
    'consentToProcessFooterText'?: string;
    'consentToProcessText'?: string;
    static readonly discriminator: string | undefined;
    static readonly attributeTypeMap: Array<{
        name: string;
        baseName: string;
        type: string;
        format: string;
    }>;
    static getAttributeTypeMap(): {
        name: string;
        baseName: string;
        type: string;
        format: string;
    }[];
    constructor();
}
export declare enum HubSpotFormDefinitionAllOfLegalConsentOptionsTypeEnum {
    None = "none",
    LegitimateInterest = "legitimate_interest",
    ExplicitConsentToProcess = "explicit_consent_to_process",
    ImplicitConsentToProcess = "implicit_consent_to_process"
}
export declare enum HubSpotFormDefinitionAllOfLegalConsentOptionsLawfulBasisEnum {
    Lead = "lead",
    Client = "client",
    Other = "other"
}
