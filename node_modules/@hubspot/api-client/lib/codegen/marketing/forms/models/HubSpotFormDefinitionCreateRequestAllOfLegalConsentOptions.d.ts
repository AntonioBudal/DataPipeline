import { LegalConsentCheckbox } from '../models/LegalConsentCheckbox';
export declare class HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptions {
    'type': HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum;
    'subscriptionTypeIds': Array<number>;
    'lawfulBasis': HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum;
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
export declare enum HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsTypeEnum {
    None = "none",
    LegitimateInterest = "legitimate_interest",
    ExplicitConsentToProcess = "explicit_consent_to_process",
    ImplicitConsentToProcess = "implicit_consent_to_process"
}
export declare enum HubSpotFormDefinitionCreateRequestAllOfLegalConsentOptionsLawfulBasisEnum {
    Lead = "lead",
    Client = "client",
    Other = "other"
}
