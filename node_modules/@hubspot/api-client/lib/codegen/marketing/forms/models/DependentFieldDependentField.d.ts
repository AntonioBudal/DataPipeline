import { DependentField } from '../models/DependentField';
import { EnumeratedFieldOption } from '../models/EnumeratedFieldOption';
import { NumberFieldValidation } from '../models/NumberFieldValidation';
export declare class DependentFieldDependentField {
    'objectTypeId': string;
    'hidden': boolean;
    'defaultValue'?: string;
    'name': string;
    'description'?: string;
    'dependentFields': Array<DependentField>;
    'label': string;
    'placeholder'?: string;
    'fieldType': DependentFieldDependentFieldFieldTypeEnum;
    'required': boolean;
    'validation': NumberFieldValidation;
    'useCountryCodeSelect': boolean;
    'options': Array<EnumeratedFieldOption>;
    'defaultValues': Array<string>;
    'allowMultipleFiles': boolean;
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
export declare enum DependentFieldDependentFieldFieldTypeEnum {
    PaymentLinkRadio = "payment_link_radio"
}
