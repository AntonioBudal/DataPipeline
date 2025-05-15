import { ValueWithTimestamp } from '../models/ValueWithTimestamp';
export declare class SimplePublicUpsertObject {
    'createdAt': Date;
    'archived'?: boolean;
    'archivedAt'?: Date;
    '_new': boolean;
    'propertiesWithHistory'?: {
        [key: string]: Array<ValueWithTimestamp>;
    };
    'id': string;
    'properties': {
        [key: string]: string;
    };
    'updatedAt': Date;
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
