import { PublicIndexOffset } from '../models/PublicIndexOffset';
import { PublicIndexedTimePointIndexReference } from '../models/PublicIndexedTimePointIndexReference';
export declare class PublicTimePointOperationTimePoint {
    'month': number;
    'hour'?: number;
    'year': number;
    'timezoneSource'?: string;
    'millisecond'?: number;
    'timeType': PublicTimePointOperationTimePointTimeTypeEnum;
    'zoneId': string;
    'day': number;
    'minute'?: number;
    'second'?: number;
    'offset'?: PublicIndexOffset;
    'indexReference': PublicIndexedTimePointIndexReference;
    'property': string;
    'referenceType': string;
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
export declare enum PublicTimePointOperationTimePointTimeTypeEnum {
    PropertyReferenced = "PROPERTY_REFERENCED"
}
