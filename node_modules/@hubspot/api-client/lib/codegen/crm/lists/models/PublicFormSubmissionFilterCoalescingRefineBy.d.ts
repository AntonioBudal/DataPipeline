import { PublicTimeOffset } from '../models/PublicTimeOffset';
import { PublicTimePointOperationTimePoint } from '../models/PublicTimePointOperationTimePoint';
export declare class PublicFormSubmissionFilterCoalescingRefineBy {
    'maxOccurrences'?: number;
    'type': string;
    'minOccurrences'?: number;
    'setType': string;
    'comparison': string;
    'timeOffset': PublicTimeOffset;
    'upperBoundOffset': PublicTimeOffset;
    'rangeType': string;
    'lowerBoundOffset': PublicTimeOffset;
    'timestamp': number;
    'upperTimestamp': number;
    'lowerTimestamp': number;
    'endpointBehavior'?: string;
    'includeObjectsWithNoValueSet': boolean;
    'propertyParser'?: string;
    'operationType': string;
    'timePoint': PublicTimePointOperationTimePoint;
    'operator': string;
    'upperBoundEndpointBehavior'?: string;
    'upperBoundTimePoint': PublicTimePointOperationTimePoint;
    'lowerBoundEndpointBehavior'?: string;
    'lowerBoundTimePoint': PublicTimePointOperationTimePoint;
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
