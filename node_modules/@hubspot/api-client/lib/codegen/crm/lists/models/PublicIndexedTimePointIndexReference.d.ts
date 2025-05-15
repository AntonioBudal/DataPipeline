export declare class PublicIndexedTimePointIndexReference {
    'hour'?: number;
    'millisecond'?: number;
    'referenceType': PublicIndexedTimePointIndexReferenceReferenceTypeEnum;
    'minute'?: number;
    'second'?: number;
    'dayOfWeek': PublicIndexedTimePointIndexReferenceDayOfWeekEnum;
    'month': number;
    'day': number;
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
export declare enum PublicIndexedTimePointIndexReferenceReferenceTypeEnum {
    Month = "MONTH"
}
export declare enum PublicIndexedTimePointIndexReferenceDayOfWeekEnum {
    Monday = "MONDAY",
    Tuesday = "TUESDAY",
    Wednesday = "WEDNESDAY",
    Thursday = "THURSDAY",
    Friday = "FRIDAY",
    Saturday = "SATURDAY",
    Sunday = "SUNDAY"
}
