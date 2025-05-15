import { PublicTimePointOperationTimePoint } from '../models/PublicTimePointOperationTimePoint';
export declare class PublicSurveyMonkeyValueFilterValueComparison {
    'includeObjectsWithNoValueSet': boolean;
    'operationType': string;
    'value': string;
    'operator': string;
    'requiresTimeZoneConversion': boolean;
    'timestamp': number;
    'upperBound': number;
    'lowerBound': number;
    'defaultComparisonValue'?: string;
    'comparisonPropertyName': string;
    'numberOfDays': number;
    'values': Array<string>;
    'month': string;
    'year': number;
    'day': number;
    'useFiscalYear'?: boolean;
    'fiscalYearStart'?: PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum;
    'timeUnitCount'?: number;
    'timeUnit': string;
    'endpointBehavior'?: string;
    'propertyParser'?: string;
    'timePoint': PublicTimePointOperationTimePoint;
    'type': string;
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
export declare enum PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum {
    January = "JANUARY",
    February = "FEBRUARY",
    March = "MARCH",
    April = "APRIL",
    May = "MAY",
    June = "JUNE",
    July = "JULY",
    August = "AUGUST",
    September = "SEPTEMBER",
    October = "OCTOBER",
    November = "NOVEMBER",
    December = "DECEMBER"
}
