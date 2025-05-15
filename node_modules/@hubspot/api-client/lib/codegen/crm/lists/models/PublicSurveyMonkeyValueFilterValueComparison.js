"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum = exports.PublicSurveyMonkeyValueFilterValueComparison = void 0;
class PublicSurveyMonkeyValueFilterValueComparison {
    static getAttributeTypeMap() {
        return PublicSurveyMonkeyValueFilterValueComparison.attributeTypeMap;
    }
    constructor() {
    }
}
exports.PublicSurveyMonkeyValueFilterValueComparison = PublicSurveyMonkeyValueFilterValueComparison;
PublicSurveyMonkeyValueFilterValueComparison.discriminator = undefined;
PublicSurveyMonkeyValueFilterValueComparison.attributeTypeMap = [
    {
        "name": "includeObjectsWithNoValueSet",
        "baseName": "includeObjectsWithNoValueSet",
        "type": "boolean",
        "format": ""
    },
    {
        "name": "operationType",
        "baseName": "operationType",
        "type": "string",
        "format": ""
    },
    {
        "name": "value",
        "baseName": "value",
        "type": "string",
        "format": ""
    },
    {
        "name": "operator",
        "baseName": "operator",
        "type": "string",
        "format": ""
    },
    {
        "name": "requiresTimeZoneConversion",
        "baseName": "requiresTimeZoneConversion",
        "type": "boolean",
        "format": ""
    },
    {
        "name": "timestamp",
        "baseName": "timestamp",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "upperBound",
        "baseName": "upperBound",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "lowerBound",
        "baseName": "lowerBound",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "defaultComparisonValue",
        "baseName": "defaultComparisonValue",
        "type": "string",
        "format": ""
    },
    {
        "name": "comparisonPropertyName",
        "baseName": "comparisonPropertyName",
        "type": "string",
        "format": ""
    },
    {
        "name": "numberOfDays",
        "baseName": "numberOfDays",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "values",
        "baseName": "values",
        "type": "Array<string>",
        "format": ""
    },
    {
        "name": "month",
        "baseName": "month",
        "type": "string",
        "format": ""
    },
    {
        "name": "year",
        "baseName": "year",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "day",
        "baseName": "day",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "useFiscalYear",
        "baseName": "useFiscalYear",
        "type": "boolean",
        "format": ""
    },
    {
        "name": "fiscalYearStart",
        "baseName": "fiscalYearStart",
        "type": "PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum",
        "format": ""
    },
    {
        "name": "timeUnitCount",
        "baseName": "timeUnitCount",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "timeUnit",
        "baseName": "timeUnit",
        "type": "string",
        "format": ""
    },
    {
        "name": "endpointBehavior",
        "baseName": "endpointBehavior",
        "type": "string",
        "format": ""
    },
    {
        "name": "propertyParser",
        "baseName": "propertyParser",
        "type": "string",
        "format": ""
    },
    {
        "name": "timePoint",
        "baseName": "timePoint",
        "type": "PublicTimePointOperationTimePoint",
        "format": ""
    },
    {
        "name": "type",
        "baseName": "type",
        "type": "string",
        "format": ""
    },
    {
        "name": "upperBoundEndpointBehavior",
        "baseName": "upperBoundEndpointBehavior",
        "type": "string",
        "format": ""
    },
    {
        "name": "upperBoundTimePoint",
        "baseName": "upperBoundTimePoint",
        "type": "PublicTimePointOperationTimePoint",
        "format": ""
    },
    {
        "name": "lowerBoundEndpointBehavior",
        "baseName": "lowerBoundEndpointBehavior",
        "type": "string",
        "format": ""
    },
    {
        "name": "lowerBoundTimePoint",
        "baseName": "lowerBoundTimePoint",
        "type": "PublicTimePointOperationTimePoint",
        "format": ""
    }
];
var PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum;
(function (PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum) {
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["January"] = "JANUARY";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["February"] = "FEBRUARY";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["March"] = "MARCH";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["April"] = "APRIL";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["May"] = "MAY";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["June"] = "JUNE";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["July"] = "JULY";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["August"] = "AUGUST";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["September"] = "SEPTEMBER";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["October"] = "OCTOBER";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["November"] = "NOVEMBER";
    PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum["December"] = "DECEMBER";
})(PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum = exports.PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum || (exports.PublicSurveyMonkeyValueFilterValueComparisonFiscalYearStartEnum = {}));
//# sourceMappingURL=PublicSurveyMonkeyValueFilterValueComparison.js.map