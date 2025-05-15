"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PublicFormSubmissionFilterCoalescingRefineBy = void 0;
class PublicFormSubmissionFilterCoalescingRefineBy {
    static getAttributeTypeMap() {
        return PublicFormSubmissionFilterCoalescingRefineBy.attributeTypeMap;
    }
    constructor() {
    }
}
exports.PublicFormSubmissionFilterCoalescingRefineBy = PublicFormSubmissionFilterCoalescingRefineBy;
PublicFormSubmissionFilterCoalescingRefineBy.discriminator = undefined;
PublicFormSubmissionFilterCoalescingRefineBy.attributeTypeMap = [
    {
        "name": "maxOccurrences",
        "baseName": "maxOccurrences",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "type",
        "baseName": "type",
        "type": "string",
        "format": ""
    },
    {
        "name": "minOccurrences",
        "baseName": "minOccurrences",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "setType",
        "baseName": "setType",
        "type": "string",
        "format": ""
    },
    {
        "name": "comparison",
        "baseName": "comparison",
        "type": "string",
        "format": ""
    },
    {
        "name": "timeOffset",
        "baseName": "timeOffset",
        "type": "PublicTimeOffset",
        "format": ""
    },
    {
        "name": "upperBoundOffset",
        "baseName": "upperBoundOffset",
        "type": "PublicTimeOffset",
        "format": ""
    },
    {
        "name": "rangeType",
        "baseName": "rangeType",
        "type": "string",
        "format": ""
    },
    {
        "name": "lowerBoundOffset",
        "baseName": "lowerBoundOffset",
        "type": "PublicTimeOffset",
        "format": ""
    },
    {
        "name": "timestamp",
        "baseName": "timestamp",
        "type": "number",
        "format": "int64"
    },
    {
        "name": "upperTimestamp",
        "baseName": "upperTimestamp",
        "type": "number",
        "format": "int64"
    },
    {
        "name": "lowerTimestamp",
        "baseName": "lowerTimestamp",
        "type": "number",
        "format": "int64"
    },
    {
        "name": "endpointBehavior",
        "baseName": "endpointBehavior",
        "type": "string",
        "format": ""
    },
    {
        "name": "includeObjectsWithNoValueSet",
        "baseName": "includeObjectsWithNoValueSet",
        "type": "boolean",
        "format": ""
    },
    {
        "name": "propertyParser",
        "baseName": "propertyParser",
        "type": "string",
        "format": ""
    },
    {
        "name": "operationType",
        "baseName": "operationType",
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
        "name": "operator",
        "baseName": "operator",
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
//# sourceMappingURL=PublicFormSubmissionFilterCoalescingRefineBy.js.map