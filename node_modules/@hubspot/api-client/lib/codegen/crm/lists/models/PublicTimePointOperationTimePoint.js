"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PublicTimePointOperationTimePointTimeTypeEnum = exports.PublicTimePointOperationTimePoint = void 0;
class PublicTimePointOperationTimePoint {
    static getAttributeTypeMap() {
        return PublicTimePointOperationTimePoint.attributeTypeMap;
    }
    constructor() {
    }
}
exports.PublicTimePointOperationTimePoint = PublicTimePointOperationTimePoint;
PublicTimePointOperationTimePoint.discriminator = undefined;
PublicTimePointOperationTimePoint.attributeTypeMap = [
    {
        "name": "month",
        "baseName": "month",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "hour",
        "baseName": "hour",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "year",
        "baseName": "year",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "timezoneSource",
        "baseName": "timezoneSource",
        "type": "string",
        "format": ""
    },
    {
        "name": "millisecond",
        "baseName": "millisecond",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "timeType",
        "baseName": "timeType",
        "type": "PublicTimePointOperationTimePointTimeTypeEnum",
        "format": ""
    },
    {
        "name": "zoneId",
        "baseName": "zoneId",
        "type": "string",
        "format": ""
    },
    {
        "name": "day",
        "baseName": "day",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "minute",
        "baseName": "minute",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "second",
        "baseName": "second",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "offset",
        "baseName": "offset",
        "type": "PublicIndexOffset",
        "format": ""
    },
    {
        "name": "indexReference",
        "baseName": "indexReference",
        "type": "PublicIndexedTimePointIndexReference",
        "format": ""
    },
    {
        "name": "property",
        "baseName": "property",
        "type": "string",
        "format": ""
    },
    {
        "name": "referenceType",
        "baseName": "referenceType",
        "type": "string",
        "format": ""
    }
];
var PublicTimePointOperationTimePointTimeTypeEnum;
(function (PublicTimePointOperationTimePointTimeTypeEnum) {
    PublicTimePointOperationTimePointTimeTypeEnum["PropertyReferenced"] = "PROPERTY_REFERENCED";
})(PublicTimePointOperationTimePointTimeTypeEnum = exports.PublicTimePointOperationTimePointTimeTypeEnum || (exports.PublicTimePointOperationTimePointTimeTypeEnum = {}));
//# sourceMappingURL=PublicTimePointOperationTimePoint.js.map