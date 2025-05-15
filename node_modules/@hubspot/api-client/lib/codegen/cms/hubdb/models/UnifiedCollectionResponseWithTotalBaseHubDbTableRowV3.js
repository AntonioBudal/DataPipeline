"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum = exports.UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3 = void 0;
class UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3 {
    static getAttributeTypeMap() {
        return UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3.attributeTypeMap;
    }
    constructor() {
    }
}
exports.UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3 = UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3;
UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3.discriminator = undefined;
UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3.attributeTypeMap = [
    {
        "name": "total",
        "baseName": "total",
        "type": "number",
        "format": "int32"
    },
    {
        "name": "paging",
        "baseName": "paging",
        "type": "Paging",
        "format": ""
    },
    {
        "name": "type",
        "baseName": "type",
        "type": "UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum",
        "format": ""
    },
    {
        "name": "results",
        "baseName": "results",
        "type": "Array<HubDbTableRowV3>",
        "format": ""
    }
];
var UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum;
(function (UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum) {
    UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum["RandomAccess"] = "RANDOM_ACCESS";
    UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum["Streaming"] = "STREAMING";
})(UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum = exports.UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum || (exports.UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3TypeEnum = {}));
//# sourceMappingURL=UnifiedCollectionResponseWithTotalBaseHubDbTableRowV3.js.map