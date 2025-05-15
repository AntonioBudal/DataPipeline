"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseSearchApi = exports.PromiseBatchApi = exports.PromiseBasicApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class PromiseBasicApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableBasicApi(configuration, requestFactory, responseProcessor);
    }
    getByIdWithHttpInfo(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        const result = this.api.getByIdWithHttpInfo(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options);
        return result.toPromise();
    }
    getById(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        const result = this.api.getById(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options);
        return result.toPromise();
    }
    getPageWithHttpInfo(limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        const result = this.api.getPageWithHttpInfo(limit, after, properties, propertiesWithHistory, associations, archived, _options);
        return result.toPromise();
    }
    getPage(limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        const result = this.api.getPage(limit, after, properties, propertiesWithHistory, associations, archived, _options);
        return result.toPromise();
    }
}
exports.PromiseBasicApi = PromiseBasicApi;
const ObservableAPI_2 = require("./ObservableAPI");
class PromiseBatchApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_2.ObservableBatchApi(configuration, requestFactory, responseProcessor);
    }
    readWithHttpInfo(batchReadInputSimplePublicObjectId, archived, _options) {
        const result = this.api.readWithHttpInfo(batchReadInputSimplePublicObjectId, archived, _options);
        return result.toPromise();
    }
    read(batchReadInputSimplePublicObjectId, archived, _options) {
        const result = this.api.read(batchReadInputSimplePublicObjectId, archived, _options);
        return result.toPromise();
    }
}
exports.PromiseBatchApi = PromiseBatchApi;
const ObservableAPI_3 = require("./ObservableAPI");
class PromiseSearchApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_3.ObservableSearchApi(configuration, requestFactory, responseProcessor);
    }
    doSearchWithHttpInfo(publicObjectSearchRequest, _options) {
        const result = this.api.doSearchWithHttpInfo(publicObjectSearchRequest, _options);
        return result.toPromise();
    }
    doSearch(publicObjectSearchRequest, _options) {
        const result = this.api.doSearch(publicObjectSearchRequest, _options);
        return result.toPromise();
    }
}
exports.PromiseSearchApi = PromiseSearchApi;
//# sourceMappingURL=PromiseAPI.js.map