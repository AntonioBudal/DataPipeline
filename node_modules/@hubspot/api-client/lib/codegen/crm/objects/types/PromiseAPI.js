"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseSearchApi = exports.PromiseBatchApi = exports.PromiseBasicApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class PromiseBasicApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableBasicApi(configuration, requestFactory, responseProcessor);
    }
    archiveWithHttpInfo(objectType, objectId, _options) {
        const result = this.api.archiveWithHttpInfo(objectType, objectId, _options);
        return result.toPromise();
    }
    archive(objectType, objectId, _options) {
        const result = this.api.archive(objectType, objectId, _options);
        return result.toPromise();
    }
    createWithHttpInfo(objectType, simplePublicObjectInputForCreate, _options) {
        const result = this.api.createWithHttpInfo(objectType, simplePublicObjectInputForCreate, _options);
        return result.toPromise();
    }
    create(objectType, simplePublicObjectInputForCreate, _options) {
        const result = this.api.create(objectType, simplePublicObjectInputForCreate, _options);
        return result.toPromise();
    }
    getByIdWithHttpInfo(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        const result = this.api.getByIdWithHttpInfo(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options);
        return result.toPromise();
    }
    getById(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        const result = this.api.getById(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options);
        return result.toPromise();
    }
    getPageWithHttpInfo(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        const result = this.api.getPageWithHttpInfo(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options);
        return result.toPromise();
    }
    getPage(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        const result = this.api.getPage(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options);
        return result.toPromise();
    }
    updateWithHttpInfo(objectType, objectId, simplePublicObjectInput, idProperty, _options) {
        const result = this.api.updateWithHttpInfo(objectType, objectId, simplePublicObjectInput, idProperty, _options);
        return result.toPromise();
    }
    update(objectType, objectId, simplePublicObjectInput, idProperty, _options) {
        const result = this.api.update(objectType, objectId, simplePublicObjectInput, idProperty, _options);
        return result.toPromise();
    }
}
exports.PromiseBasicApi = PromiseBasicApi;
const ObservableAPI_2 = require("./ObservableAPI");
class PromiseBatchApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_2.ObservableBatchApi(configuration, requestFactory, responseProcessor);
    }
    archiveWithHttpInfo(objectType, batchInputSimplePublicObjectId, _options) {
        const result = this.api.archiveWithHttpInfo(objectType, batchInputSimplePublicObjectId, _options);
        return result.toPromise();
    }
    archive(objectType, batchInputSimplePublicObjectId, _options) {
        const result = this.api.archive(objectType, batchInputSimplePublicObjectId, _options);
        return result.toPromise();
    }
    createWithHttpInfo(objectType, batchInputSimplePublicObjectInputForCreate, _options) {
        const result = this.api.createWithHttpInfo(objectType, batchInputSimplePublicObjectInputForCreate, _options);
        return result.toPromise();
    }
    create(objectType, batchInputSimplePublicObjectInputForCreate, _options) {
        const result = this.api.create(objectType, batchInputSimplePublicObjectInputForCreate, _options);
        return result.toPromise();
    }
    readWithHttpInfo(objectType, batchReadInputSimplePublicObjectId, archived, _options) {
        const result = this.api.readWithHttpInfo(objectType, batchReadInputSimplePublicObjectId, archived, _options);
        return result.toPromise();
    }
    read(objectType, batchReadInputSimplePublicObjectId, archived, _options) {
        const result = this.api.read(objectType, batchReadInputSimplePublicObjectId, archived, _options);
        return result.toPromise();
    }
    updateWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInput, _options) {
        const result = this.api.updateWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInput, _options);
        return result.toPromise();
    }
    update(objectType, batchInputSimplePublicObjectBatchInput, _options) {
        const result = this.api.update(objectType, batchInputSimplePublicObjectBatchInput, _options);
        return result.toPromise();
    }
    upsertWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options) {
        const result = this.api.upsertWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options);
        return result.toPromise();
    }
    upsert(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options) {
        const result = this.api.upsert(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options);
        return result.toPromise();
    }
}
exports.PromiseBatchApi = PromiseBatchApi;
const ObservableAPI_3 = require("./ObservableAPI");
class PromiseSearchApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_3.ObservableSearchApi(configuration, requestFactory, responseProcessor);
    }
    doSearchWithHttpInfo(objectType, publicObjectSearchRequest, _options) {
        const result = this.api.doSearchWithHttpInfo(objectType, publicObjectSearchRequest, _options);
        return result.toPromise();
    }
    doSearch(objectType, publicObjectSearchRequest, _options) {
        const result = this.api.doSearch(objectType, publicObjectSearchRequest, _options);
        return result.toPromise();
    }
}
exports.PromiseSearchApi = PromiseSearchApi;
//# sourceMappingURL=PromiseAPI.js.map