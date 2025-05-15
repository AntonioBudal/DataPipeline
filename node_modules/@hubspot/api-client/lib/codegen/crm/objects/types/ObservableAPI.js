"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ObservableSearchApi = exports.ObservableBatchApi = exports.ObservableBasicApi = void 0;
const rxjsStub_1 = require("../rxjsStub");
const rxjsStub_2 = require("../rxjsStub");
const BasicApi_1 = require("../apis/BasicApi");
class ObservableBasicApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new BasicApi_1.BasicApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new BasicApi_1.BasicApiResponseProcessor();
    }
    archiveWithHttpInfo(objectType, objectId, _options) {
        const requestContextPromise = this.requestFactory.archive(objectType, objectId, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.archiveWithHttpInfo(rsp)));
        }));
    }
    archive(objectType, objectId, _options) {
        return this.archiveWithHttpInfo(objectType, objectId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    createWithHttpInfo(objectType, simplePublicObjectInputForCreate, _options) {
        const requestContextPromise = this.requestFactory.create(objectType, simplePublicObjectInputForCreate, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.createWithHttpInfo(rsp)));
        }));
    }
    create(objectType, simplePublicObjectInputForCreate, _options) {
        return this.createWithHttpInfo(objectType, simplePublicObjectInputForCreate, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getByIdWithHttpInfo(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        const requestContextPromise = this.requestFactory.getById(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getByIdWithHttpInfo(rsp)));
        }));
    }
    getById(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        return this.getByIdWithHttpInfo(objectType, objectId, properties, propertiesWithHistory, associations, archived, idProperty, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getPageWithHttpInfo(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        const requestContextPromise = this.requestFactory.getPage(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getPageWithHttpInfo(rsp)));
        }));
    }
    getPage(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        return this.getPageWithHttpInfo(objectType, limit, after, properties, propertiesWithHistory, associations, archived, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    updateWithHttpInfo(objectType, objectId, simplePublicObjectInput, idProperty, _options) {
        const requestContextPromise = this.requestFactory.update(objectType, objectId, simplePublicObjectInput, idProperty, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.updateWithHttpInfo(rsp)));
        }));
    }
    update(objectType, objectId, simplePublicObjectInput, idProperty, _options) {
        return this.updateWithHttpInfo(objectType, objectId, simplePublicObjectInput, idProperty, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableBasicApi = ObservableBasicApi;
const BatchApi_1 = require("../apis/BatchApi");
class ObservableBatchApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new BatchApi_1.BatchApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new BatchApi_1.BatchApiResponseProcessor();
    }
    archiveWithHttpInfo(objectType, batchInputSimplePublicObjectId, _options) {
        const requestContextPromise = this.requestFactory.archive(objectType, batchInputSimplePublicObjectId, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.archiveWithHttpInfo(rsp)));
        }));
    }
    archive(objectType, batchInputSimplePublicObjectId, _options) {
        return this.archiveWithHttpInfo(objectType, batchInputSimplePublicObjectId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    createWithHttpInfo(objectType, batchInputSimplePublicObjectInputForCreate, _options) {
        const requestContextPromise = this.requestFactory.create(objectType, batchInputSimplePublicObjectInputForCreate, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.createWithHttpInfo(rsp)));
        }));
    }
    create(objectType, batchInputSimplePublicObjectInputForCreate, _options) {
        return this.createWithHttpInfo(objectType, batchInputSimplePublicObjectInputForCreate, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    readWithHttpInfo(objectType, batchReadInputSimplePublicObjectId, archived, _options) {
        const requestContextPromise = this.requestFactory.read(objectType, batchReadInputSimplePublicObjectId, archived, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.readWithHttpInfo(rsp)));
        }));
    }
    read(objectType, batchReadInputSimplePublicObjectId, archived, _options) {
        return this.readWithHttpInfo(objectType, batchReadInputSimplePublicObjectId, archived, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    updateWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInput, _options) {
        const requestContextPromise = this.requestFactory.update(objectType, batchInputSimplePublicObjectBatchInput, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.updateWithHttpInfo(rsp)));
        }));
    }
    update(objectType, batchInputSimplePublicObjectBatchInput, _options) {
        return this.updateWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInput, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    upsertWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options) {
        const requestContextPromise = this.requestFactory.upsert(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.upsertWithHttpInfo(rsp)));
        }));
    }
    upsert(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options) {
        return this.upsertWithHttpInfo(objectType, batchInputSimplePublicObjectBatchInputUpsert, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableBatchApi = ObservableBatchApi;
const SearchApi_1 = require("../apis/SearchApi");
class ObservableSearchApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new SearchApi_1.SearchApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new SearchApi_1.SearchApiResponseProcessor();
    }
    doSearchWithHttpInfo(objectType, publicObjectSearchRequest, _options) {
        const requestContextPromise = this.requestFactory.doSearch(objectType, publicObjectSearchRequest, _options);
        let middlewarePreObservable = (0, rxjsStub_1.from)(requestContextPromise);
        for (let middleware of this.configuration.middleware) {
            middlewarePreObservable = middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => middleware.pre(ctx)));
        }
        return middlewarePreObservable.pipe((0, rxjsStub_2.mergeMap)((ctx) => this.configuration.httpApi.send(ctx))).
            pipe((0, rxjsStub_2.mergeMap)((response) => {
            let middlewarePostObservable = (0, rxjsStub_1.of)(response);
            for (let middleware of this.configuration.middleware) {
                middlewarePostObservable = middlewarePostObservable.pipe((0, rxjsStub_2.mergeMap)((rsp) => middleware.post(rsp)));
            }
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.doSearchWithHttpInfo(rsp)));
        }));
    }
    doSearch(objectType, publicObjectSearchRequest, _options) {
        return this.doSearchWithHttpInfo(objectType, publicObjectSearchRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableSearchApi = ObservableSearchApi;
//# sourceMappingURL=ObservableAPI.js.map