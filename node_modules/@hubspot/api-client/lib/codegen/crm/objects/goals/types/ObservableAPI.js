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
    getByIdWithHttpInfo(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        const requestContextPromise = this.requestFactory.getById(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options);
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
    getById(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options) {
        return this.getByIdWithHttpInfo(goalTargetId, properties, propertiesWithHistory, associations, archived, idProperty, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getPageWithHttpInfo(limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        const requestContextPromise = this.requestFactory.getPage(limit, after, properties, propertiesWithHistory, associations, archived, _options);
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
    getPage(limit, after, properties, propertiesWithHistory, associations, archived, _options) {
        return this.getPageWithHttpInfo(limit, after, properties, propertiesWithHistory, associations, archived, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
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
    readWithHttpInfo(batchReadInputSimplePublicObjectId, archived, _options) {
        const requestContextPromise = this.requestFactory.read(batchReadInputSimplePublicObjectId, archived, _options);
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
    read(batchReadInputSimplePublicObjectId, archived, _options) {
        return this.readWithHttpInfo(batchReadInputSimplePublicObjectId, archived, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
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
    doSearchWithHttpInfo(publicObjectSearchRequest, _options) {
        const requestContextPromise = this.requestFactory.doSearch(publicObjectSearchRequest, _options);
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
    doSearch(publicObjectSearchRequest, _options) {
        return this.doSearchWithHttpInfo(publicObjectSearchRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableSearchApi = ObservableSearchApi;
//# sourceMappingURL=ObservableAPI.js.map