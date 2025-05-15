"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ObservableMembershipsApi = exports.ObservableMappingApi = exports.ObservableListsApi = exports.ObservableFoldersApi = void 0;
const rxjsStub_1 = require("../rxjsStub");
const rxjsStub_2 = require("../rxjsStub");
const FoldersApi_1 = require("../apis/FoldersApi");
class ObservableFoldersApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new FoldersApi_1.FoldersApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new FoldersApi_1.FoldersApiResponseProcessor();
    }
    createWithHttpInfo(listFolderCreateRequest, _options) {
        const requestContextPromise = this.requestFactory.create(listFolderCreateRequest, _options);
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
    create(listFolderCreateRequest, _options) {
        return this.createWithHttpInfo(listFolderCreateRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getAllWithHttpInfo(folderId, _options) {
        const requestContextPromise = this.requestFactory.getAll(folderId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getAllWithHttpInfo(rsp)));
        }));
    }
    getAll(folderId, _options) {
        return this.getAllWithHttpInfo(folderId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    moveWithHttpInfo(folderId, newParentFolderId, _options) {
        const requestContextPromise = this.requestFactory.move(folderId, newParentFolderId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.moveWithHttpInfo(rsp)));
        }));
    }
    move(folderId, newParentFolderId, _options) {
        return this.moveWithHttpInfo(folderId, newParentFolderId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    moveListWithHttpInfo(listMoveRequest, _options) {
        const requestContextPromise = this.requestFactory.moveList(listMoveRequest, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.moveListWithHttpInfo(rsp)));
        }));
    }
    moveList(listMoveRequest, _options) {
        return this.moveListWithHttpInfo(listMoveRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    removeWithHttpInfo(folderId, _options) {
        const requestContextPromise = this.requestFactory.remove(folderId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.removeWithHttpInfo(rsp)));
        }));
    }
    remove(folderId, _options) {
        return this.removeWithHttpInfo(folderId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    renameWithHttpInfo(folderId, newFolderName, _options) {
        const requestContextPromise = this.requestFactory.rename(folderId, newFolderName, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.renameWithHttpInfo(rsp)));
        }));
    }
    rename(folderId, newFolderName, _options) {
        return this.renameWithHttpInfo(folderId, newFolderName, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableFoldersApi = ObservableFoldersApi;
const ListsApi_1 = require("../apis/ListsApi");
class ObservableListsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new ListsApi_1.ListsApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new ListsApi_1.ListsApiResponseProcessor();
    }
    createWithHttpInfo(listCreateRequest, _options) {
        const requestContextPromise = this.requestFactory.create(listCreateRequest, _options);
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
    create(listCreateRequest, _options) {
        return this.createWithHttpInfo(listCreateRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    doSearchWithHttpInfo(listSearchRequest, _options) {
        const requestContextPromise = this.requestFactory.doSearch(listSearchRequest, _options);
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
    doSearch(listSearchRequest, _options) {
        return this.doSearchWithHttpInfo(listSearchRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getAllWithHttpInfo(listIds, includeFilters, _options) {
        const requestContextPromise = this.requestFactory.getAll(listIds, includeFilters, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getAllWithHttpInfo(rsp)));
        }));
    }
    getAll(listIds, includeFilters, _options) {
        return this.getAllWithHttpInfo(listIds, includeFilters, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getByIdWithHttpInfo(listId, includeFilters, _options) {
        const requestContextPromise = this.requestFactory.getById(listId, includeFilters, _options);
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
    getById(listId, includeFilters, _options) {
        return this.getByIdWithHttpInfo(listId, includeFilters, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getByNameWithHttpInfo(listName, objectTypeId, includeFilters, _options) {
        const requestContextPromise = this.requestFactory.getByName(listName, objectTypeId, includeFilters, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getByNameWithHttpInfo(rsp)));
        }));
    }
    getByName(listName, objectTypeId, includeFilters, _options) {
        return this.getByNameWithHttpInfo(listName, objectTypeId, includeFilters, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    removeWithHttpInfo(listId, _options) {
        const requestContextPromise = this.requestFactory.remove(listId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.removeWithHttpInfo(rsp)));
        }));
    }
    remove(listId, _options) {
        return this.removeWithHttpInfo(listId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    restoreWithHttpInfo(listId, _options) {
        const requestContextPromise = this.requestFactory.restore(listId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.restoreWithHttpInfo(rsp)));
        }));
    }
    restore(listId, _options) {
        return this.restoreWithHttpInfo(listId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    updateListFiltersWithHttpInfo(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options) {
        const requestContextPromise = this.requestFactory.updateListFilters(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.updateListFiltersWithHttpInfo(rsp)));
        }));
    }
    updateListFilters(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options) {
        return this.updateListFiltersWithHttpInfo(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    updateNameWithHttpInfo(listId, listName, includeFilters, _options) {
        const requestContextPromise = this.requestFactory.updateName(listId, listName, includeFilters, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.updateNameWithHttpInfo(rsp)));
        }));
    }
    updateName(listId, listName, includeFilters, _options) {
        return this.updateNameWithHttpInfo(listId, listName, includeFilters, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableListsApi = ObservableListsApi;
const MappingApi_1 = require("../apis/MappingApi");
class ObservableMappingApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new MappingApi_1.MappingApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new MappingApi_1.MappingApiResponseProcessor();
    }
    translateLegacyListIdToListIdWithHttpInfo(legacyListId, _options) {
        const requestContextPromise = this.requestFactory.translateLegacyListIdToListId(legacyListId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.translateLegacyListIdToListIdWithHttpInfo(rsp)));
        }));
    }
    translateLegacyListIdToListId(legacyListId, _options) {
        return this.translateLegacyListIdToListIdWithHttpInfo(legacyListId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    translateLegacyListIdToListIdBatchWithHttpInfo(requestBody, _options) {
        const requestContextPromise = this.requestFactory.translateLegacyListIdToListIdBatch(requestBody, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.translateLegacyListIdToListIdBatchWithHttpInfo(rsp)));
        }));
    }
    translateLegacyListIdToListIdBatch(requestBody, _options) {
        return this.translateLegacyListIdToListIdBatchWithHttpInfo(requestBody, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableMappingApi = ObservableMappingApi;
const MembershipsApi_1 = require("../apis/MembershipsApi");
class ObservableMembershipsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new MembershipsApi_1.MembershipsApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new MembershipsApi_1.MembershipsApiResponseProcessor();
    }
    addWithHttpInfo(listId, requestBody, _options) {
        const requestContextPromise = this.requestFactory.add(listId, requestBody, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.addWithHttpInfo(rsp)));
        }));
    }
    add(listId, requestBody, _options) {
        return this.addWithHttpInfo(listId, requestBody, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    addAllFromListWithHttpInfo(listId, sourceListId, _options) {
        const requestContextPromise = this.requestFactory.addAllFromList(listId, sourceListId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.addAllFromListWithHttpInfo(rsp)));
        }));
    }
    addAllFromList(listId, sourceListId, _options) {
        return this.addAllFromListWithHttpInfo(listId, sourceListId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    addAndRemoveWithHttpInfo(listId, membershipChangeRequest, _options) {
        const requestContextPromise = this.requestFactory.addAndRemove(listId, membershipChangeRequest, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.addAndRemoveWithHttpInfo(rsp)));
        }));
    }
    addAndRemove(listId, membershipChangeRequest, _options) {
        return this.addAndRemoveWithHttpInfo(listId, membershipChangeRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getListsWithHttpInfo(objectTypeId, recordId, _options) {
        const requestContextPromise = this.requestFactory.getLists(objectTypeId, recordId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getListsWithHttpInfo(rsp)));
        }));
    }
    getLists(objectTypeId, recordId, _options) {
        return this.getListsWithHttpInfo(objectTypeId, recordId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getPageWithHttpInfo(listId, after, before, limit, _options) {
        const requestContextPromise = this.requestFactory.getPage(listId, after, before, limit, _options);
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
    getPage(listId, after, before, limit, _options) {
        return this.getPageWithHttpInfo(listId, after, before, limit, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getPageOrderedByAddedToListDateWithHttpInfo(listId, after, before, limit, _options) {
        const requestContextPromise = this.requestFactory.getPageOrderedByAddedToListDate(listId, after, before, limit, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getPageOrderedByAddedToListDateWithHttpInfo(rsp)));
        }));
    }
    getPageOrderedByAddedToListDate(listId, after, before, limit, _options) {
        return this.getPageOrderedByAddedToListDateWithHttpInfo(listId, after, before, limit, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    removeWithHttpInfo(listId, requestBody, _options) {
        const requestContextPromise = this.requestFactory.remove(listId, requestBody, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.removeWithHttpInfo(rsp)));
        }));
    }
    remove(listId, requestBody, _options) {
        return this.removeWithHttpInfo(listId, requestBody, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    removeAllWithHttpInfo(listId, _options) {
        const requestContextPromise = this.requestFactory.removeAll(listId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.removeAllWithHttpInfo(rsp)));
        }));
    }
    removeAll(listId, _options) {
        return this.removeAllWithHttpInfo(listId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableMembershipsApi = ObservableMembershipsApi;
//# sourceMappingURL=ObservableAPI.js.map