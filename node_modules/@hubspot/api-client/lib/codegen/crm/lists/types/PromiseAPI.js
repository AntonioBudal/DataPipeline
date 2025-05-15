"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseMembershipsApi = exports.PromiseMappingApi = exports.PromiseListsApi = exports.PromiseFoldersApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class PromiseFoldersApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableFoldersApi(configuration, requestFactory, responseProcessor);
    }
    createWithHttpInfo(listFolderCreateRequest, _options) {
        const result = this.api.createWithHttpInfo(listFolderCreateRequest, _options);
        return result.toPromise();
    }
    create(listFolderCreateRequest, _options) {
        const result = this.api.create(listFolderCreateRequest, _options);
        return result.toPromise();
    }
    getAllWithHttpInfo(folderId, _options) {
        const result = this.api.getAllWithHttpInfo(folderId, _options);
        return result.toPromise();
    }
    getAll(folderId, _options) {
        const result = this.api.getAll(folderId, _options);
        return result.toPromise();
    }
    moveWithHttpInfo(folderId, newParentFolderId, _options) {
        const result = this.api.moveWithHttpInfo(folderId, newParentFolderId, _options);
        return result.toPromise();
    }
    move(folderId, newParentFolderId, _options) {
        const result = this.api.move(folderId, newParentFolderId, _options);
        return result.toPromise();
    }
    moveListWithHttpInfo(listMoveRequest, _options) {
        const result = this.api.moveListWithHttpInfo(listMoveRequest, _options);
        return result.toPromise();
    }
    moveList(listMoveRequest, _options) {
        const result = this.api.moveList(listMoveRequest, _options);
        return result.toPromise();
    }
    removeWithHttpInfo(folderId, _options) {
        const result = this.api.removeWithHttpInfo(folderId, _options);
        return result.toPromise();
    }
    remove(folderId, _options) {
        const result = this.api.remove(folderId, _options);
        return result.toPromise();
    }
    renameWithHttpInfo(folderId, newFolderName, _options) {
        const result = this.api.renameWithHttpInfo(folderId, newFolderName, _options);
        return result.toPromise();
    }
    rename(folderId, newFolderName, _options) {
        const result = this.api.rename(folderId, newFolderName, _options);
        return result.toPromise();
    }
}
exports.PromiseFoldersApi = PromiseFoldersApi;
const ObservableAPI_2 = require("./ObservableAPI");
class PromiseListsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_2.ObservableListsApi(configuration, requestFactory, responseProcessor);
    }
    createWithHttpInfo(listCreateRequest, _options) {
        const result = this.api.createWithHttpInfo(listCreateRequest, _options);
        return result.toPromise();
    }
    create(listCreateRequest, _options) {
        const result = this.api.create(listCreateRequest, _options);
        return result.toPromise();
    }
    doSearchWithHttpInfo(listSearchRequest, _options) {
        const result = this.api.doSearchWithHttpInfo(listSearchRequest, _options);
        return result.toPromise();
    }
    doSearch(listSearchRequest, _options) {
        const result = this.api.doSearch(listSearchRequest, _options);
        return result.toPromise();
    }
    getAllWithHttpInfo(listIds, includeFilters, _options) {
        const result = this.api.getAllWithHttpInfo(listIds, includeFilters, _options);
        return result.toPromise();
    }
    getAll(listIds, includeFilters, _options) {
        const result = this.api.getAll(listIds, includeFilters, _options);
        return result.toPromise();
    }
    getByIdWithHttpInfo(listId, includeFilters, _options) {
        const result = this.api.getByIdWithHttpInfo(listId, includeFilters, _options);
        return result.toPromise();
    }
    getById(listId, includeFilters, _options) {
        const result = this.api.getById(listId, includeFilters, _options);
        return result.toPromise();
    }
    getByNameWithHttpInfo(listName, objectTypeId, includeFilters, _options) {
        const result = this.api.getByNameWithHttpInfo(listName, objectTypeId, includeFilters, _options);
        return result.toPromise();
    }
    getByName(listName, objectTypeId, includeFilters, _options) {
        const result = this.api.getByName(listName, objectTypeId, includeFilters, _options);
        return result.toPromise();
    }
    removeWithHttpInfo(listId, _options) {
        const result = this.api.removeWithHttpInfo(listId, _options);
        return result.toPromise();
    }
    remove(listId, _options) {
        const result = this.api.remove(listId, _options);
        return result.toPromise();
    }
    restoreWithHttpInfo(listId, _options) {
        const result = this.api.restoreWithHttpInfo(listId, _options);
        return result.toPromise();
    }
    restore(listId, _options) {
        const result = this.api.restore(listId, _options);
        return result.toPromise();
    }
    updateListFiltersWithHttpInfo(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options) {
        const result = this.api.updateListFiltersWithHttpInfo(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options);
        return result.toPromise();
    }
    updateListFilters(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options) {
        const result = this.api.updateListFilters(listId, listFilterUpdateRequest, enrollObjectsInWorkflows, _options);
        return result.toPromise();
    }
    updateNameWithHttpInfo(listId, listName, includeFilters, _options) {
        const result = this.api.updateNameWithHttpInfo(listId, listName, includeFilters, _options);
        return result.toPromise();
    }
    updateName(listId, listName, includeFilters, _options) {
        const result = this.api.updateName(listId, listName, includeFilters, _options);
        return result.toPromise();
    }
}
exports.PromiseListsApi = PromiseListsApi;
const ObservableAPI_3 = require("./ObservableAPI");
class PromiseMappingApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_3.ObservableMappingApi(configuration, requestFactory, responseProcessor);
    }
    translateLegacyListIdToListIdWithHttpInfo(legacyListId, _options) {
        const result = this.api.translateLegacyListIdToListIdWithHttpInfo(legacyListId, _options);
        return result.toPromise();
    }
    translateLegacyListIdToListId(legacyListId, _options) {
        const result = this.api.translateLegacyListIdToListId(legacyListId, _options);
        return result.toPromise();
    }
    translateLegacyListIdToListIdBatchWithHttpInfo(requestBody, _options) {
        const result = this.api.translateLegacyListIdToListIdBatchWithHttpInfo(requestBody, _options);
        return result.toPromise();
    }
    translateLegacyListIdToListIdBatch(requestBody, _options) {
        const result = this.api.translateLegacyListIdToListIdBatch(requestBody, _options);
        return result.toPromise();
    }
}
exports.PromiseMappingApi = PromiseMappingApi;
const ObservableAPI_4 = require("./ObservableAPI");
class PromiseMembershipsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_4.ObservableMembershipsApi(configuration, requestFactory, responseProcessor);
    }
    addWithHttpInfo(listId, requestBody, _options) {
        const result = this.api.addWithHttpInfo(listId, requestBody, _options);
        return result.toPromise();
    }
    add(listId, requestBody, _options) {
        const result = this.api.add(listId, requestBody, _options);
        return result.toPromise();
    }
    addAllFromListWithHttpInfo(listId, sourceListId, _options) {
        const result = this.api.addAllFromListWithHttpInfo(listId, sourceListId, _options);
        return result.toPromise();
    }
    addAllFromList(listId, sourceListId, _options) {
        const result = this.api.addAllFromList(listId, sourceListId, _options);
        return result.toPromise();
    }
    addAndRemoveWithHttpInfo(listId, membershipChangeRequest, _options) {
        const result = this.api.addAndRemoveWithHttpInfo(listId, membershipChangeRequest, _options);
        return result.toPromise();
    }
    addAndRemove(listId, membershipChangeRequest, _options) {
        const result = this.api.addAndRemove(listId, membershipChangeRequest, _options);
        return result.toPromise();
    }
    getListsWithHttpInfo(objectTypeId, recordId, _options) {
        const result = this.api.getListsWithHttpInfo(objectTypeId, recordId, _options);
        return result.toPromise();
    }
    getLists(objectTypeId, recordId, _options) {
        const result = this.api.getLists(objectTypeId, recordId, _options);
        return result.toPromise();
    }
    getPageWithHttpInfo(listId, after, before, limit, _options) {
        const result = this.api.getPageWithHttpInfo(listId, after, before, limit, _options);
        return result.toPromise();
    }
    getPage(listId, after, before, limit, _options) {
        const result = this.api.getPage(listId, after, before, limit, _options);
        return result.toPromise();
    }
    getPageOrderedByAddedToListDateWithHttpInfo(listId, after, before, limit, _options) {
        const result = this.api.getPageOrderedByAddedToListDateWithHttpInfo(listId, after, before, limit, _options);
        return result.toPromise();
    }
    getPageOrderedByAddedToListDate(listId, after, before, limit, _options) {
        const result = this.api.getPageOrderedByAddedToListDate(listId, after, before, limit, _options);
        return result.toPromise();
    }
    removeWithHttpInfo(listId, requestBody, _options) {
        const result = this.api.removeWithHttpInfo(listId, requestBody, _options);
        return result.toPromise();
    }
    remove(listId, requestBody, _options) {
        const result = this.api.remove(listId, requestBody, _options);
        return result.toPromise();
    }
    removeAllWithHttpInfo(listId, _options) {
        const result = this.api.removeAllWithHttpInfo(listId, _options);
        return result.toPromise();
    }
    removeAll(listId, _options) {
        const result = this.api.removeAll(listId, _options);
        return result.toPromise();
    }
}
exports.PromiseMembershipsApi = PromiseMembershipsApi;
//# sourceMappingURL=PromiseAPI.js.map