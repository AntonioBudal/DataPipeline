"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseEventsApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class PromiseEventsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableEventsApi(configuration, requestFactory, responseProcessor);
    }
    getPageWithHttpInfo(objectType, eventType, after, before, limit, sort, occurredAfter, occurredBefore, objectId, objectPropertyPropname, propertyPropname, id, _options) {
        const result = this.api.getPageWithHttpInfo(objectType, eventType, after, before, limit, sort, occurredAfter, occurredBefore, objectId, objectPropertyPropname, propertyPropname, id, _options);
        return result.toPromise();
    }
    getPage(objectType, eventType, after, before, limit, sort, occurredAfter, occurredBefore, objectId, objectPropertyPropname, propertyPropname, id, _options) {
        const result = this.api.getPage(objectType, eventType, after, before, limit, sort, occurredAfter, occurredBefore, objectId, objectPropertyPropname, propertyPropname, id, _options);
        return result.toPromise();
    }
    getTypesWithHttpInfo(_options) {
        const result = this.api.getTypesWithHttpInfo(_options);
        return result.toPromise();
    }
    getTypes(_options) {
        const result = this.api.getTypes(_options);
        return result.toPromise();
    }
}
exports.PromiseEventsApi = PromiseEventsApi;
//# sourceMappingURL=PromiseAPI.js.map