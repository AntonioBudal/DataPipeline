"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ObjectCustomEventDataApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class ObjectCustomEventDataApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableCustomEventDataApi(configuration, requestFactory, responseProcessor);
    }
    sendWithHttpInfo(param, options) {
        return this.api.sendWithHttpInfo(param.behavioralEventHttpCompletionRequest, options).toPromise();
    }
    send(param, options) {
        return this.api.send(param.behavioralEventHttpCompletionRequest, options).toPromise();
    }
}
exports.ObjectCustomEventDataApi = ObjectCustomEventDataApi;
//# sourceMappingURL=ObjectParamAPI.js.map