"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseCustomEventDataApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class PromiseCustomEventDataApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableCustomEventDataApi(configuration, requestFactory, responseProcessor);
    }
    sendWithHttpInfo(behavioralEventHttpCompletionRequest, _options) {
        const result = this.api.sendWithHttpInfo(behavioralEventHttpCompletionRequest, _options);
        return result.toPromise();
    }
    send(behavioralEventHttpCompletionRequest, _options) {
        const result = this.api.send(behavioralEventHttpCompletionRequest, _options);
        return result.toPromise();
    }
}
exports.PromiseCustomEventDataApi = PromiseCustomEventDataApi;
//# sourceMappingURL=PromiseAPI.js.map