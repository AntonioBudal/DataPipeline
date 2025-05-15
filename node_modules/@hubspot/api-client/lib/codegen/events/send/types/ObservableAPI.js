"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ObservableCustomEventDataApi = void 0;
const rxjsStub_1 = require("../rxjsStub");
const rxjsStub_2 = require("../rxjsStub");
const CustomEventDataApi_1 = require("../apis/CustomEventDataApi");
class ObservableCustomEventDataApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new CustomEventDataApi_1.CustomEventDataApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new CustomEventDataApi_1.CustomEventDataApiResponseProcessor();
    }
    sendWithHttpInfo(behavioralEventHttpCompletionRequest, _options) {
        const requestContextPromise = this.requestFactory.send(behavioralEventHttpCompletionRequest, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.sendWithHttpInfo(rsp)));
        }));
    }
    send(behavioralEventHttpCompletionRequest, _options) {
        return this.sendWithHttpInfo(behavioralEventHttpCompletionRequest, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableCustomEventDataApi = ObservableCustomEventDataApi;
//# sourceMappingURL=ObservableAPI.js.map