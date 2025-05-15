"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ObservableSubscriberStateChangesApi = exports.ObservableSettingsApi = exports.ObservableParticipantStateApi = exports.ObservableBasicApi = exports.ObservableAttendanceSubscriberStateChangesApi = void 0;
const rxjsStub_1 = require("../rxjsStub");
const rxjsStub_2 = require("../rxjsStub");
const AttendanceSubscriberStateChangesApi_1 = require("../apis/AttendanceSubscriberStateChangesApi");
class ObservableAttendanceSubscriberStateChangesApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new AttendanceSubscriberStateChangesApi_1.AttendanceSubscriberStateChangesApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new AttendanceSubscriberStateChangesApi_1.AttendanceSubscriberStateChangesApiResponseProcessor();
    }
    recordByContactEmailsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options) {
        const requestContextPromise = this.requestFactory.recordByContactEmails(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.recordByContactEmailsWithHttpInfo(rsp)));
        }));
    }
    recordByContactEmails(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options) {
        return this.recordByContactEmailsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    recordByContactIdsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options) {
        const requestContextPromise = this.requestFactory.recordByContactIds(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.recordByContactIdsWithHttpInfo(rsp)));
        }));
    }
    recordByContactIds(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options) {
        return this.recordByContactIdsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableAttendanceSubscriberStateChangesApi = ObservableAttendanceSubscriberStateChangesApi;
const BasicApi_1 = require("../apis/BasicApi");
class ObservableBasicApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new BasicApi_1.BasicApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new BasicApi_1.BasicApiResponseProcessor();
    }
    archiveWithHttpInfo(externalEventId, externalAccountId, _options) {
        const requestContextPromise = this.requestFactory.archive(externalEventId, externalAccountId, _options);
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
    archive(externalEventId, externalAccountId, _options) {
        return this.archiveWithHttpInfo(externalEventId, externalAccountId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    batchArchiveWithHttpInfo(batchInputMarketingEventExternalUniqueIdentifier, _options) {
        const requestContextPromise = this.requestFactory.batchArchive(batchInputMarketingEventExternalUniqueIdentifier, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.batchArchiveWithHttpInfo(rsp)));
        }));
    }
    batchArchive(batchInputMarketingEventExternalUniqueIdentifier, _options) {
        return this.batchArchiveWithHttpInfo(batchInputMarketingEventExternalUniqueIdentifier, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    batchUpsertWithHttpInfo(batchInputMarketingEventCreateRequestParams, _options) {
        const requestContextPromise = this.requestFactory.batchUpsert(batchInputMarketingEventCreateRequestParams, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.batchUpsertWithHttpInfo(rsp)));
        }));
    }
    batchUpsert(batchInputMarketingEventCreateRequestParams, _options) {
        return this.batchUpsertWithHttpInfo(batchInputMarketingEventCreateRequestParams, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    cancelWithHttpInfo(externalEventId, externalAccountId, _options) {
        const requestContextPromise = this.requestFactory.cancel(externalEventId, externalAccountId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.cancelWithHttpInfo(rsp)));
        }));
    }
    cancel(externalEventId, externalAccountId, _options) {
        return this.cancelWithHttpInfo(externalEventId, externalAccountId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    completeWithHttpInfo(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options) {
        const requestContextPromise = this.requestFactory.complete(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.completeWithHttpInfo(rsp)));
        }));
    }
    complete(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options) {
        return this.completeWithHttpInfo(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    createWithHttpInfo(marketingEventCreateRequestParams, _options) {
        const requestContextPromise = this.requestFactory.create(marketingEventCreateRequestParams, _options);
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
    create(marketingEventCreateRequestParams, _options) {
        return this.createWithHttpInfo(marketingEventCreateRequestParams, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    doSearchWithHttpInfo(q, _options) {
        const requestContextPromise = this.requestFactory.doSearch(q, _options);
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
    doSearch(q, _options) {
        return this.doSearchWithHttpInfo(q, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getDetailsWithHttpInfo(externalEventId, externalAccountId, _options) {
        const requestContextPromise = this.requestFactory.getDetails(externalEventId, externalAccountId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getDetailsWithHttpInfo(rsp)));
        }));
    }
    getDetails(externalEventId, externalAccountId, _options) {
        return this.getDetailsWithHttpInfo(externalEventId, externalAccountId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    updateWithHttpInfo(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options) {
        const requestContextPromise = this.requestFactory.update(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options);
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
    update(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options) {
        return this.updateWithHttpInfo(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    upsertWithHttpInfo(externalEventId, marketingEventCreateRequestParams, _options) {
        const requestContextPromise = this.requestFactory.upsert(externalEventId, marketingEventCreateRequestParams, _options);
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
    upsert(externalEventId, marketingEventCreateRequestParams, _options) {
        return this.upsertWithHttpInfo(externalEventId, marketingEventCreateRequestParams, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableBasicApi = ObservableBasicApi;
const ParticipantStateApi_1 = require("../apis/ParticipantStateApi");
class ObservableParticipantStateApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new ParticipantStateApi_1.ParticipantStateApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new ParticipantStateApi_1.ParticipantStateApiResponseProcessor();
    }
    getParticipationsBreakdownByContactIdWithHttpInfo(contactIdentifier, state, limit, after, _options) {
        const requestContextPromise = this.requestFactory.getParticipationsBreakdownByContactId(contactIdentifier, state, limit, after, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getParticipationsBreakdownByContactIdWithHttpInfo(rsp)));
        }));
    }
    getParticipationsBreakdownByContactId(contactIdentifier, state, limit, after, _options) {
        return this.getParticipationsBreakdownByContactIdWithHttpInfo(contactIdentifier, state, limit, after, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getParticipationsBreakdownByExternalEventIdWithHttpInfo(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options) {
        const requestContextPromise = this.requestFactory.getParticipationsBreakdownByExternalEventId(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getParticipationsBreakdownByExternalEventIdWithHttpInfo(rsp)));
        }));
    }
    getParticipationsBreakdownByExternalEventId(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options) {
        return this.getParticipationsBreakdownByExternalEventIdWithHttpInfo(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getParticipationsBreakdownByMarketingEventIdWithHttpInfo(marketingEventId, contactIdentifier, state, limit, after, _options) {
        const requestContextPromise = this.requestFactory.getParticipationsBreakdownByMarketingEventId(marketingEventId, contactIdentifier, state, limit, after, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getParticipationsBreakdownByMarketingEventIdWithHttpInfo(rsp)));
        }));
    }
    getParticipationsBreakdownByMarketingEventId(marketingEventId, contactIdentifier, state, limit, after, _options) {
        return this.getParticipationsBreakdownByMarketingEventIdWithHttpInfo(marketingEventId, contactIdentifier, state, limit, after, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getParticipationsCountersByEventExternalIdWithHttpInfo(externalAccountId, externalEventId, _options) {
        const requestContextPromise = this.requestFactory.getParticipationsCountersByEventExternalId(externalAccountId, externalEventId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getParticipationsCountersByEventExternalIdWithHttpInfo(rsp)));
        }));
    }
    getParticipationsCountersByEventExternalId(externalAccountId, externalEventId, _options) {
        return this.getParticipationsCountersByEventExternalIdWithHttpInfo(externalAccountId, externalEventId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    getParticipationsCountersByMarketingEventIdWithHttpInfo(marketingEventId, _options) {
        const requestContextPromise = this.requestFactory.getParticipationsCountersByMarketingEventId(marketingEventId, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.getParticipationsCountersByMarketingEventIdWithHttpInfo(rsp)));
        }));
    }
    getParticipationsCountersByMarketingEventId(marketingEventId, _options) {
        return this.getParticipationsCountersByMarketingEventIdWithHttpInfo(marketingEventId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableParticipantStateApi = ObservableParticipantStateApi;
const SettingsApi_1 = require("../apis/SettingsApi");
class ObservableSettingsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new SettingsApi_1.SettingsApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new SettingsApi_1.SettingsApiResponseProcessor();
    }
    getAllWithHttpInfo(appId, _options) {
        const requestContextPromise = this.requestFactory.getAll(appId, _options);
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
    getAll(appId, _options) {
        return this.getAllWithHttpInfo(appId, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    updateWithHttpInfo(appId, eventDetailSettingsUrl, _options) {
        const requestContextPromise = this.requestFactory.update(appId, eventDetailSettingsUrl, _options);
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
    update(appId, eventDetailSettingsUrl, _options) {
        return this.updateWithHttpInfo(appId, eventDetailSettingsUrl, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableSettingsApi = ObservableSettingsApi;
const SubscriberStateChangesApi_1 = require("../apis/SubscriberStateChangesApi");
class ObservableSubscriberStateChangesApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.configuration = configuration;
        this.requestFactory = requestFactory || new SubscriberStateChangesApi_1.SubscriberStateChangesApiRequestFactory(configuration);
        this.responseProcessor = responseProcessor || new SubscriberStateChangesApi_1.SubscriberStateChangesApiResponseProcessor();
    }
    upsertByContactEmailWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options) {
        const requestContextPromise = this.requestFactory.upsertByContactEmail(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.upsertByContactEmailWithHttpInfo(rsp)));
        }));
    }
    upsertByContactEmail(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options) {
        return this.upsertByContactEmailWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
    upsertByContactIdWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options) {
        const requestContextPromise = this.requestFactory.upsertByContactId(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options);
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
            return middlewarePostObservable.pipe((0, rxjsStub_2.map)((rsp) => this.responseProcessor.upsertByContactIdWithHttpInfo(rsp)));
        }));
    }
    upsertByContactId(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options) {
        return this.upsertByContactIdWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options).pipe((0, rxjsStub_2.map)((apiResponse) => apiResponse.data));
    }
}
exports.ObservableSubscriberStateChangesApi = ObservableSubscriberStateChangesApi;
//# sourceMappingURL=ObservableAPI.js.map