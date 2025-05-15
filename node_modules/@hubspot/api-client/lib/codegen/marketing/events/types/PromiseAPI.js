"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseSubscriberStateChangesApi = exports.PromiseSettingsApi = exports.PromiseParticipantStateApi = exports.PromiseBasicApi = exports.PromiseAttendanceSubscriberStateChangesApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class PromiseAttendanceSubscriberStateChangesApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableAttendanceSubscriberStateChangesApi(configuration, requestFactory, responseProcessor);
    }
    recordByContactEmailsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options) {
        const result = this.api.recordByContactEmailsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options);
        return result.toPromise();
    }
    recordByContactEmails(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options) {
        const result = this.api.recordByContactEmails(externalEventId, subscriberState, batchInputMarketingEventEmailSubscriber, externalAccountId, _options);
        return result.toPromise();
    }
    recordByContactIdsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options) {
        const result = this.api.recordByContactIdsWithHttpInfo(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options);
        return result.toPromise();
    }
    recordByContactIds(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options) {
        const result = this.api.recordByContactIds(externalEventId, subscriberState, batchInputMarketingEventSubscriber, externalAccountId, _options);
        return result.toPromise();
    }
}
exports.PromiseAttendanceSubscriberStateChangesApi = PromiseAttendanceSubscriberStateChangesApi;
const ObservableAPI_2 = require("./ObservableAPI");
class PromiseBasicApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_2.ObservableBasicApi(configuration, requestFactory, responseProcessor);
    }
    archiveWithHttpInfo(externalEventId, externalAccountId, _options) {
        const result = this.api.archiveWithHttpInfo(externalEventId, externalAccountId, _options);
        return result.toPromise();
    }
    archive(externalEventId, externalAccountId, _options) {
        const result = this.api.archive(externalEventId, externalAccountId, _options);
        return result.toPromise();
    }
    batchArchiveWithHttpInfo(batchInputMarketingEventExternalUniqueIdentifier, _options) {
        const result = this.api.batchArchiveWithHttpInfo(batchInputMarketingEventExternalUniqueIdentifier, _options);
        return result.toPromise();
    }
    batchArchive(batchInputMarketingEventExternalUniqueIdentifier, _options) {
        const result = this.api.batchArchive(batchInputMarketingEventExternalUniqueIdentifier, _options);
        return result.toPromise();
    }
    batchUpsertWithHttpInfo(batchInputMarketingEventCreateRequestParams, _options) {
        const result = this.api.batchUpsertWithHttpInfo(batchInputMarketingEventCreateRequestParams, _options);
        return result.toPromise();
    }
    batchUpsert(batchInputMarketingEventCreateRequestParams, _options) {
        const result = this.api.batchUpsert(batchInputMarketingEventCreateRequestParams, _options);
        return result.toPromise();
    }
    cancelWithHttpInfo(externalEventId, externalAccountId, _options) {
        const result = this.api.cancelWithHttpInfo(externalEventId, externalAccountId, _options);
        return result.toPromise();
    }
    cancel(externalEventId, externalAccountId, _options) {
        const result = this.api.cancel(externalEventId, externalAccountId, _options);
        return result.toPromise();
    }
    completeWithHttpInfo(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options) {
        const result = this.api.completeWithHttpInfo(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options);
        return result.toPromise();
    }
    complete(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options) {
        const result = this.api.complete(externalEventId, externalAccountId, marketingEventCompleteRequestParams, _options);
        return result.toPromise();
    }
    createWithHttpInfo(marketingEventCreateRequestParams, _options) {
        const result = this.api.createWithHttpInfo(marketingEventCreateRequestParams, _options);
        return result.toPromise();
    }
    create(marketingEventCreateRequestParams, _options) {
        const result = this.api.create(marketingEventCreateRequestParams, _options);
        return result.toPromise();
    }
    doSearchWithHttpInfo(q, _options) {
        const result = this.api.doSearchWithHttpInfo(q, _options);
        return result.toPromise();
    }
    doSearch(q, _options) {
        const result = this.api.doSearch(q, _options);
        return result.toPromise();
    }
    getDetailsWithHttpInfo(externalEventId, externalAccountId, _options) {
        const result = this.api.getDetailsWithHttpInfo(externalEventId, externalAccountId, _options);
        return result.toPromise();
    }
    getDetails(externalEventId, externalAccountId, _options) {
        const result = this.api.getDetails(externalEventId, externalAccountId, _options);
        return result.toPromise();
    }
    updateWithHttpInfo(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options) {
        const result = this.api.updateWithHttpInfo(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options);
        return result.toPromise();
    }
    update(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options) {
        const result = this.api.update(externalEventId, externalAccountId, marketingEventUpdateRequestParams, _options);
        return result.toPromise();
    }
    upsertWithHttpInfo(externalEventId, marketingEventCreateRequestParams, _options) {
        const result = this.api.upsertWithHttpInfo(externalEventId, marketingEventCreateRequestParams, _options);
        return result.toPromise();
    }
    upsert(externalEventId, marketingEventCreateRequestParams, _options) {
        const result = this.api.upsert(externalEventId, marketingEventCreateRequestParams, _options);
        return result.toPromise();
    }
}
exports.PromiseBasicApi = PromiseBasicApi;
const ObservableAPI_3 = require("./ObservableAPI");
class PromiseParticipantStateApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_3.ObservableParticipantStateApi(configuration, requestFactory, responseProcessor);
    }
    getParticipationsBreakdownByContactIdWithHttpInfo(contactIdentifier, state, limit, after, _options) {
        const result = this.api.getParticipationsBreakdownByContactIdWithHttpInfo(contactIdentifier, state, limit, after, _options);
        return result.toPromise();
    }
    getParticipationsBreakdownByContactId(contactIdentifier, state, limit, after, _options) {
        const result = this.api.getParticipationsBreakdownByContactId(contactIdentifier, state, limit, after, _options);
        return result.toPromise();
    }
    getParticipationsBreakdownByExternalEventIdWithHttpInfo(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options) {
        const result = this.api.getParticipationsBreakdownByExternalEventIdWithHttpInfo(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options);
        return result.toPromise();
    }
    getParticipationsBreakdownByExternalEventId(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options) {
        const result = this.api.getParticipationsBreakdownByExternalEventId(externalAccountId, externalEventId, contactIdentifier, state, limit, after, _options);
        return result.toPromise();
    }
    getParticipationsBreakdownByMarketingEventIdWithHttpInfo(marketingEventId, contactIdentifier, state, limit, after, _options) {
        const result = this.api.getParticipationsBreakdownByMarketingEventIdWithHttpInfo(marketingEventId, contactIdentifier, state, limit, after, _options);
        return result.toPromise();
    }
    getParticipationsBreakdownByMarketingEventId(marketingEventId, contactIdentifier, state, limit, after, _options) {
        const result = this.api.getParticipationsBreakdownByMarketingEventId(marketingEventId, contactIdentifier, state, limit, after, _options);
        return result.toPromise();
    }
    getParticipationsCountersByEventExternalIdWithHttpInfo(externalAccountId, externalEventId, _options) {
        const result = this.api.getParticipationsCountersByEventExternalIdWithHttpInfo(externalAccountId, externalEventId, _options);
        return result.toPromise();
    }
    getParticipationsCountersByEventExternalId(externalAccountId, externalEventId, _options) {
        const result = this.api.getParticipationsCountersByEventExternalId(externalAccountId, externalEventId, _options);
        return result.toPromise();
    }
    getParticipationsCountersByMarketingEventIdWithHttpInfo(marketingEventId, _options) {
        const result = this.api.getParticipationsCountersByMarketingEventIdWithHttpInfo(marketingEventId, _options);
        return result.toPromise();
    }
    getParticipationsCountersByMarketingEventId(marketingEventId, _options) {
        const result = this.api.getParticipationsCountersByMarketingEventId(marketingEventId, _options);
        return result.toPromise();
    }
}
exports.PromiseParticipantStateApi = PromiseParticipantStateApi;
const ObservableAPI_4 = require("./ObservableAPI");
class PromiseSettingsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_4.ObservableSettingsApi(configuration, requestFactory, responseProcessor);
    }
    getAllWithHttpInfo(appId, _options) {
        const result = this.api.getAllWithHttpInfo(appId, _options);
        return result.toPromise();
    }
    getAll(appId, _options) {
        const result = this.api.getAll(appId, _options);
        return result.toPromise();
    }
    updateWithHttpInfo(appId, eventDetailSettingsUrl, _options) {
        const result = this.api.updateWithHttpInfo(appId, eventDetailSettingsUrl, _options);
        return result.toPromise();
    }
    update(appId, eventDetailSettingsUrl, _options) {
        const result = this.api.update(appId, eventDetailSettingsUrl, _options);
        return result.toPromise();
    }
}
exports.PromiseSettingsApi = PromiseSettingsApi;
const ObservableAPI_5 = require("./ObservableAPI");
class PromiseSubscriberStateChangesApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_5.ObservableSubscriberStateChangesApi(configuration, requestFactory, responseProcessor);
    }
    upsertByContactEmailWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options) {
        const result = this.api.upsertByContactEmailWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options);
        return result.toPromise();
    }
    upsertByContactEmail(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options) {
        const result = this.api.upsertByContactEmail(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventEmailSubscriber, _options);
        return result.toPromise();
    }
    upsertByContactIdWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options) {
        const result = this.api.upsertByContactIdWithHttpInfo(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options);
        return result.toPromise();
    }
    upsertByContactId(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options) {
        const result = this.api.upsertByContactId(externalEventId, subscriberState, externalAccountId, batchInputMarketingEventSubscriber, _options);
        return result.toPromise();
    }
}
exports.PromiseSubscriberStateChangesApi = PromiseSubscriberStateChangesApi;
//# sourceMappingURL=PromiseAPI.js.map