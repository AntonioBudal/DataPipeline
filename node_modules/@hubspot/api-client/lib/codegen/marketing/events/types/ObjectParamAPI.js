"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ObjectSubscriberStateChangesApi = exports.ObjectSettingsApi = exports.ObjectParticipantStateApi = exports.ObjectBasicApi = exports.ObjectAttendanceSubscriberStateChangesApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class ObjectAttendanceSubscriberStateChangesApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableAttendanceSubscriberStateChangesApi(configuration, requestFactory, responseProcessor);
    }
    recordByContactEmailsWithHttpInfo(param, options) {
        return this.api.recordByContactEmailsWithHttpInfo(param.externalEventId, param.subscriberState, param.batchInputMarketingEventEmailSubscriber, param.externalAccountId, options).toPromise();
    }
    recordByContactEmails(param, options) {
        return this.api.recordByContactEmails(param.externalEventId, param.subscriberState, param.batchInputMarketingEventEmailSubscriber, param.externalAccountId, options).toPromise();
    }
    recordByContactIdsWithHttpInfo(param, options) {
        return this.api.recordByContactIdsWithHttpInfo(param.externalEventId, param.subscriberState, param.batchInputMarketingEventSubscriber, param.externalAccountId, options).toPromise();
    }
    recordByContactIds(param, options) {
        return this.api.recordByContactIds(param.externalEventId, param.subscriberState, param.batchInputMarketingEventSubscriber, param.externalAccountId, options).toPromise();
    }
}
exports.ObjectAttendanceSubscriberStateChangesApi = ObjectAttendanceSubscriberStateChangesApi;
const ObservableAPI_2 = require("./ObservableAPI");
class ObjectBasicApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_2.ObservableBasicApi(configuration, requestFactory, responseProcessor);
    }
    archiveWithHttpInfo(param, options) {
        return this.api.archiveWithHttpInfo(param.externalEventId, param.externalAccountId, options).toPromise();
    }
    archive(param, options) {
        return this.api.archive(param.externalEventId, param.externalAccountId, options).toPromise();
    }
    batchArchiveWithHttpInfo(param, options) {
        return this.api.batchArchiveWithHttpInfo(param.batchInputMarketingEventExternalUniqueIdentifier, options).toPromise();
    }
    batchArchive(param, options) {
        return this.api.batchArchive(param.batchInputMarketingEventExternalUniqueIdentifier, options).toPromise();
    }
    batchUpsertWithHttpInfo(param, options) {
        return this.api.batchUpsertWithHttpInfo(param.batchInputMarketingEventCreateRequestParams, options).toPromise();
    }
    batchUpsert(param, options) {
        return this.api.batchUpsert(param.batchInputMarketingEventCreateRequestParams, options).toPromise();
    }
    cancelWithHttpInfo(param, options) {
        return this.api.cancelWithHttpInfo(param.externalEventId, param.externalAccountId, options).toPromise();
    }
    cancel(param, options) {
        return this.api.cancel(param.externalEventId, param.externalAccountId, options).toPromise();
    }
    completeWithHttpInfo(param, options) {
        return this.api.completeWithHttpInfo(param.externalEventId, param.externalAccountId, param.marketingEventCompleteRequestParams, options).toPromise();
    }
    complete(param, options) {
        return this.api.complete(param.externalEventId, param.externalAccountId, param.marketingEventCompleteRequestParams, options).toPromise();
    }
    createWithHttpInfo(param, options) {
        return this.api.createWithHttpInfo(param.marketingEventCreateRequestParams, options).toPromise();
    }
    create(param, options) {
        return this.api.create(param.marketingEventCreateRequestParams, options).toPromise();
    }
    doSearchWithHttpInfo(param, options) {
        return this.api.doSearchWithHttpInfo(param.q, options).toPromise();
    }
    doSearch(param, options) {
        return this.api.doSearch(param.q, options).toPromise();
    }
    getDetailsWithHttpInfo(param, options) {
        return this.api.getDetailsWithHttpInfo(param.externalEventId, param.externalAccountId, options).toPromise();
    }
    getDetails(param, options) {
        return this.api.getDetails(param.externalEventId, param.externalAccountId, options).toPromise();
    }
    updateWithHttpInfo(param, options) {
        return this.api.updateWithHttpInfo(param.externalEventId, param.externalAccountId, param.marketingEventUpdateRequestParams, options).toPromise();
    }
    update(param, options) {
        return this.api.update(param.externalEventId, param.externalAccountId, param.marketingEventUpdateRequestParams, options).toPromise();
    }
    upsertWithHttpInfo(param, options) {
        return this.api.upsertWithHttpInfo(param.externalEventId, param.marketingEventCreateRequestParams, options).toPromise();
    }
    upsert(param, options) {
        return this.api.upsert(param.externalEventId, param.marketingEventCreateRequestParams, options).toPromise();
    }
}
exports.ObjectBasicApi = ObjectBasicApi;
const ObservableAPI_3 = require("./ObservableAPI");
class ObjectParticipantStateApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_3.ObservableParticipantStateApi(configuration, requestFactory, responseProcessor);
    }
    getParticipationsBreakdownByContactIdWithHttpInfo(param, options) {
        return this.api.getParticipationsBreakdownByContactIdWithHttpInfo(param.contactIdentifier, param.state, param.limit, param.after, options).toPromise();
    }
    getParticipationsBreakdownByContactId(param, options) {
        return this.api.getParticipationsBreakdownByContactId(param.contactIdentifier, param.state, param.limit, param.after, options).toPromise();
    }
    getParticipationsBreakdownByExternalEventIdWithHttpInfo(param, options) {
        return this.api.getParticipationsBreakdownByExternalEventIdWithHttpInfo(param.externalAccountId, param.externalEventId, param.contactIdentifier, param.state, param.limit, param.after, options).toPromise();
    }
    getParticipationsBreakdownByExternalEventId(param, options) {
        return this.api.getParticipationsBreakdownByExternalEventId(param.externalAccountId, param.externalEventId, param.contactIdentifier, param.state, param.limit, param.after, options).toPromise();
    }
    getParticipationsBreakdownByMarketingEventIdWithHttpInfo(param, options) {
        return this.api.getParticipationsBreakdownByMarketingEventIdWithHttpInfo(param.marketingEventId, param.contactIdentifier, param.state, param.limit, param.after, options).toPromise();
    }
    getParticipationsBreakdownByMarketingEventId(param, options) {
        return this.api.getParticipationsBreakdownByMarketingEventId(param.marketingEventId, param.contactIdentifier, param.state, param.limit, param.after, options).toPromise();
    }
    getParticipationsCountersByEventExternalIdWithHttpInfo(param, options) {
        return this.api.getParticipationsCountersByEventExternalIdWithHttpInfo(param.externalAccountId, param.externalEventId, options).toPromise();
    }
    getParticipationsCountersByEventExternalId(param, options) {
        return this.api.getParticipationsCountersByEventExternalId(param.externalAccountId, param.externalEventId, options).toPromise();
    }
    getParticipationsCountersByMarketingEventIdWithHttpInfo(param, options) {
        return this.api.getParticipationsCountersByMarketingEventIdWithHttpInfo(param.marketingEventId, options).toPromise();
    }
    getParticipationsCountersByMarketingEventId(param, options) {
        return this.api.getParticipationsCountersByMarketingEventId(param.marketingEventId, options).toPromise();
    }
}
exports.ObjectParticipantStateApi = ObjectParticipantStateApi;
const ObservableAPI_4 = require("./ObservableAPI");
class ObjectSettingsApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_4.ObservableSettingsApi(configuration, requestFactory, responseProcessor);
    }
    getAllWithHttpInfo(param, options) {
        return this.api.getAllWithHttpInfo(param.appId, options).toPromise();
    }
    getAll(param, options) {
        return this.api.getAll(param.appId, options).toPromise();
    }
    updateWithHttpInfo(param, options) {
        return this.api.updateWithHttpInfo(param.appId, param.eventDetailSettingsUrl, options).toPromise();
    }
    update(param, options) {
        return this.api.update(param.appId, param.eventDetailSettingsUrl, options).toPromise();
    }
}
exports.ObjectSettingsApi = ObjectSettingsApi;
const ObservableAPI_5 = require("./ObservableAPI");
class ObjectSubscriberStateChangesApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_5.ObservableSubscriberStateChangesApi(configuration, requestFactory, responseProcessor);
    }
    upsertByContactEmailWithHttpInfo(param, options) {
        return this.api.upsertByContactEmailWithHttpInfo(param.externalEventId, param.subscriberState, param.externalAccountId, param.batchInputMarketingEventEmailSubscriber, options).toPromise();
    }
    upsertByContactEmail(param, options) {
        return this.api.upsertByContactEmail(param.externalEventId, param.subscriberState, param.externalAccountId, param.batchInputMarketingEventEmailSubscriber, options).toPromise();
    }
    upsertByContactIdWithHttpInfo(param, options) {
        return this.api.upsertByContactIdWithHttpInfo(param.externalEventId, param.subscriberState, param.externalAccountId, param.batchInputMarketingEventSubscriber, options).toPromise();
    }
    upsertByContactId(param, options) {
        return this.api.upsertByContactId(param.externalEventId, param.subscriberState, param.externalAccountId, param.batchInputMarketingEventSubscriber, options).toPromise();
    }
}
exports.ObjectSubscriberStateChangesApi = ObjectSubscriberStateChangesApi;
//# sourceMappingURL=ObjectParamAPI.js.map