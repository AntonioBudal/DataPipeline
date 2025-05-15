import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { AttendanceCounters } from '../models/AttendanceCounters';
import { BatchInputMarketingEventCreateRequestParams } from '../models/BatchInputMarketingEventCreateRequestParams';
import { BatchInputMarketingEventEmailSubscriber } from '../models/BatchInputMarketingEventEmailSubscriber';
import { BatchInputMarketingEventExternalUniqueIdentifier } from '../models/BatchInputMarketingEventExternalUniqueIdentifier';
import { BatchInputMarketingEventSubscriber } from '../models/BatchInputMarketingEventSubscriber';
import { BatchResponseMarketingEventPublicDefaultResponse } from '../models/BatchResponseMarketingEventPublicDefaultResponse';
import { BatchResponseSubscriberEmailResponse } from '../models/BatchResponseSubscriberEmailResponse';
import { BatchResponseSubscriberVidResponse } from '../models/BatchResponseSubscriberVidResponse';
import { CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging } from '../models/CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging';
import { CollectionResponseWithTotalParticipationBreakdownForwardPaging } from '../models/CollectionResponseWithTotalParticipationBreakdownForwardPaging';
import { EventDetailSettings } from '../models/EventDetailSettings';
import { EventDetailSettingsUrl } from '../models/EventDetailSettingsUrl';
import { MarketingEventCompleteRequestParams } from '../models/MarketingEventCompleteRequestParams';
import { MarketingEventCreateRequestParams } from '../models/MarketingEventCreateRequestParams';
import { MarketingEventDefaultResponse } from '../models/MarketingEventDefaultResponse';
import { MarketingEventPublicDefaultResponse } from '../models/MarketingEventPublicDefaultResponse';
import { MarketingEventPublicReadResponse } from '../models/MarketingEventPublicReadResponse';
import { MarketingEventUpdateRequestParams } from '../models/MarketingEventUpdateRequestParams';
import { AttendanceSubscriberStateChangesApiRequestFactory, AttendanceSubscriberStateChangesApiResponseProcessor } from "../apis/AttendanceSubscriberStateChangesApi";
export declare class PromiseAttendanceSubscriberStateChangesApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: AttendanceSubscriberStateChangesApiRequestFactory, responseProcessor?: AttendanceSubscriberStateChangesApiResponseProcessor);
    recordByContactEmailsWithHttpInfo(externalEventId: string, subscriberState: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, externalAccountId?: string, _options?: Configuration): Promise<HttpInfo<BatchResponseSubscriberEmailResponse>>;
    recordByContactEmails(externalEventId: string, subscriberState: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, externalAccountId?: string, _options?: Configuration): Promise<BatchResponseSubscriberEmailResponse>;
    recordByContactIdsWithHttpInfo(externalEventId: string, subscriberState: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, externalAccountId?: string, _options?: Configuration): Promise<HttpInfo<BatchResponseSubscriberVidResponse>>;
    recordByContactIds(externalEventId: string, subscriberState: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, externalAccountId?: string, _options?: Configuration): Promise<BatchResponseSubscriberVidResponse>;
}
import { BasicApiRequestFactory, BasicApiResponseProcessor } from "../apis/BasicApi";
export declare class PromiseBasicApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BasicApiRequestFactory, responseProcessor?: BasicApiResponseProcessor);
    archiveWithHttpInfo(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<HttpInfo<void>>;
    archive(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<void>;
    batchArchiveWithHttpInfo(batchInputMarketingEventExternalUniqueIdentifier: BatchInputMarketingEventExternalUniqueIdentifier, _options?: Configuration): Promise<HttpInfo<void>>;
    batchArchive(batchInputMarketingEventExternalUniqueIdentifier: BatchInputMarketingEventExternalUniqueIdentifier, _options?: Configuration): Promise<void>;
    batchUpsertWithHttpInfo(batchInputMarketingEventCreateRequestParams: BatchInputMarketingEventCreateRequestParams, _options?: Configuration): Promise<HttpInfo<BatchResponseMarketingEventPublicDefaultResponse>>;
    batchUpsert(batchInputMarketingEventCreateRequestParams: BatchInputMarketingEventCreateRequestParams, _options?: Configuration): Promise<BatchResponseMarketingEventPublicDefaultResponse>;
    cancelWithHttpInfo(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    cancel(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<MarketingEventDefaultResponse>;
    completeWithHttpInfo(externalEventId: string, externalAccountId: string, marketingEventCompleteRequestParams: MarketingEventCompleteRequestParams, _options?: Configuration): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    complete(externalEventId: string, externalAccountId: string, marketingEventCompleteRequestParams: MarketingEventCompleteRequestParams, _options?: Configuration): Promise<MarketingEventDefaultResponse>;
    createWithHttpInfo(marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    create(marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Promise<MarketingEventDefaultResponse>;
    doSearchWithHttpInfo(q: string, _options?: Configuration): Promise<HttpInfo<CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging>>;
    doSearch(q: string, _options?: Configuration): Promise<CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging>;
    getDetailsWithHttpInfo(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<HttpInfo<MarketingEventPublicReadResponse>>;
    getDetails(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<MarketingEventPublicReadResponse>;
    updateWithHttpInfo(externalEventId: string, externalAccountId: string, marketingEventUpdateRequestParams: MarketingEventUpdateRequestParams, _options?: Configuration): Promise<HttpInfo<MarketingEventPublicDefaultResponse>>;
    update(externalEventId: string, externalAccountId: string, marketingEventUpdateRequestParams: MarketingEventUpdateRequestParams, _options?: Configuration): Promise<MarketingEventPublicDefaultResponse>;
    upsertWithHttpInfo(externalEventId: string, marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Promise<HttpInfo<MarketingEventPublicDefaultResponse>>;
    upsert(externalEventId: string, marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Promise<MarketingEventPublicDefaultResponse>;
}
import { ParticipantStateApiRequestFactory, ParticipantStateApiResponseProcessor } from "../apis/ParticipantStateApi";
export declare class PromiseParticipantStateApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: ParticipantStateApiRequestFactory, responseProcessor?: ParticipantStateApiResponseProcessor);
    getParticipationsBreakdownByContactIdWithHttpInfo(contactIdentifier: string, state?: string, limit?: number, after?: string, _options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByContactId(contactIdentifier: string, state?: string, limit?: number, after?: string, _options?: Configuration): Promise<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsBreakdownByExternalEventIdWithHttpInfo(externalAccountId: string, externalEventId: string, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByExternalEventId(externalAccountId: string, externalEventId: string, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Promise<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsBreakdownByMarketingEventIdWithHttpInfo(marketingEventId: number, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByMarketingEventId(marketingEventId: number, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Promise<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsCountersByEventExternalIdWithHttpInfo(externalAccountId: string, externalEventId: string, _options?: Configuration): Promise<HttpInfo<AttendanceCounters>>;
    getParticipationsCountersByEventExternalId(externalAccountId: string, externalEventId: string, _options?: Configuration): Promise<AttendanceCounters>;
    getParticipationsCountersByMarketingEventIdWithHttpInfo(marketingEventId: number, _options?: Configuration): Promise<HttpInfo<AttendanceCounters>>;
    getParticipationsCountersByMarketingEventId(marketingEventId: number, _options?: Configuration): Promise<AttendanceCounters>;
}
import { SettingsApiRequestFactory, SettingsApiResponseProcessor } from "../apis/SettingsApi";
export declare class PromiseSettingsApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SettingsApiRequestFactory, responseProcessor?: SettingsApiResponseProcessor);
    getAllWithHttpInfo(appId: number, _options?: Configuration): Promise<HttpInfo<EventDetailSettings>>;
    getAll(appId: number, _options?: Configuration): Promise<EventDetailSettings>;
    updateWithHttpInfo(appId: number, eventDetailSettingsUrl: EventDetailSettingsUrl, _options?: Configuration): Promise<HttpInfo<EventDetailSettings>>;
    update(appId: number, eventDetailSettingsUrl: EventDetailSettingsUrl, _options?: Configuration): Promise<EventDetailSettings>;
}
import { SubscriberStateChangesApiRequestFactory, SubscriberStateChangesApiResponseProcessor } from "../apis/SubscriberStateChangesApi";
export declare class PromiseSubscriberStateChangesApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SubscriberStateChangesApiRequestFactory, responseProcessor?: SubscriberStateChangesApiResponseProcessor);
    upsertByContactEmailWithHttpInfo(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, _options?: Configuration): Promise<HttpInfo<void>>;
    upsertByContactEmail(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, _options?: Configuration): Promise<void>;
    upsertByContactIdWithHttpInfo(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, _options?: Configuration): Promise<HttpInfo<void>>;
    upsertByContactId(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, _options?: Configuration): Promise<void>;
}
