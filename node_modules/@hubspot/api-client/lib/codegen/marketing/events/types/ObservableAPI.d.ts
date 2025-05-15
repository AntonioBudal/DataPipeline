import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { Observable } from '../rxjsStub';
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
export declare class ObservableAttendanceSubscriberStateChangesApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: AttendanceSubscriberStateChangesApiRequestFactory, responseProcessor?: AttendanceSubscriberStateChangesApiResponseProcessor);
    recordByContactEmailsWithHttpInfo(externalEventId: string, subscriberState: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, externalAccountId?: string, _options?: Configuration): Observable<HttpInfo<BatchResponseSubscriberEmailResponse>>;
    recordByContactEmails(externalEventId: string, subscriberState: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, externalAccountId?: string, _options?: Configuration): Observable<BatchResponseSubscriberEmailResponse>;
    recordByContactIdsWithHttpInfo(externalEventId: string, subscriberState: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, externalAccountId?: string, _options?: Configuration): Observable<HttpInfo<BatchResponseSubscriberVidResponse>>;
    recordByContactIds(externalEventId: string, subscriberState: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, externalAccountId?: string, _options?: Configuration): Observable<BatchResponseSubscriberVidResponse>;
}
import { BasicApiRequestFactory, BasicApiResponseProcessor } from "../apis/BasicApi";
export declare class ObservableBasicApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: BasicApiRequestFactory, responseProcessor?: BasicApiResponseProcessor);
    archiveWithHttpInfo(externalEventId: string, externalAccountId: string, _options?: Configuration): Observable<HttpInfo<void>>;
    archive(externalEventId: string, externalAccountId: string, _options?: Configuration): Observable<void>;
    batchArchiveWithHttpInfo(batchInputMarketingEventExternalUniqueIdentifier: BatchInputMarketingEventExternalUniqueIdentifier, _options?: Configuration): Observable<HttpInfo<void>>;
    batchArchive(batchInputMarketingEventExternalUniqueIdentifier: BatchInputMarketingEventExternalUniqueIdentifier, _options?: Configuration): Observable<void>;
    batchUpsertWithHttpInfo(batchInputMarketingEventCreateRequestParams: BatchInputMarketingEventCreateRequestParams, _options?: Configuration): Observable<HttpInfo<BatchResponseMarketingEventPublicDefaultResponse>>;
    batchUpsert(batchInputMarketingEventCreateRequestParams: BatchInputMarketingEventCreateRequestParams, _options?: Configuration): Observable<BatchResponseMarketingEventPublicDefaultResponse>;
    cancelWithHttpInfo(externalEventId: string, externalAccountId: string, _options?: Configuration): Observable<HttpInfo<MarketingEventDefaultResponse>>;
    cancel(externalEventId: string, externalAccountId: string, _options?: Configuration): Observable<MarketingEventDefaultResponse>;
    completeWithHttpInfo(externalEventId: string, externalAccountId: string, marketingEventCompleteRequestParams: MarketingEventCompleteRequestParams, _options?: Configuration): Observable<HttpInfo<MarketingEventDefaultResponse>>;
    complete(externalEventId: string, externalAccountId: string, marketingEventCompleteRequestParams: MarketingEventCompleteRequestParams, _options?: Configuration): Observable<MarketingEventDefaultResponse>;
    createWithHttpInfo(marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Observable<HttpInfo<MarketingEventDefaultResponse>>;
    create(marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Observable<MarketingEventDefaultResponse>;
    doSearchWithHttpInfo(q: string, _options?: Configuration): Observable<HttpInfo<CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging>>;
    doSearch(q: string, _options?: Configuration): Observable<CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging>;
    getDetailsWithHttpInfo(externalEventId: string, externalAccountId: string, _options?: Configuration): Observable<HttpInfo<MarketingEventPublicReadResponse>>;
    getDetails(externalEventId: string, externalAccountId: string, _options?: Configuration): Observable<MarketingEventPublicReadResponse>;
    updateWithHttpInfo(externalEventId: string, externalAccountId: string, marketingEventUpdateRequestParams: MarketingEventUpdateRequestParams, _options?: Configuration): Observable<HttpInfo<MarketingEventPublicDefaultResponse>>;
    update(externalEventId: string, externalAccountId: string, marketingEventUpdateRequestParams: MarketingEventUpdateRequestParams, _options?: Configuration): Observable<MarketingEventPublicDefaultResponse>;
    upsertWithHttpInfo(externalEventId: string, marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Observable<HttpInfo<MarketingEventPublicDefaultResponse>>;
    upsert(externalEventId: string, marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Observable<MarketingEventPublicDefaultResponse>;
}
import { ParticipantStateApiRequestFactory, ParticipantStateApiResponseProcessor } from "../apis/ParticipantStateApi";
export declare class ObservableParticipantStateApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: ParticipantStateApiRequestFactory, responseProcessor?: ParticipantStateApiResponseProcessor);
    getParticipationsBreakdownByContactIdWithHttpInfo(contactIdentifier: string, state?: string, limit?: number, after?: string, _options?: Configuration): Observable<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByContactId(contactIdentifier: string, state?: string, limit?: number, after?: string, _options?: Configuration): Observable<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsBreakdownByExternalEventIdWithHttpInfo(externalAccountId: string, externalEventId: string, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Observable<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByExternalEventId(externalAccountId: string, externalEventId: string, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Observable<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsBreakdownByMarketingEventIdWithHttpInfo(marketingEventId: number, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Observable<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByMarketingEventId(marketingEventId: number, contactIdentifier?: string, state?: string, limit?: number, after?: string, _options?: Configuration): Observable<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsCountersByEventExternalIdWithHttpInfo(externalAccountId: string, externalEventId: string, _options?: Configuration): Observable<HttpInfo<AttendanceCounters>>;
    getParticipationsCountersByEventExternalId(externalAccountId: string, externalEventId: string, _options?: Configuration): Observable<AttendanceCounters>;
    getParticipationsCountersByMarketingEventIdWithHttpInfo(marketingEventId: number, _options?: Configuration): Observable<HttpInfo<AttendanceCounters>>;
    getParticipationsCountersByMarketingEventId(marketingEventId: number, _options?: Configuration): Observable<AttendanceCounters>;
}
import { SettingsApiRequestFactory, SettingsApiResponseProcessor } from "../apis/SettingsApi";
export declare class ObservableSettingsApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: SettingsApiRequestFactory, responseProcessor?: SettingsApiResponseProcessor);
    getAllWithHttpInfo(appId: number, _options?: Configuration): Observable<HttpInfo<EventDetailSettings>>;
    getAll(appId: number, _options?: Configuration): Observable<EventDetailSettings>;
    updateWithHttpInfo(appId: number, eventDetailSettingsUrl: EventDetailSettingsUrl, _options?: Configuration): Observable<HttpInfo<EventDetailSettings>>;
    update(appId: number, eventDetailSettingsUrl: EventDetailSettingsUrl, _options?: Configuration): Observable<EventDetailSettings>;
}
import { SubscriberStateChangesApiRequestFactory, SubscriberStateChangesApiResponseProcessor } from "../apis/SubscriberStateChangesApi";
export declare class ObservableSubscriberStateChangesApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: SubscriberStateChangesApiRequestFactory, responseProcessor?: SubscriberStateChangesApiResponseProcessor);
    upsertByContactEmailWithHttpInfo(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, _options?: Configuration): Observable<HttpInfo<void>>;
    upsertByContactEmail(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber, _options?: Configuration): Observable<void>;
    upsertByContactIdWithHttpInfo(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, _options?: Configuration): Observable<HttpInfo<void>>;
    upsertByContactId(externalEventId: string, subscriberState: string, externalAccountId: string, batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber, _options?: Configuration): Observable<void>;
}
