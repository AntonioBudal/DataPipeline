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
export interface AttendanceSubscriberStateChangesApiRecordByContactEmailsRequest {
    externalEventId: string;
    subscriberState: string;
    batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber;
    externalAccountId?: string;
}
export interface AttendanceSubscriberStateChangesApiRecordByContactIdsRequest {
    externalEventId: string;
    subscriberState: string;
    batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber;
    externalAccountId?: string;
}
export declare class ObjectAttendanceSubscriberStateChangesApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: AttendanceSubscriberStateChangesApiRequestFactory, responseProcessor?: AttendanceSubscriberStateChangesApiResponseProcessor);
    recordByContactEmailsWithHttpInfo(param: AttendanceSubscriberStateChangesApiRecordByContactEmailsRequest, options?: Configuration): Promise<HttpInfo<BatchResponseSubscriberEmailResponse>>;
    recordByContactEmails(param: AttendanceSubscriberStateChangesApiRecordByContactEmailsRequest, options?: Configuration): Promise<BatchResponseSubscriberEmailResponse>;
    recordByContactIdsWithHttpInfo(param: AttendanceSubscriberStateChangesApiRecordByContactIdsRequest, options?: Configuration): Promise<HttpInfo<BatchResponseSubscriberVidResponse>>;
    recordByContactIds(param: AttendanceSubscriberStateChangesApiRecordByContactIdsRequest, options?: Configuration): Promise<BatchResponseSubscriberVidResponse>;
}
import { BasicApiRequestFactory, BasicApiResponseProcessor } from "../apis/BasicApi";
export interface BasicApiArchiveRequest {
    externalEventId: string;
    externalAccountId: string;
}
export interface BasicApiBatchArchiveRequest {
    batchInputMarketingEventExternalUniqueIdentifier: BatchInputMarketingEventExternalUniqueIdentifier;
}
export interface BasicApiBatchUpsertRequest {
    batchInputMarketingEventCreateRequestParams: BatchInputMarketingEventCreateRequestParams;
}
export interface BasicApiCancelRequest {
    externalEventId: string;
    externalAccountId: string;
}
export interface BasicApiCompleteRequest {
    externalEventId: string;
    externalAccountId: string;
    marketingEventCompleteRequestParams: MarketingEventCompleteRequestParams;
}
export interface BasicApiCreateRequest {
    marketingEventCreateRequestParams: MarketingEventCreateRequestParams;
}
export interface BasicApiDoSearchRequest {
    q: string;
}
export interface BasicApiGetDetailsRequest {
    externalEventId: string;
    externalAccountId: string;
}
export interface BasicApiUpdateRequest {
    externalEventId: string;
    externalAccountId: string;
    marketingEventUpdateRequestParams: MarketingEventUpdateRequestParams;
}
export interface BasicApiUpsertRequest {
    externalEventId: string;
    marketingEventCreateRequestParams: MarketingEventCreateRequestParams;
}
export declare class ObjectBasicApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BasicApiRequestFactory, responseProcessor?: BasicApiResponseProcessor);
    archiveWithHttpInfo(param: BasicApiArchiveRequest, options?: Configuration): Promise<HttpInfo<void>>;
    archive(param: BasicApiArchiveRequest, options?: Configuration): Promise<void>;
    batchArchiveWithHttpInfo(param: BasicApiBatchArchiveRequest, options?: Configuration): Promise<HttpInfo<void>>;
    batchArchive(param: BasicApiBatchArchiveRequest, options?: Configuration): Promise<void>;
    batchUpsertWithHttpInfo(param: BasicApiBatchUpsertRequest, options?: Configuration): Promise<HttpInfo<BatchResponseMarketingEventPublicDefaultResponse>>;
    batchUpsert(param: BasicApiBatchUpsertRequest, options?: Configuration): Promise<BatchResponseMarketingEventPublicDefaultResponse>;
    cancelWithHttpInfo(param: BasicApiCancelRequest, options?: Configuration): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    cancel(param: BasicApiCancelRequest, options?: Configuration): Promise<MarketingEventDefaultResponse>;
    completeWithHttpInfo(param: BasicApiCompleteRequest, options?: Configuration): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    complete(param: BasicApiCompleteRequest, options?: Configuration): Promise<MarketingEventDefaultResponse>;
    createWithHttpInfo(param: BasicApiCreateRequest, options?: Configuration): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    create(param: BasicApiCreateRequest, options?: Configuration): Promise<MarketingEventDefaultResponse>;
    doSearchWithHttpInfo(param: BasicApiDoSearchRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging>>;
    doSearch(param: BasicApiDoSearchRequest, options?: Configuration): Promise<CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging>;
    getDetailsWithHttpInfo(param: BasicApiGetDetailsRequest, options?: Configuration): Promise<HttpInfo<MarketingEventPublicReadResponse>>;
    getDetails(param: BasicApiGetDetailsRequest, options?: Configuration): Promise<MarketingEventPublicReadResponse>;
    updateWithHttpInfo(param: BasicApiUpdateRequest, options?: Configuration): Promise<HttpInfo<MarketingEventPublicDefaultResponse>>;
    update(param: BasicApiUpdateRequest, options?: Configuration): Promise<MarketingEventPublicDefaultResponse>;
    upsertWithHttpInfo(param: BasicApiUpsertRequest, options?: Configuration): Promise<HttpInfo<MarketingEventPublicDefaultResponse>>;
    upsert(param: BasicApiUpsertRequest, options?: Configuration): Promise<MarketingEventPublicDefaultResponse>;
}
import { ParticipantStateApiRequestFactory, ParticipantStateApiResponseProcessor } from "../apis/ParticipantStateApi";
export interface ParticipantStateApiGetParticipationsBreakdownByContactIdRequest {
    contactIdentifier: string;
    state?: string;
    limit?: number;
    after?: string;
}
export interface ParticipantStateApiGetParticipationsBreakdownByExternalEventIdRequest {
    externalAccountId: string;
    externalEventId: string;
    contactIdentifier?: string;
    state?: string;
    limit?: number;
    after?: string;
}
export interface ParticipantStateApiGetParticipationsBreakdownByMarketingEventIdRequest {
    marketingEventId: number;
    contactIdentifier?: string;
    state?: string;
    limit?: number;
    after?: string;
}
export interface ParticipantStateApiGetParticipationsCountersByEventExternalIdRequest {
    externalAccountId: string;
    externalEventId: string;
}
export interface ParticipantStateApiGetParticipationsCountersByMarketingEventIdRequest {
    marketingEventId: number;
}
export declare class ObjectParticipantStateApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: ParticipantStateApiRequestFactory, responseProcessor?: ParticipantStateApiResponseProcessor);
    getParticipationsBreakdownByContactIdWithHttpInfo(param: ParticipantStateApiGetParticipationsBreakdownByContactIdRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByContactId(param: ParticipantStateApiGetParticipationsBreakdownByContactIdRequest, options?: Configuration): Promise<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsBreakdownByExternalEventIdWithHttpInfo(param: ParticipantStateApiGetParticipationsBreakdownByExternalEventIdRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByExternalEventId(param: ParticipantStateApiGetParticipationsBreakdownByExternalEventIdRequest, options?: Configuration): Promise<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsBreakdownByMarketingEventIdWithHttpInfo(param: ParticipantStateApiGetParticipationsBreakdownByMarketingEventIdRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalParticipationBreakdownForwardPaging>>;
    getParticipationsBreakdownByMarketingEventId(param: ParticipantStateApiGetParticipationsBreakdownByMarketingEventIdRequest, options?: Configuration): Promise<CollectionResponseWithTotalParticipationBreakdownForwardPaging>;
    getParticipationsCountersByEventExternalIdWithHttpInfo(param: ParticipantStateApiGetParticipationsCountersByEventExternalIdRequest, options?: Configuration): Promise<HttpInfo<AttendanceCounters>>;
    getParticipationsCountersByEventExternalId(param: ParticipantStateApiGetParticipationsCountersByEventExternalIdRequest, options?: Configuration): Promise<AttendanceCounters>;
    getParticipationsCountersByMarketingEventIdWithHttpInfo(param: ParticipantStateApiGetParticipationsCountersByMarketingEventIdRequest, options?: Configuration): Promise<HttpInfo<AttendanceCounters>>;
    getParticipationsCountersByMarketingEventId(param: ParticipantStateApiGetParticipationsCountersByMarketingEventIdRequest, options?: Configuration): Promise<AttendanceCounters>;
}
import { SettingsApiRequestFactory, SettingsApiResponseProcessor } from "../apis/SettingsApi";
export interface SettingsApiGetAllRequest {
    appId: number;
}
export interface SettingsApiUpdateRequest {
    appId: number;
    eventDetailSettingsUrl: EventDetailSettingsUrl;
}
export declare class ObjectSettingsApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SettingsApiRequestFactory, responseProcessor?: SettingsApiResponseProcessor);
    getAllWithHttpInfo(param: SettingsApiGetAllRequest, options?: Configuration): Promise<HttpInfo<EventDetailSettings>>;
    getAll(param: SettingsApiGetAllRequest, options?: Configuration): Promise<EventDetailSettings>;
    updateWithHttpInfo(param: SettingsApiUpdateRequest, options?: Configuration): Promise<HttpInfo<EventDetailSettings>>;
    update(param: SettingsApiUpdateRequest, options?: Configuration): Promise<EventDetailSettings>;
}
import { SubscriberStateChangesApiRequestFactory, SubscriberStateChangesApiResponseProcessor } from "../apis/SubscriberStateChangesApi";
export interface SubscriberStateChangesApiUpsertByContactEmailRequest {
    externalEventId: string;
    subscriberState: string;
    externalAccountId: string;
    batchInputMarketingEventEmailSubscriber: BatchInputMarketingEventEmailSubscriber;
}
export interface SubscriberStateChangesApiUpsertByContactIdRequest {
    externalEventId: string;
    subscriberState: string;
    externalAccountId: string;
    batchInputMarketingEventSubscriber: BatchInputMarketingEventSubscriber;
}
export declare class ObjectSubscriberStateChangesApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SubscriberStateChangesApiRequestFactory, responseProcessor?: SubscriberStateChangesApiResponseProcessor);
    upsertByContactEmailWithHttpInfo(param: SubscriberStateChangesApiUpsertByContactEmailRequest, options?: Configuration): Promise<HttpInfo<void>>;
    upsertByContactEmail(param: SubscriberStateChangesApiUpsertByContactEmailRequest, options?: Configuration): Promise<void>;
    upsertByContactIdWithHttpInfo(param: SubscriberStateChangesApiUpsertByContactIdRequest, options?: Configuration): Promise<HttpInfo<void>>;
    upsertByContactId(param: SubscriberStateChangesApiUpsertByContactIdRequest, options?: Configuration): Promise<void>;
}
