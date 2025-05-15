import { BaseAPIRequestFactory } from './baseapi';
import { Configuration } from '../configuration';
import { RequestContext, ResponseContext, HttpInfo } from '../http/http';
import { BatchInputMarketingEventCreateRequestParams } from '../models/BatchInputMarketingEventCreateRequestParams';
import { BatchInputMarketingEventExternalUniqueIdentifier } from '../models/BatchInputMarketingEventExternalUniqueIdentifier';
import { BatchResponseMarketingEventPublicDefaultResponse } from '../models/BatchResponseMarketingEventPublicDefaultResponse';
import { CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging } from '../models/CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging';
import { MarketingEventCompleteRequestParams } from '../models/MarketingEventCompleteRequestParams';
import { MarketingEventCreateRequestParams } from '../models/MarketingEventCreateRequestParams';
import { MarketingEventDefaultResponse } from '../models/MarketingEventDefaultResponse';
import { MarketingEventPublicDefaultResponse } from '../models/MarketingEventPublicDefaultResponse';
import { MarketingEventPublicReadResponse } from '../models/MarketingEventPublicReadResponse';
import { MarketingEventUpdateRequestParams } from '../models/MarketingEventUpdateRequestParams';
export declare class BasicApiRequestFactory extends BaseAPIRequestFactory {
    archive(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<RequestContext>;
    batchArchive(batchInputMarketingEventExternalUniqueIdentifier: BatchInputMarketingEventExternalUniqueIdentifier, _options?: Configuration): Promise<RequestContext>;
    batchUpsert(batchInputMarketingEventCreateRequestParams: BatchInputMarketingEventCreateRequestParams, _options?: Configuration): Promise<RequestContext>;
    cancel(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<RequestContext>;
    complete(externalEventId: string, externalAccountId: string, marketingEventCompleteRequestParams: MarketingEventCompleteRequestParams, _options?: Configuration): Promise<RequestContext>;
    create(marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Promise<RequestContext>;
    doSearch(q: string, _options?: Configuration): Promise<RequestContext>;
    getDetails(externalEventId: string, externalAccountId: string, _options?: Configuration): Promise<RequestContext>;
    update(externalEventId: string, externalAccountId: string, marketingEventUpdateRequestParams: MarketingEventUpdateRequestParams, _options?: Configuration): Promise<RequestContext>;
    upsert(externalEventId: string, marketingEventCreateRequestParams: MarketingEventCreateRequestParams, _options?: Configuration): Promise<RequestContext>;
}
export declare class BasicApiResponseProcessor {
    archiveWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void>>;
    batchArchiveWithHttpInfo(response: ResponseContext): Promise<HttpInfo<void>>;
    batchUpsertWithHttpInfo(response: ResponseContext): Promise<HttpInfo<BatchResponseMarketingEventPublicDefaultResponse>>;
    cancelWithHttpInfo(response: ResponseContext): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    completeWithHttpInfo(response: ResponseContext): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    createWithHttpInfo(response: ResponseContext): Promise<HttpInfo<MarketingEventDefaultResponse>>;
    doSearchWithHttpInfo(response: ResponseContext): Promise<HttpInfo<CollectionResponseMarketingEventExternalUniqueIdentifierNoPaging>>;
    getDetailsWithHttpInfo(response: ResponseContext): Promise<HttpInfo<MarketingEventPublicReadResponse>>;
    updateWithHttpInfo(response: ResponseContext): Promise<HttpInfo<MarketingEventPublicDefaultResponse>>;
    upsertWithHttpInfo(response: ResponseContext): Promise<HttpInfo<MarketingEventPublicDefaultResponse>>;
}
