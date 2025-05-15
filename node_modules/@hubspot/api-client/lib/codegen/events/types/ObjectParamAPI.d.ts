import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { CollectionResponseExternalUnifiedEvent } from '../models/CollectionResponseExternalUnifiedEvent';
import { VisibleExternalEventTypeNames } from '../models/VisibleExternalEventTypeNames';
import { EventsApiRequestFactory, EventsApiResponseProcessor } from "../apis/EventsApi";
export interface EventsApiGetPageRequest {
    objectType?: string;
    eventType?: string;
    after?: string;
    before?: string;
    limit?: number;
    sort?: Array<string>;
    occurredAfter?: Date;
    occurredBefore?: Date;
    objectId?: number;
    objectPropertyPropname?: any;
    propertyPropname?: any;
    id?: Array<string>;
}
export interface EventsApiGetTypesRequest {
}
export declare class ObjectEventsApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: EventsApiRequestFactory, responseProcessor?: EventsApiResponseProcessor);
    getPageWithHttpInfo(param?: EventsApiGetPageRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseExternalUnifiedEvent>>;
    getPage(param?: EventsApiGetPageRequest, options?: Configuration): Promise<CollectionResponseExternalUnifiedEvent>;
    getTypesWithHttpInfo(param?: EventsApiGetTypesRequest, options?: Configuration): Promise<HttpInfo<VisibleExternalEventTypeNames>>;
    getTypes(param?: EventsApiGetTypesRequest, options?: Configuration): Promise<VisibleExternalEventTypeNames>;
}
