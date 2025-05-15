import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { CollectionResponseExternalUnifiedEvent } from '../models/CollectionResponseExternalUnifiedEvent';
import { VisibleExternalEventTypeNames } from '../models/VisibleExternalEventTypeNames';
import { EventsApiRequestFactory, EventsApiResponseProcessor } from "../apis/EventsApi";
export declare class PromiseEventsApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: EventsApiRequestFactory, responseProcessor?: EventsApiResponseProcessor);
    getPageWithHttpInfo(objectType?: string, eventType?: string, after?: string, before?: string, limit?: number, sort?: Array<string>, occurredAfter?: Date, occurredBefore?: Date, objectId?: number, objectPropertyPropname?: any, propertyPropname?: any, id?: Array<string>, _options?: Configuration): Promise<HttpInfo<CollectionResponseExternalUnifiedEvent>>;
    getPage(objectType?: string, eventType?: string, after?: string, before?: string, limit?: number, sort?: Array<string>, occurredAfter?: Date, occurredBefore?: Date, objectId?: number, objectPropertyPropname?: any, propertyPropname?: any, id?: Array<string>, _options?: Configuration): Promise<CollectionResponseExternalUnifiedEvent>;
    getTypesWithHttpInfo(_options?: Configuration): Promise<HttpInfo<VisibleExternalEventTypeNames>>;
    getTypes(_options?: Configuration): Promise<VisibleExternalEventTypeNames>;
}
