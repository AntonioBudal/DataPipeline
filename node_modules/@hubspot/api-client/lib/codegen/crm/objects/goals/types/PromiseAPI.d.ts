import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { BatchReadInputSimplePublicObjectId } from '../models/BatchReadInputSimplePublicObjectId';
import { BatchResponseSimplePublicObject } from '../models/BatchResponseSimplePublicObject';
import { BatchResponseSimplePublicObjectWithErrors } from '../models/BatchResponseSimplePublicObjectWithErrors';
import { CollectionResponseSimplePublicObjectWithAssociationsForwardPaging } from '../models/CollectionResponseSimplePublicObjectWithAssociationsForwardPaging';
import { CollectionResponseWithTotalSimplePublicObjectForwardPaging } from '../models/CollectionResponseWithTotalSimplePublicObjectForwardPaging';
import { PublicObjectSearchRequest } from '../models/PublicObjectSearchRequest';
import { SimplePublicObjectWithAssociations } from '../models/SimplePublicObjectWithAssociations';
import { BasicApiRequestFactory, BasicApiResponseProcessor } from "../apis/BasicApi";
export declare class PromiseBasicApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BasicApiRequestFactory, responseProcessor?: BasicApiResponseProcessor);
    getByIdWithHttpInfo(goalTargetId: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, idProperty?: string, _options?: Configuration): Promise<HttpInfo<SimplePublicObjectWithAssociations>>;
    getById(goalTargetId: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, idProperty?: string, _options?: Configuration): Promise<SimplePublicObjectWithAssociations>;
    getPageWithHttpInfo(limit?: number, after?: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, _options?: Configuration): Promise<HttpInfo<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>>;
    getPage(limit?: number, after?: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, _options?: Configuration): Promise<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>;
}
import { BatchApiRequestFactory, BatchApiResponseProcessor } from "../apis/BatchApi";
export declare class PromiseBatchApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BatchApiRequestFactory, responseProcessor?: BatchApiResponseProcessor);
    readWithHttpInfo(batchReadInputSimplePublicObjectId: BatchReadInputSimplePublicObjectId, archived?: boolean, _options?: Configuration): Promise<HttpInfo<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>>;
    read(batchReadInputSimplePublicObjectId: BatchReadInputSimplePublicObjectId, archived?: boolean, _options?: Configuration): Promise<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>;
}
import { SearchApiRequestFactory, SearchApiResponseProcessor } from "../apis/SearchApi";
export declare class PromiseSearchApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SearchApiRequestFactory, responseProcessor?: SearchApiResponseProcessor);
    doSearchWithHttpInfo(publicObjectSearchRequest: PublicObjectSearchRequest, _options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalSimplePublicObjectForwardPaging>>;
    doSearch(publicObjectSearchRequest: PublicObjectSearchRequest, _options?: Configuration): Promise<CollectionResponseWithTotalSimplePublicObjectForwardPaging>;
}
