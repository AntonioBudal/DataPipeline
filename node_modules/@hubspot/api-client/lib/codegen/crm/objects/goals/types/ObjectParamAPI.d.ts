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
export interface BasicApiGetByIdRequest {
    goalTargetId: string;
    properties?: Array<string>;
    propertiesWithHistory?: Array<string>;
    associations?: Array<string>;
    archived?: boolean;
    idProperty?: string;
}
export interface BasicApiGetPageRequest {
    limit?: number;
    after?: string;
    properties?: Array<string>;
    propertiesWithHistory?: Array<string>;
    associations?: Array<string>;
    archived?: boolean;
}
export declare class ObjectBasicApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BasicApiRequestFactory, responseProcessor?: BasicApiResponseProcessor);
    getByIdWithHttpInfo(param: BasicApiGetByIdRequest, options?: Configuration): Promise<HttpInfo<SimplePublicObjectWithAssociations>>;
    getById(param: BasicApiGetByIdRequest, options?: Configuration): Promise<SimplePublicObjectWithAssociations>;
    getPageWithHttpInfo(param?: BasicApiGetPageRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>>;
    getPage(param?: BasicApiGetPageRequest, options?: Configuration): Promise<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>;
}
import { BatchApiRequestFactory, BatchApiResponseProcessor } from "../apis/BatchApi";
export interface BatchApiReadRequest {
    batchReadInputSimplePublicObjectId: BatchReadInputSimplePublicObjectId;
    archived?: boolean;
}
export declare class ObjectBatchApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BatchApiRequestFactory, responseProcessor?: BatchApiResponseProcessor);
    readWithHttpInfo(param: BatchApiReadRequest, options?: Configuration): Promise<HttpInfo<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>>;
    read(param: BatchApiReadRequest, options?: Configuration): Promise<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>;
}
import { SearchApiRequestFactory, SearchApiResponseProcessor } from "../apis/SearchApi";
export interface SearchApiDoSearchRequest {
    publicObjectSearchRequest: PublicObjectSearchRequest;
}
export declare class ObjectSearchApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SearchApiRequestFactory, responseProcessor?: SearchApiResponseProcessor);
    doSearchWithHttpInfo(param: SearchApiDoSearchRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalSimplePublicObjectForwardPaging>>;
    doSearch(param: SearchApiDoSearchRequest, options?: Configuration): Promise<CollectionResponseWithTotalSimplePublicObjectForwardPaging>;
}
