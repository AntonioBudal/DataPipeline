import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { Observable } from '../rxjsStub';
import { BatchReadInputSimplePublicObjectId } from '../models/BatchReadInputSimplePublicObjectId';
import { BatchResponseSimplePublicObject } from '../models/BatchResponseSimplePublicObject';
import { BatchResponseSimplePublicObjectWithErrors } from '../models/BatchResponseSimplePublicObjectWithErrors';
import { CollectionResponseSimplePublicObjectWithAssociationsForwardPaging } from '../models/CollectionResponseSimplePublicObjectWithAssociationsForwardPaging';
import { CollectionResponseWithTotalSimplePublicObjectForwardPaging } from '../models/CollectionResponseWithTotalSimplePublicObjectForwardPaging';
import { PublicObjectSearchRequest } from '../models/PublicObjectSearchRequest';
import { SimplePublicObjectWithAssociations } from '../models/SimplePublicObjectWithAssociations';
import { BasicApiRequestFactory, BasicApiResponseProcessor } from "../apis/BasicApi";
export declare class ObservableBasicApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: BasicApiRequestFactory, responseProcessor?: BasicApiResponseProcessor);
    getByIdWithHttpInfo(goalTargetId: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, idProperty?: string, _options?: Configuration): Observable<HttpInfo<SimplePublicObjectWithAssociations>>;
    getById(goalTargetId: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, idProperty?: string, _options?: Configuration): Observable<SimplePublicObjectWithAssociations>;
    getPageWithHttpInfo(limit?: number, after?: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, _options?: Configuration): Observable<HttpInfo<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>>;
    getPage(limit?: number, after?: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, _options?: Configuration): Observable<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>;
}
import { BatchApiRequestFactory, BatchApiResponseProcessor } from "../apis/BatchApi";
export declare class ObservableBatchApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: BatchApiRequestFactory, responseProcessor?: BatchApiResponseProcessor);
    readWithHttpInfo(batchReadInputSimplePublicObjectId: BatchReadInputSimplePublicObjectId, archived?: boolean, _options?: Configuration): Observable<HttpInfo<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>>;
    read(batchReadInputSimplePublicObjectId: BatchReadInputSimplePublicObjectId, archived?: boolean, _options?: Configuration): Observable<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>;
}
import { SearchApiRequestFactory, SearchApiResponseProcessor } from "../apis/SearchApi";
export declare class ObservableSearchApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: SearchApiRequestFactory, responseProcessor?: SearchApiResponseProcessor);
    doSearchWithHttpInfo(publicObjectSearchRequest: PublicObjectSearchRequest, _options?: Configuration): Observable<HttpInfo<CollectionResponseWithTotalSimplePublicObjectForwardPaging>>;
    doSearch(publicObjectSearchRequest: PublicObjectSearchRequest, _options?: Configuration): Observable<CollectionResponseWithTotalSimplePublicObjectForwardPaging>;
}
