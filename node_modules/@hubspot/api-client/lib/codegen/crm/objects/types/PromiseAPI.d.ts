import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { BatchInputSimplePublicObjectBatchInput } from '../models/BatchInputSimplePublicObjectBatchInput';
import { BatchInputSimplePublicObjectBatchInputUpsert } from '../models/BatchInputSimplePublicObjectBatchInputUpsert';
import { BatchInputSimplePublicObjectId } from '../models/BatchInputSimplePublicObjectId';
import { BatchInputSimplePublicObjectInputForCreate } from '../models/BatchInputSimplePublicObjectInputForCreate';
import { BatchReadInputSimplePublicObjectId } from '../models/BatchReadInputSimplePublicObjectId';
import { BatchResponseSimplePublicObject } from '../models/BatchResponseSimplePublicObject';
import { BatchResponseSimplePublicObjectWithErrors } from '../models/BatchResponseSimplePublicObjectWithErrors';
import { BatchResponseSimplePublicUpsertObject } from '../models/BatchResponseSimplePublicUpsertObject';
import { BatchResponseSimplePublicUpsertObjectWithErrors } from '../models/BatchResponseSimplePublicUpsertObjectWithErrors';
import { CollectionResponseSimplePublicObjectWithAssociationsForwardPaging } from '../models/CollectionResponseSimplePublicObjectWithAssociationsForwardPaging';
import { CollectionResponseWithTotalSimplePublicObjectForwardPaging } from '../models/CollectionResponseWithTotalSimplePublicObjectForwardPaging';
import { PublicObjectSearchRequest } from '../models/PublicObjectSearchRequest';
import { SimplePublicObject } from '../models/SimplePublicObject';
import { SimplePublicObjectInput } from '../models/SimplePublicObjectInput';
import { SimplePublicObjectInputForCreate } from '../models/SimplePublicObjectInputForCreate';
import { SimplePublicObjectWithAssociations } from '../models/SimplePublicObjectWithAssociations';
import { BasicApiRequestFactory, BasicApiResponseProcessor } from "../apis/BasicApi";
export declare class PromiseBasicApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BasicApiRequestFactory, responseProcessor?: BasicApiResponseProcessor);
    archiveWithHttpInfo(objectType: string, objectId: string, _options?: Configuration): Promise<HttpInfo<void>>;
    archive(objectType: string, objectId: string, _options?: Configuration): Promise<void>;
    createWithHttpInfo(objectType: string, simplePublicObjectInputForCreate: SimplePublicObjectInputForCreate, _options?: Configuration): Promise<HttpInfo<SimplePublicObject>>;
    create(objectType: string, simplePublicObjectInputForCreate: SimplePublicObjectInputForCreate, _options?: Configuration): Promise<SimplePublicObject>;
    getByIdWithHttpInfo(objectType: string, objectId: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, idProperty?: string, _options?: Configuration): Promise<HttpInfo<SimplePublicObjectWithAssociations>>;
    getById(objectType: string, objectId: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, idProperty?: string, _options?: Configuration): Promise<SimplePublicObjectWithAssociations>;
    getPageWithHttpInfo(objectType: string, limit?: number, after?: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, _options?: Configuration): Promise<HttpInfo<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>>;
    getPage(objectType: string, limit?: number, after?: string, properties?: Array<string>, propertiesWithHistory?: Array<string>, associations?: Array<string>, archived?: boolean, _options?: Configuration): Promise<CollectionResponseSimplePublicObjectWithAssociationsForwardPaging>;
    updateWithHttpInfo(objectType: string, objectId: string, simplePublicObjectInput: SimplePublicObjectInput, idProperty?: string, _options?: Configuration): Promise<HttpInfo<SimplePublicObject>>;
    update(objectType: string, objectId: string, simplePublicObjectInput: SimplePublicObjectInput, idProperty?: string, _options?: Configuration): Promise<SimplePublicObject>;
}
import { BatchApiRequestFactory, BatchApiResponseProcessor } from "../apis/BatchApi";
export declare class PromiseBatchApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: BatchApiRequestFactory, responseProcessor?: BatchApiResponseProcessor);
    archiveWithHttpInfo(objectType: string, batchInputSimplePublicObjectId: BatchInputSimplePublicObjectId, _options?: Configuration): Promise<HttpInfo<void>>;
    archive(objectType: string, batchInputSimplePublicObjectId: BatchInputSimplePublicObjectId, _options?: Configuration): Promise<void>;
    createWithHttpInfo(objectType: string, batchInputSimplePublicObjectInputForCreate: BatchInputSimplePublicObjectInputForCreate, _options?: Configuration): Promise<HttpInfo<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>>;
    create(objectType: string, batchInputSimplePublicObjectInputForCreate: BatchInputSimplePublicObjectInputForCreate, _options?: Configuration): Promise<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>;
    readWithHttpInfo(objectType: string, batchReadInputSimplePublicObjectId: BatchReadInputSimplePublicObjectId, archived?: boolean, _options?: Configuration): Promise<HttpInfo<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>>;
    read(objectType: string, batchReadInputSimplePublicObjectId: BatchReadInputSimplePublicObjectId, archived?: boolean, _options?: Configuration): Promise<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>;
    updateWithHttpInfo(objectType: string, batchInputSimplePublicObjectBatchInput: BatchInputSimplePublicObjectBatchInput, _options?: Configuration): Promise<HttpInfo<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>>;
    update(objectType: string, batchInputSimplePublicObjectBatchInput: BatchInputSimplePublicObjectBatchInput, _options?: Configuration): Promise<BatchResponseSimplePublicObject | BatchResponseSimplePublicObjectWithErrors>;
    upsertWithHttpInfo(objectType: string, batchInputSimplePublicObjectBatchInputUpsert: BatchInputSimplePublicObjectBatchInputUpsert, _options?: Configuration): Promise<HttpInfo<BatchResponseSimplePublicUpsertObjectWithErrors | BatchResponseSimplePublicUpsertObject>>;
    upsert(objectType: string, batchInputSimplePublicObjectBatchInputUpsert: BatchInputSimplePublicObjectBatchInputUpsert, _options?: Configuration): Promise<BatchResponseSimplePublicUpsertObjectWithErrors | BatchResponseSimplePublicUpsertObject>;
}
import { SearchApiRequestFactory, SearchApiResponseProcessor } from "../apis/SearchApi";
export declare class PromiseSearchApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SearchApiRequestFactory, responseProcessor?: SearchApiResponseProcessor);
    doSearchWithHttpInfo(objectType: string, publicObjectSearchRequest: PublicObjectSearchRequest, _options?: Configuration): Promise<HttpInfo<CollectionResponseWithTotalSimplePublicObjectForwardPaging>>;
    doSearch(objectType: string, publicObjectSearchRequest: PublicObjectSearchRequest, _options?: Configuration): Promise<CollectionResponseWithTotalSimplePublicObjectForwardPaging>;
}
