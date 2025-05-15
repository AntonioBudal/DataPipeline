import { HttpFile, HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { ActionResponse } from '../models/ActionResponse';
import { AssetFileMetadata } from '../models/AssetFileMetadata';
import { FileExtractRequest } from '../models/FileExtractRequest';
import { TaskLocator } from '../models/TaskLocator';
import { ContentApiRequestFactory, ContentApiResponseProcessor } from "../apis/ContentApi";
export interface ContentApiArchiveRequest {
    environment: string;
    path: string;
}
export interface ContentApiCreateRequest {
    environment: string;
    path: string;
    file?: HttpFile;
}
export interface ContentApiCreateOrUpdateRequest {
    environment: string;
    path: string;
    file?: HttpFile;
}
export interface ContentApiDownloadRequest {
    environment: string;
    path: string;
}
export declare class ObjectContentApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: ContentApiRequestFactory, responseProcessor?: ContentApiResponseProcessor);
    archiveWithHttpInfo(param: ContentApiArchiveRequest, options?: Configuration): Promise<HttpInfo<void>>;
    archive(param: ContentApiArchiveRequest, options?: Configuration): Promise<void>;
    createWithHttpInfo(param: ContentApiCreateRequest, options?: Configuration): Promise<HttpInfo<AssetFileMetadata>>;
    create(param: ContentApiCreateRequest, options?: Configuration): Promise<AssetFileMetadata>;
    createOrUpdateWithHttpInfo(param: ContentApiCreateOrUpdateRequest, options?: Configuration): Promise<HttpInfo<AssetFileMetadata>>;
    createOrUpdate(param: ContentApiCreateOrUpdateRequest, options?: Configuration): Promise<AssetFileMetadata>;
    downloadWithHttpInfo(param: ContentApiDownloadRequest, options?: Configuration): Promise<HttpInfo<void>>;
    download(param: ContentApiDownloadRequest, options?: Configuration): Promise<void>;
}
import { ExtractApiRequestFactory, ExtractApiResponseProcessor } from "../apis/ExtractApi";
export interface ExtractApiDoAsyncRequest {
    fileExtractRequest: FileExtractRequest;
}
export interface ExtractApiGetAsyncStatusRequest {
    taskId: number;
}
export declare class ObjectExtractApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: ExtractApiRequestFactory, responseProcessor?: ExtractApiResponseProcessor);
    doAsyncWithHttpInfo(param: ExtractApiDoAsyncRequest, options?: Configuration): Promise<HttpInfo<TaskLocator>>;
    doAsync(param: ExtractApiDoAsyncRequest, options?: Configuration): Promise<TaskLocator>;
    getAsyncStatusWithHttpInfo(param: ExtractApiGetAsyncStatusRequest, options?: Configuration): Promise<HttpInfo<ActionResponse>>;
    getAsyncStatus(param: ExtractApiGetAsyncStatusRequest, options?: Configuration): Promise<ActionResponse>;
}
import { MetadataApiRequestFactory, MetadataApiResponseProcessor } from "../apis/MetadataApi";
export interface MetadataApiGetRequest {
    environment: string;
    path: string;
    properties?: string;
}
export declare class ObjectMetadataApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: MetadataApiRequestFactory, responseProcessor?: MetadataApiResponseProcessor);
    getWithHttpInfo(param: MetadataApiGetRequest, options?: Configuration): Promise<HttpInfo<AssetFileMetadata>>;
    get(param: MetadataApiGetRequest, options?: Configuration): Promise<AssetFileMetadata>;
}
import { ValidationApiRequestFactory, ValidationApiResponseProcessor } from "../apis/ValidationApi";
export interface ValidationApiDoValidateRequest {
    path: string;
    file?: HttpFile;
}
export declare class ObjectValidationApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: ValidationApiRequestFactory, responseProcessor?: ValidationApiResponseProcessor);
    doValidateWithHttpInfo(param: ValidationApiDoValidateRequest, options?: Configuration): Promise<HttpInfo<void>>;
    doValidate(param: ValidationApiDoValidateRequest, options?: Configuration): Promise<void>;
}
