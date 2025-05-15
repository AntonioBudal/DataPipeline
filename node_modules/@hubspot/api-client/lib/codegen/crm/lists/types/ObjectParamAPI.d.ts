import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { ApiCollectionResponseJoinTimeAndRecordId } from '../models/ApiCollectionResponseJoinTimeAndRecordId';
import { ApiCollectionResponseRecordListMembershipNoPaging } from '../models/ApiCollectionResponseRecordListMembershipNoPaging';
import { ListCreateRequest } from '../models/ListCreateRequest';
import { ListCreateResponse } from '../models/ListCreateResponse';
import { ListFetchResponse } from '../models/ListFetchResponse';
import { ListFilterUpdateRequest } from '../models/ListFilterUpdateRequest';
import { ListFolderCreateRequest } from '../models/ListFolderCreateRequest';
import { ListFolderCreateResponse } from '../models/ListFolderCreateResponse';
import { ListFolderFetchResponse } from '../models/ListFolderFetchResponse';
import { ListMoveRequest } from '../models/ListMoveRequest';
import { ListSearchRequest } from '../models/ListSearchRequest';
import { ListSearchResponse } from '../models/ListSearchResponse';
import { ListUpdateResponse } from '../models/ListUpdateResponse';
import { ListsByIdResponse } from '../models/ListsByIdResponse';
import { MembershipChangeRequest } from '../models/MembershipChangeRequest';
import { MembershipsUpdateResponse } from '../models/MembershipsUpdateResponse';
import { PublicBatchMigrationMapping } from '../models/PublicBatchMigrationMapping';
import { PublicMigrationMapping } from '../models/PublicMigrationMapping';
import { FoldersApiRequestFactory, FoldersApiResponseProcessor } from "../apis/FoldersApi";
export interface FoldersApiCreateRequest {
    listFolderCreateRequest: ListFolderCreateRequest;
}
export interface FoldersApiGetAllRequest {
    folderId?: string;
}
export interface FoldersApiMoveRequest {
    folderId: string;
    newParentFolderId: string;
}
export interface FoldersApiMoveListRequest {
    listMoveRequest: ListMoveRequest;
}
export interface FoldersApiRemoveRequest {
    folderId: string;
}
export interface FoldersApiRenameRequest {
    folderId: string;
    newFolderName?: string;
}
export declare class ObjectFoldersApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: FoldersApiRequestFactory, responseProcessor?: FoldersApiResponseProcessor);
    createWithHttpInfo(param: FoldersApiCreateRequest, options?: Configuration): Promise<HttpInfo<ListFolderCreateResponse>>;
    create(param: FoldersApiCreateRequest, options?: Configuration): Promise<ListFolderCreateResponse>;
    getAllWithHttpInfo(param?: FoldersApiGetAllRequest, options?: Configuration): Promise<HttpInfo<ListFolderFetchResponse>>;
    getAll(param?: FoldersApiGetAllRequest, options?: Configuration): Promise<ListFolderFetchResponse>;
    moveWithHttpInfo(param: FoldersApiMoveRequest, options?: Configuration): Promise<HttpInfo<ListFolderFetchResponse>>;
    move(param: FoldersApiMoveRequest, options?: Configuration): Promise<ListFolderFetchResponse>;
    moveListWithHttpInfo(param: FoldersApiMoveListRequest, options?: Configuration): Promise<HttpInfo<void>>;
    moveList(param: FoldersApiMoveListRequest, options?: Configuration): Promise<void>;
    removeWithHttpInfo(param: FoldersApiRemoveRequest, options?: Configuration): Promise<HttpInfo<void>>;
    remove(param: FoldersApiRemoveRequest, options?: Configuration): Promise<void>;
    renameWithHttpInfo(param: FoldersApiRenameRequest, options?: Configuration): Promise<HttpInfo<ListFolderFetchResponse>>;
    rename(param: FoldersApiRenameRequest, options?: Configuration): Promise<ListFolderFetchResponse>;
}
import { ListsApiRequestFactory, ListsApiResponseProcessor } from "../apis/ListsApi";
export interface ListsApiCreateRequest {
    listCreateRequest: ListCreateRequest;
}
export interface ListsApiDoSearchRequest {
    listSearchRequest: ListSearchRequest;
}
export interface ListsApiGetAllRequest {
    listIds?: Array<string>;
    includeFilters?: boolean;
}
export interface ListsApiGetByIdRequest {
    listId: string;
    includeFilters?: boolean;
}
export interface ListsApiGetByNameRequest {
    listName: string;
    objectTypeId: string;
    includeFilters?: boolean;
}
export interface ListsApiRemoveRequest {
    listId: string;
}
export interface ListsApiRestoreRequest {
    listId: string;
}
export interface ListsApiUpdateListFiltersRequest {
    listId: string;
    listFilterUpdateRequest: ListFilterUpdateRequest;
    enrollObjectsInWorkflows?: boolean;
}
export interface ListsApiUpdateNameRequest {
    listId: string;
    listName?: string;
    includeFilters?: boolean;
}
export declare class ObjectListsApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: ListsApiRequestFactory, responseProcessor?: ListsApiResponseProcessor);
    createWithHttpInfo(param: ListsApiCreateRequest, options?: Configuration): Promise<HttpInfo<ListCreateResponse>>;
    create(param: ListsApiCreateRequest, options?: Configuration): Promise<ListCreateResponse>;
    doSearchWithHttpInfo(param: ListsApiDoSearchRequest, options?: Configuration): Promise<HttpInfo<ListSearchResponse>>;
    doSearch(param: ListsApiDoSearchRequest, options?: Configuration): Promise<ListSearchResponse>;
    getAllWithHttpInfo(param?: ListsApiGetAllRequest, options?: Configuration): Promise<HttpInfo<ListsByIdResponse>>;
    getAll(param?: ListsApiGetAllRequest, options?: Configuration): Promise<ListsByIdResponse>;
    getByIdWithHttpInfo(param: ListsApiGetByIdRequest, options?: Configuration): Promise<HttpInfo<ListFetchResponse>>;
    getById(param: ListsApiGetByIdRequest, options?: Configuration): Promise<ListFetchResponse>;
    getByNameWithHttpInfo(param: ListsApiGetByNameRequest, options?: Configuration): Promise<HttpInfo<ListFetchResponse>>;
    getByName(param: ListsApiGetByNameRequest, options?: Configuration): Promise<ListFetchResponse>;
    removeWithHttpInfo(param: ListsApiRemoveRequest, options?: Configuration): Promise<HttpInfo<void>>;
    remove(param: ListsApiRemoveRequest, options?: Configuration): Promise<void>;
    restoreWithHttpInfo(param: ListsApiRestoreRequest, options?: Configuration): Promise<HttpInfo<void>>;
    restore(param: ListsApiRestoreRequest, options?: Configuration): Promise<void>;
    updateListFiltersWithHttpInfo(param: ListsApiUpdateListFiltersRequest, options?: Configuration): Promise<HttpInfo<ListUpdateResponse>>;
    updateListFilters(param: ListsApiUpdateListFiltersRequest, options?: Configuration): Promise<ListUpdateResponse>;
    updateNameWithHttpInfo(param: ListsApiUpdateNameRequest, options?: Configuration): Promise<HttpInfo<ListUpdateResponse>>;
    updateName(param: ListsApiUpdateNameRequest, options?: Configuration): Promise<ListUpdateResponse>;
}
import { MappingApiRequestFactory, MappingApiResponseProcessor } from "../apis/MappingApi";
export interface MappingApiTranslateLegacyListIdToListIdRequest {
    legacyListId?: string;
}
export interface MappingApiTranslateLegacyListIdToListIdBatchRequest {
    requestBody: Array<string>;
}
export declare class ObjectMappingApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: MappingApiRequestFactory, responseProcessor?: MappingApiResponseProcessor);
    translateLegacyListIdToListIdWithHttpInfo(param?: MappingApiTranslateLegacyListIdToListIdRequest, options?: Configuration): Promise<HttpInfo<PublicMigrationMapping>>;
    translateLegacyListIdToListId(param?: MappingApiTranslateLegacyListIdToListIdRequest, options?: Configuration): Promise<PublicMigrationMapping>;
    translateLegacyListIdToListIdBatchWithHttpInfo(param: MappingApiTranslateLegacyListIdToListIdBatchRequest, options?: Configuration): Promise<HttpInfo<PublicBatchMigrationMapping>>;
    translateLegacyListIdToListIdBatch(param: MappingApiTranslateLegacyListIdToListIdBatchRequest, options?: Configuration): Promise<PublicBatchMigrationMapping>;
}
import { MembershipsApiRequestFactory, MembershipsApiResponseProcessor } from "../apis/MembershipsApi";
export interface MembershipsApiAddRequest {
    listId: string;
    requestBody: Array<string>;
}
export interface MembershipsApiAddAllFromListRequest {
    listId: string;
    sourceListId: string;
}
export interface MembershipsApiAddAndRemoveRequest {
    listId: string;
    membershipChangeRequest: MembershipChangeRequest;
}
export interface MembershipsApiGetListsRequest {
    objectTypeId: string;
    recordId: string;
}
export interface MembershipsApiGetPageRequest {
    listId: string;
    after?: string;
    before?: string;
    limit?: number;
}
export interface MembershipsApiGetPageOrderedByAddedToListDateRequest {
    listId: string;
    after?: string;
    before?: string;
    limit?: number;
}
export interface MembershipsApiRemoveRequest {
    listId: string;
    requestBody: Array<string>;
}
export interface MembershipsApiRemoveAllRequest {
    listId: string;
}
export declare class ObjectMembershipsApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: MembershipsApiRequestFactory, responseProcessor?: MembershipsApiResponseProcessor);
    addWithHttpInfo(param: MembershipsApiAddRequest, options?: Configuration): Promise<HttpInfo<MembershipsUpdateResponse>>;
    add(param: MembershipsApiAddRequest, options?: Configuration): Promise<MembershipsUpdateResponse>;
    addAllFromListWithHttpInfo(param: MembershipsApiAddAllFromListRequest, options?: Configuration): Promise<HttpInfo<void>>;
    addAllFromList(param: MembershipsApiAddAllFromListRequest, options?: Configuration): Promise<void>;
    addAndRemoveWithHttpInfo(param: MembershipsApiAddAndRemoveRequest, options?: Configuration): Promise<HttpInfo<MembershipsUpdateResponse>>;
    addAndRemove(param: MembershipsApiAddAndRemoveRequest, options?: Configuration): Promise<MembershipsUpdateResponse>;
    getListsWithHttpInfo(param: MembershipsApiGetListsRequest, options?: Configuration): Promise<HttpInfo<ApiCollectionResponseRecordListMembershipNoPaging>>;
    getLists(param: MembershipsApiGetListsRequest, options?: Configuration): Promise<ApiCollectionResponseRecordListMembershipNoPaging>;
    getPageWithHttpInfo(param: MembershipsApiGetPageRequest, options?: Configuration): Promise<HttpInfo<ApiCollectionResponseJoinTimeAndRecordId>>;
    getPage(param: MembershipsApiGetPageRequest, options?: Configuration): Promise<ApiCollectionResponseJoinTimeAndRecordId>;
    getPageOrderedByAddedToListDateWithHttpInfo(param: MembershipsApiGetPageOrderedByAddedToListDateRequest, options?: Configuration): Promise<HttpInfo<ApiCollectionResponseJoinTimeAndRecordId>>;
    getPageOrderedByAddedToListDate(param: MembershipsApiGetPageOrderedByAddedToListDateRequest, options?: Configuration): Promise<ApiCollectionResponseJoinTimeAndRecordId>;
    removeWithHttpInfo(param: MembershipsApiRemoveRequest, options?: Configuration): Promise<HttpInfo<MembershipsUpdateResponse>>;
    remove(param: MembershipsApiRemoveRequest, options?: Configuration): Promise<MembershipsUpdateResponse>;
    removeAllWithHttpInfo(param: MembershipsApiRemoveAllRequest, options?: Configuration): Promise<HttpInfo<void>>;
    removeAll(param: MembershipsApiRemoveAllRequest, options?: Configuration): Promise<void>;
}
