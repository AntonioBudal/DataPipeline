import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { BehavioralEventHttpCompletionRequest } from '../models/BehavioralEventHttpCompletionRequest';
import { CustomEventDataApiRequestFactory, CustomEventDataApiResponseProcessor } from "../apis/CustomEventDataApi";
export interface CustomEventDataApiSendRequest {
    behavioralEventHttpCompletionRequest: BehavioralEventHttpCompletionRequest;
}
export declare class ObjectCustomEventDataApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: CustomEventDataApiRequestFactory, responseProcessor?: CustomEventDataApiResponseProcessor);
    sendWithHttpInfo(param: CustomEventDataApiSendRequest, options?: Configuration): Promise<HttpInfo<void>>;
    send(param: CustomEventDataApiSendRequest, options?: Configuration): Promise<void>;
}
