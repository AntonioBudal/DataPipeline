import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { BehavioralEventHttpCompletionRequest } from '../models/BehavioralEventHttpCompletionRequest';
import { CustomEventDataApiRequestFactory, CustomEventDataApiResponseProcessor } from "../apis/CustomEventDataApi";
export declare class PromiseCustomEventDataApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: CustomEventDataApiRequestFactory, responseProcessor?: CustomEventDataApiResponseProcessor);
    sendWithHttpInfo(behavioralEventHttpCompletionRequest: BehavioralEventHttpCompletionRequest, _options?: Configuration): Promise<HttpInfo<void>>;
    send(behavioralEventHttpCompletionRequest: BehavioralEventHttpCompletionRequest, _options?: Configuration): Promise<void>;
}
