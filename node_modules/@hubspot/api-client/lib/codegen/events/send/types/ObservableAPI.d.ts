import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { Observable } from '../rxjsStub';
import { BehavioralEventHttpCompletionRequest } from '../models/BehavioralEventHttpCompletionRequest';
import { CustomEventDataApiRequestFactory, CustomEventDataApiResponseProcessor } from "../apis/CustomEventDataApi";
export declare class ObservableCustomEventDataApi {
    private requestFactory;
    private responseProcessor;
    private configuration;
    constructor(configuration: Configuration, requestFactory?: CustomEventDataApiRequestFactory, responseProcessor?: CustomEventDataApiResponseProcessor);
    sendWithHttpInfo(behavioralEventHttpCompletionRequest: BehavioralEventHttpCompletionRequest, _options?: Configuration): Observable<HttpInfo<void>>;
    send(behavioralEventHttpCompletionRequest: BehavioralEventHttpCompletionRequest, _options?: Configuration): Observable<void>;
}
