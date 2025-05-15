import { HttpInfo } from '../http/http';
import { Configuration } from '../configuration';
import { CollectionResponseSmtpApiTokenViewForwardPaging } from '../models/CollectionResponseSmtpApiTokenViewForwardPaging';
import { EmailSendStatusView } from '../models/EmailSendStatusView';
import { PublicSingleSendRequestEgg } from '../models/PublicSingleSendRequestEgg';
import { SmtpApiTokenRequestEgg } from '../models/SmtpApiTokenRequestEgg';
import { SmtpApiTokenView } from '../models/SmtpApiTokenView';
import { PublicSMTPTokensApiRequestFactory, PublicSMTPTokensApiResponseProcessor } from "../apis/PublicSMTPTokensApi";
export interface PublicSMTPTokensApiArchiveTokenRequest {
    tokenId: string;
}
export interface PublicSMTPTokensApiCreateTokenRequest {
    smtpApiTokenRequestEgg: SmtpApiTokenRequestEgg;
}
export interface PublicSMTPTokensApiGetTokenByIdRequest {
    tokenId: string;
}
export interface PublicSMTPTokensApiGetTokensPageRequest {
    campaignName?: string;
    emailCampaignId?: string;
    after?: string;
    limit?: number;
}
export interface PublicSMTPTokensApiResetPasswordRequest {
    tokenId: string;
}
export declare class ObjectPublicSMTPTokensApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: PublicSMTPTokensApiRequestFactory, responseProcessor?: PublicSMTPTokensApiResponseProcessor);
    archiveTokenWithHttpInfo(param: PublicSMTPTokensApiArchiveTokenRequest, options?: Configuration): Promise<HttpInfo<void>>;
    archiveToken(param: PublicSMTPTokensApiArchiveTokenRequest, options?: Configuration): Promise<void>;
    createTokenWithHttpInfo(param: PublicSMTPTokensApiCreateTokenRequest, options?: Configuration): Promise<HttpInfo<SmtpApiTokenView>>;
    createToken(param: PublicSMTPTokensApiCreateTokenRequest, options?: Configuration): Promise<SmtpApiTokenView>;
    getTokenByIdWithHttpInfo(param: PublicSMTPTokensApiGetTokenByIdRequest, options?: Configuration): Promise<HttpInfo<SmtpApiTokenView>>;
    getTokenById(param: PublicSMTPTokensApiGetTokenByIdRequest, options?: Configuration): Promise<SmtpApiTokenView>;
    getTokensPageWithHttpInfo(param?: PublicSMTPTokensApiGetTokensPageRequest, options?: Configuration): Promise<HttpInfo<CollectionResponseSmtpApiTokenViewForwardPaging>>;
    getTokensPage(param?: PublicSMTPTokensApiGetTokensPageRequest, options?: Configuration): Promise<CollectionResponseSmtpApiTokenViewForwardPaging>;
    resetPasswordWithHttpInfo(param: PublicSMTPTokensApiResetPasswordRequest, options?: Configuration): Promise<HttpInfo<SmtpApiTokenView>>;
    resetPassword(param: PublicSMTPTokensApiResetPasswordRequest, options?: Configuration): Promise<SmtpApiTokenView>;
}
import { SingleSendApiRequestFactory, SingleSendApiResponseProcessor } from "../apis/SingleSendApi";
export interface SingleSendApiSendEmailRequest {
    publicSingleSendRequestEgg: PublicSingleSendRequestEgg;
}
export declare class ObjectSingleSendApi {
    private api;
    constructor(configuration: Configuration, requestFactory?: SingleSendApiRequestFactory, responseProcessor?: SingleSendApiResponseProcessor);
    sendEmailWithHttpInfo(param: SingleSendApiSendEmailRequest, options?: Configuration): Promise<HttpInfo<EmailSendStatusView>>;
    sendEmail(param: SingleSendApiSendEmailRequest, options?: Configuration): Promise<EmailSendStatusView>;
}
