import { PublicEventFilterMetadata } from '../models/PublicEventFilterMetadata';
import { PublicFormSubmissionFilterCoalescingRefineBy } from '../models/PublicFormSubmissionFilterCoalescingRefineBy';
import { PublicInListFilterMetadata } from '../models/PublicInListFilterMetadata';
import { PublicSurveyMonkeyValueFilterValueComparison } from '../models/PublicSurveyMonkeyValueFilterValueComparison';
export declare class PublicPropertyAssociationFilterBranchFiltersInner {
    'property': string;
    'filterType': PublicPropertyAssociationFilterBranchFiltersInnerFilterTypeEnum;
    'operation': PublicSurveyMonkeyValueFilterValueComparison;
    'listId': string;
    'coalescingRefineBy': PublicFormSubmissionFilterCoalescingRefineBy;
    'toObjectType'?: string;
    'associationTypeId': number;
    'associationCategory': string;
    'toObjectTypeId'?: string;
    'operator': string;
    'enableTracking'?: boolean;
    'pruningRefineBy': PublicFormSubmissionFilterCoalescingRefineBy;
    'pageUrl': string;
    'ctaName': string;
    'eventId': string;
    'formId'?: string;
    'pageId': string;
    'eventTypeId': string;
    'filterLines': Array<PublicEventFilterMetadata>;
    'subscriptionType': string;
    'subscriptionIds': Array<string>;
    'acceptedStatuses': Array<string>;
    'channel': string;
    'acceptedOptStates': Array<string>;
    'businessUnitId'?: string;
    'campaignId': string;
    'surveyId': string;
    'valueComparison': PublicSurveyMonkeyValueFilterValueComparison;
    'surveyQuestion': string;
    'surveyAnswerRowId'?: string;
    'surveyAnswerColId'?: string;
    'webinarId'?: string;
    'clickUrl'?: string;
    'level': string;
    'appId': string;
    'emailId': string;
    'privacyName': string;
    'searchTerms': Array<string>;
    'entityType': string;
    'adNetwork': string;
    'searchTermType': string;
    'metadata'?: PublicInListFilterMetadata;
    'propertyWithObjectId': string;
    'shouldAccept': boolean;
    'source'?: string;
    static readonly discriminator: string | undefined;
    static readonly attributeTypeMap: Array<{
        name: string;
        baseName: string;
        type: string;
        format: string;
    }>;
    static getAttributeTypeMap(): {
        name: string;
        baseName: string;
        type: string;
        format: string;
    }[];
    constructor();
}
export declare enum PublicPropertyAssociationFilterBranchFiltersInnerFilterTypeEnum {
    Constant = "CONSTANT"
}
