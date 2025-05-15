import { PublicFormSubmissionFilterCoalescingRefineBy } from '../models/PublicFormSubmissionFilterCoalescingRefineBy';
import { PublicPropertyAssociationFilterBranchFiltersInner } from '../models/PublicPropertyAssociationFilterBranchFiltersInner';
export declare class PublicPropertyAssociationFilterBranchFilterBranchesInner {
    'filterBranchType': PublicPropertyAssociationFilterBranchFilterBranchesInnerFilterBranchTypeEnum;
    'filterBranches': Array<PublicPropertyAssociationFilterBranchFilterBranchesInner>;
    'filterBranchOperator': string;
    'filters': Array<PublicPropertyAssociationFilterBranchFiltersInner>;
    'eventTypeId': string;
    'coalescingRefineBy'?: PublicFormSubmissionFilterCoalescingRefineBy;
    'operator': string;
    'objectTypeId': string;
    'propertyWithObjectId': string;
    'associationTypeId': number;
    'associationCategory': string;
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
export declare enum PublicPropertyAssociationFilterBranchFilterBranchesInnerFilterBranchTypeEnum {
    Association = "ASSOCIATION"
}
