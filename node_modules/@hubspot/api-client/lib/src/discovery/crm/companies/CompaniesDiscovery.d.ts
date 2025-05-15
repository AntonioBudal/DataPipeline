import { BasicApi, BatchApi, MergeApi, SearchApi, SimplePublicObjectWithAssociations } from '../../../../codegen/crm/companies/index';
import IConfiguration from '../../../configuration/IConfiguration';
export default class CompaniesDiscovery {
    basicApi: BasicApi;
    batchApi: BatchApi;
    mergeApi: MergeApi;
    searchApi: SearchApi;
    constructor(config: IConfiguration);
    getAll(limit?: number, after?: string, properties?: string[], propertiesWithHistory?: string[], associations?: string[], archived?: boolean): Promise<SimplePublicObjectWithAssociations[]>;
}
