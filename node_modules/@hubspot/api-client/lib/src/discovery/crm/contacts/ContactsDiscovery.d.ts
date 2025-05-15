import { BasicApi, BatchApi, GDPRApi, MergeApi, SearchApi, SimplePublicObjectWithAssociations } from '../../../../codegen/crm/contacts/index';
import IConfiguration from '../../../configuration/IConfiguration';
export default class ContactsDiscovery {
    basicApi: BasicApi;
    batchApi: BatchApi;
    gdprApi: GDPRApi;
    mergeApi: MergeApi;
    searchApi: SearchApi;
    constructor(config: IConfiguration);
    getAll(limit?: number, after?: string, properties?: string[], propertiesWithHistory?: string[], associations?: string[], archived?: boolean): Promise<SimplePublicObjectWithAssociations[]>;
}
