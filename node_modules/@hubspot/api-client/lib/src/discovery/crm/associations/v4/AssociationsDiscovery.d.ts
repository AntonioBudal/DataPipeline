import { BasicApi, BatchApi, ReportApi } from '../../../../../codegen/crm/associations/v4/index';
import IConfiguration from '../../../../configuration/IConfiguration';
import BaseDiscovery from '../../../BaseDiscovery';
import type SchemaDiscovery from './schema/SchemaDiscovery';
export default class AssociationsDiscovery extends BaseDiscovery {
    basicApi: BasicApi;
    batchApi: BatchApi;
    reportApi: ReportApi;
    protected _schema: SchemaDiscovery | undefined;
    constructor(config: IConfiguration);
    get schema(): SchemaDiscovery;
}
