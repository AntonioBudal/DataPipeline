import { CustomEventDataApi } from '../../../../codegen/events/send/index';
import IConfiguration from '../../../configuration/IConfiguration';
export default class EventsDiscovery {
    customEventDataApi: CustomEventDataApi;
    constructor(config: IConfiguration);
}
