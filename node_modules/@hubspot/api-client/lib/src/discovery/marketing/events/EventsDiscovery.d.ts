import { AttendanceSubscriberStateChangesApi, BasicApi, ParticipantStateApi, SettingsApi, SubscriberStateChangesApi } from '../../../../codegen/marketing/events/index';
import IConfiguration from '../../../configuration/IConfiguration';
export default class EventsDiscovery {
    attendanceSubscriberStateChangesApi: AttendanceSubscriberStateChangesApi;
    basicApi: BasicApi;
    participantStateApi: ParticipantStateApi;
    settingsApi: SettingsApi;
    subscriberStateChangesApi: SubscriberStateChangesApi;
    constructor(config: IConfiguration);
}
