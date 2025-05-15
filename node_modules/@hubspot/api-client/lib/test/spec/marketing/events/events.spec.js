"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("../../../../codegen/marketing/events/index");
const index_2 = require("../../../../index");
describe('api client', () => {
    it('is discoverable', () => {
        const client = new index_2.Client().marketing.events;
        expect(client.hasOwnProperty('attendanceSubscriberStateChangesApi')).toBeTruthy();
        expect(index_1.AttendanceSubscriberStateChangesApi.name).toBe(client.attendanceSubscriberStateChangesApi.constructor.name);
        expect(client.hasOwnProperty('basicApi')).toBeTruthy();
        expect(index_1.BasicApi.name).toBe(client.basicApi.constructor.name);
        expect(client.hasOwnProperty('participantStateApi')).toBeTruthy();
        expect(index_1.ParticipantStateApi.name).toBe(client.participantStateApi.constructor.name);
        expect(client.hasOwnProperty('settingsApi')).toBeTruthy();
        expect(index_1.SettingsApi.name).toBe(client.settingsApi.constructor.name);
        expect(client.hasOwnProperty('subscriberStateChangesApi')).toBeTruthy();
        expect(index_1.SubscriberStateChangesApi.name).toBe(client.subscriberStateChangesApi.constructor.name);
    });
});
//# sourceMappingURL=events.spec.js.map