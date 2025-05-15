"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("../../../../codegen/events/send/index");
const index_2 = require("../../../../index");
describe('api client', () => {
    it('is discoverable', () => {
        const client = new index_2.Client().events.send;
        expect(client.hasOwnProperty('customEventDataApi')).toBeTruthy();
        expect(index_1.CustomEventDataApi.name).toBe(client.customEventDataApi.constructor.name);
    });
});
//# sourceMappingURL=send.spec.js.map