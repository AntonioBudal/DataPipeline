"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("../../../../codegen/crm/schemas/index");
const index_2 = require("../../../../index");
describe('api client', () => {
    it('is discoverable', () => {
        const client = new index_2.Client().crm.schemas;
        expect(client.hasOwnProperty('coreApi')).toBeTruthy();
        expect(index_1.CoreApi.name).toBe(client.coreApi.constructor.name);
    });
});
//# sourceMappingURL=schemas.spec.js.map