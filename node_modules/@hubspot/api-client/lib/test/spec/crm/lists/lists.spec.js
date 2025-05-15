"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("../../../../codegen/crm/lists/index");
const index_2 = require("../../../../index");
describe('api client', () => {
    it('is discoverable', () => {
        const client = new index_2.Client().crm.lists;
        expect(client.hasOwnProperty('foldersApi')).toBeTruthy();
        expect(index_1.FoldersApi.name).toBe(client.foldersApi.constructor.name);
        expect(client.hasOwnProperty('listsApi')).toBeTruthy();
        expect(index_1.ListsApi.name).toBe(client.listsApi.constructor.name);
        expect(client.hasOwnProperty('mappingApi')).toBeTruthy();
        expect(index_1.MappingApi.name).toBe(client.mappingApi.constructor.name);
        expect(client.hasOwnProperty('membershipsApi')).toBeTruthy();
        expect(index_1.MembershipsApi.name).toBe(client.membershipsApi.constructor.name);
    });
});
//# sourceMappingURL=lists.spec.js.map