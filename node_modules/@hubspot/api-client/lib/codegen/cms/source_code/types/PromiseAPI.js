"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseValidationApi = exports.PromiseMetadataApi = exports.PromiseExtractApi = exports.PromiseContentApi = void 0;
const ObservableAPI_1 = require("./ObservableAPI");
class PromiseContentApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_1.ObservableContentApi(configuration, requestFactory, responseProcessor);
    }
    archiveWithHttpInfo(environment, path, _options) {
        const result = this.api.archiveWithHttpInfo(environment, path, _options);
        return result.toPromise();
    }
    archive(environment, path, _options) {
        const result = this.api.archive(environment, path, _options);
        return result.toPromise();
    }
    createWithHttpInfo(environment, path, file, _options) {
        const result = this.api.createWithHttpInfo(environment, path, file, _options);
        return result.toPromise();
    }
    create(environment, path, file, _options) {
        const result = this.api.create(environment, path, file, _options);
        return result.toPromise();
    }
    createOrUpdateWithHttpInfo(environment, path, file, _options) {
        const result = this.api.createOrUpdateWithHttpInfo(environment, path, file, _options);
        return result.toPromise();
    }
    createOrUpdate(environment, path, file, _options) {
        const result = this.api.createOrUpdate(environment, path, file, _options);
        return result.toPromise();
    }
    downloadWithHttpInfo(environment, path, _options) {
        const result = this.api.downloadWithHttpInfo(environment, path, _options);
        return result.toPromise();
    }
    download(environment, path, _options) {
        const result = this.api.download(environment, path, _options);
        return result.toPromise();
    }
}
exports.PromiseContentApi = PromiseContentApi;
const ObservableAPI_2 = require("./ObservableAPI");
class PromiseExtractApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_2.ObservableExtractApi(configuration, requestFactory, responseProcessor);
    }
    doAsyncWithHttpInfo(fileExtractRequest, _options) {
        const result = this.api.doAsyncWithHttpInfo(fileExtractRequest, _options);
        return result.toPromise();
    }
    doAsync(fileExtractRequest, _options) {
        const result = this.api.doAsync(fileExtractRequest, _options);
        return result.toPromise();
    }
    getAsyncStatusWithHttpInfo(taskId, _options) {
        const result = this.api.getAsyncStatusWithHttpInfo(taskId, _options);
        return result.toPromise();
    }
    getAsyncStatus(taskId, _options) {
        const result = this.api.getAsyncStatus(taskId, _options);
        return result.toPromise();
    }
}
exports.PromiseExtractApi = PromiseExtractApi;
const ObservableAPI_3 = require("./ObservableAPI");
class PromiseMetadataApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_3.ObservableMetadataApi(configuration, requestFactory, responseProcessor);
    }
    getWithHttpInfo(environment, path, properties, _options) {
        const result = this.api.getWithHttpInfo(environment, path, properties, _options);
        return result.toPromise();
    }
    get(environment, path, properties, _options) {
        const result = this.api.get(environment, path, properties, _options);
        return result.toPromise();
    }
}
exports.PromiseMetadataApi = PromiseMetadataApi;
const ObservableAPI_4 = require("./ObservableAPI");
class PromiseValidationApi {
    constructor(configuration, requestFactory, responseProcessor) {
        this.api = new ObservableAPI_4.ObservableValidationApi(configuration, requestFactory, responseProcessor);
    }
    doValidateWithHttpInfo(path, file, _options) {
        const result = this.api.doValidateWithHttpInfo(path, file, _options);
        return result.toPromise();
    }
    doValidate(path, file, _options) {
        const result = this.api.doValidate(path, file, _options);
        return result.toPromise();
    }
}
exports.PromiseValidationApi = PromiseValidationApi;
//# sourceMappingURL=PromiseAPI.js.map