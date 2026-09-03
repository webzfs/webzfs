(function (global) {
    "use strict";

    const WEBZFS_PORT = 26619;
    const WEBZFS_PARSE_ORIGIN = `http://127.0.0.1:${WEBZFS_PORT}`;
    const WEBZFS_ORIGINS = new Set([
        WEBZFS_PARSE_ORIGIN,
        `http://localhost:${WEBZFS_PORT}`,
    ]);
    let httpClient = null;
    let binaryHttpClient = null;

    function normalizePath(url) {
        const parsed = new URL(url || "/", WEBZFS_PARSE_ORIGIN);
        if (!WEBZFS_ORIGINS.has(parsed.origin)) {
            throw new Error(`Not a WebZFS URL: ${url}`);
        }
        return parsed.pathname + parsed.search + parsed.hash;
    }

    function normalizeHeaders(headers) {
        const result = {};
        if (headers && typeof headers.forEach === "function") {
            headers.forEach((value, name) => {
                result[name] = String(value);
            });
            return result;
        }
        Object.entries(headers || {}).forEach(([name, value]) => {
            if (value !== null && value !== undefined) {
                result[name] = Array.isArray(value) ? value.join(", ") : String(value);
            }
        });
        return result;
    }

    function createRequestOptions(method, headers, body) {
        const options = {
            method: String(method || "GET").toUpperCase(),
            headers: global.WebZFSSession.addCookieHeader(normalizeHeaders(headers)),
        };
        if (body !== undefined && body !== null) {
            options.body = body;
        }
        return options;
    }

    function request(method, path, options) {
        const requestOptions = options || {};
        const normalizedPath = normalizePath(path);
        const cockpitRequest = createRequestOptions(
            method,
            requestOptions.headers,
            requestOptions.body === undefined ? "" : requestOptions.body
        );
        cockpitRequest.path = normalizedPath;
        const httpRequest = httpClient.request(cockpitRequest);
        let responseStatus = null;
        let responseHeaders = {};

        httpRequest.response((status, headers) => {
            responseStatus = status;
            responseHeaders = headers || {};
            global.WebZFSSession.updateFromHeaders(responseHeaders);
        });

        return new Promise((resolve, reject) => {
            httpRequest.done(body => {
                resolve({
                    body,
                    headers: responseHeaders,
                    path: normalizedPath,
                    status: responseStatus === null ? 200 : responseStatus,
                });
            });
            httpRequest.fail((error, body) => {
                if (error && error.status !== undefined) {
                    responseStatus = error.status;
                }
                reject({
                    body: body || "",
                    error,
                    headers: responseHeaders,
                    path: normalizedPath,
                    status: responseStatus,
                });
            });
        });
    }

    function requestBinary(method, path, options) {
        const requestOptions = options || {};
        const normalizedPath = normalizePath(path);
        const headers = global.WebZFSSession.addCookieHeader(
            normalizeHeaders(requestOptions.headers)
        );
        let body = requestOptions.body;
        if (typeof body === "string") {
            body = new global.TextEncoder().encode(body);
        }
        if (body === undefined || body === null) {
            body = new global.Uint8Array(0);
        }
        const httpRequest = binaryHttpClient.request({
            method: String(method || "GET").toUpperCase(),
            path: normalizedPath,
            headers,
            body,
        });
        let responseStatus = null;
        let responseHeaders = {};

        httpRequest.response((status, receivedHeaders) => {
            responseStatus = status;
            responseHeaders = receivedHeaders || {};
            global.WebZFSSession.updateFromHeaders(responseHeaders);
        });

        return new Promise((resolve, reject) => {
            httpRequest.done(responseBody => {
                resolve({
                    body: responseBody,
                    headers: responseHeaders,
                    path: normalizedPath,
                    status: responseStatus === null ? 200 : responseStatus,
                });
            });
            httpRequest.fail((error, responseBody) => {
                reject({
                    body: responseBody,
                    error,
                    headers: responseHeaders,
                    path: normalizedPath,
                    status: error && error.status !== undefined ? error.status : responseStatus,
                });
            });
        });
    }

    function followRedirect(response) {
        const location = global.WebZFSSession.getHeader(response.headers, "location");
        if (response.status >= 300 && response.status < 400 && location) {
            return normalizePath(location);
        }
        return null;
    }

    function buildExternalChannelUrl(method, url, headers) {
        const path = normalizePath(url);
        const channel = {
            payload: "http-stream2",
            method: String(method || "GET").toUpperCase(),
            port: WEBZFS_PORT,
            path,
            headers: global.WebZFSSession.addCookieHeader(normalizeHeaders(headers)),
        };
        const encodedChannel = global.btoa(JSON.stringify(channel));
        return `/cockpit/channel/${global.cockpit.transport.csrf_token}?${encodedChannel}`;
    }

    function createEvent(type, details) {
        if (typeof global.ProgressEvent === "function" && details) {
            return new global.ProgressEvent(type, details);
        }
        if (typeof global.Event === "function") {
            return new global.Event(type);
        }
        return Object.assign({ type }, details || {});
    }

    class SimpleEventTarget {
        constructor() {
            this.listeners = {};
        }

        addEventListener(type, callback) {
            if (!callback) {
                return;
            }
            if (!this.listeners[type]) {
                this.listeners[type] = [];
            }
            this.listeners[type].push(callback);
        }

        removeEventListener(type, callback) {
            const callbacks = this.listeners[type] || [];
            this.listeners[type] = callbacks.filter(candidate => candidate !== callback);
        }

        dispatchEvent(event) {
            const callbacks = (this.listeners[event.type] || []).slice();
            callbacks.forEach(callback => callback.call(this, event));
            const handler = this[`on${event.type}`];
            if (typeof handler === "function") {
                handler.call(this, event);
            }
            return true;
        }
    }

    function responseHeadersToString(headers) {
        return Object.entries(headers || {})
            .map(([name, value]) => `${name}: ${Array.isArray(value) ? value.join(", ") : value}`)
            .join("\r\n");
    }

    function deleteHeader(headers, name) {
        const expectedName = name.toLowerCase();
        Object.keys(headers || {}).forEach(headerName => {
            if (headerName.toLowerCase() === expectedName) {
                delete headers[headerName];
            }
        });
    }

    function setHeader(headers, name, value) {
        deleteHeader(headers, name);
        headers[name] = value;
    }

    async function requestFollowingRedirects(method, path, options, redirectMode) {
        let currentMethod = String(method || "GET").toUpperCase();
        let currentPath = normalizePath(path);
        let currentOptions = Object.assign({}, options || {}, {
            headers: Object.assign({}, options && options.headers ? options.headers : {}),
        });

        for (let redirectCount = 0; redirectCount < 10; redirectCount += 1) {
            try {
                const response = await request(currentMethod, currentPath, currentOptions);
                response.redirected = redirectCount > 0;
                return response;
            } catch (failure) {
                const redirectPath = followRedirect(failure);
                if (!redirectPath || redirectMode === "manual") {
                    throw failure;
                }
                currentPath = redirectPath;
                if ([301, 302, 303].includes(failure.status)) {
                    currentMethod = "GET";
                    currentOptions = { headers: currentOptions.headers || {}, body: "" };
                    delete currentOptions.headers["Content-Type"];
                    delete currentOptions.headers["content-type"];
                }
            }
        }
        throw new Error("Too many WebZFS redirects.");
    }

    class CockpitXMLHttpRequest extends SimpleEventTarget {
        constructor() {
            super();
            this.upload = new SimpleEventTarget();
            this.readyState = CockpitXMLHttpRequest.UNSENT;
            this.response = "";
            this.responseText = "";
            this.responseType = "";
            this.responseURL = "";
            this.status = 0;
            this.statusText = "";
            this.timeout = 0;
            this.withCredentials = false;
            this.requestHeaders = {};
            this.responseHeaders = {};
            this.aborted = false;
            this.timeoutTimer = null;
        }

        open(method, url, async) {
            if (async === false) {
                throw new Error("Synchronous XMLHttpRequest is not supported in Cockpit.");
            }
            this.method = String(method || "GET").toUpperCase();
            this.path = normalizePath(url);
            this.readyState = CockpitXMLHttpRequest.OPENED;
            this.dispatchEvent(createEvent("readystatechange"));
        }

        setRequestHeader(name, value) {
            const headerName = String(name);
            if (this.requestHeaders[headerName]) {
                this.requestHeaders[headerName] += `, ${value}`;
            } else {
                this.requestHeaders[headerName] = String(value);
            }
        }

        getResponseHeader(name) {
            return global.WebZFSSession.getHeader(this.responseHeaders, name);
        }

        getAllResponseHeaders() {
            return responseHeadersToString(this.responseHeaders);
        }

        overrideMimeType() {}

        abort() {
            if (this.readyState === CockpitXMLHttpRequest.DONE || this.aborted) {
                return;
            }
            this.aborted = true;
            if (this.timeoutTimer) {
                global.clearTimeout(this.timeoutTimer);
            }
            this.readyState = CockpitXMLHttpRequest.DONE;
            this.dispatchEvent(createEvent("readystatechange"));
            this.dispatchEvent(createEvent("abort"));
            this.dispatchEvent(createEvent("loadend"));
        }

        send(body) {
            if (this.readyState !== CockpitXMLHttpRequest.OPENED) {
                throw new Error("XMLHttpRequest.open() must be called before send().");
            }
            this.dispatchEvent(createEvent("loadstart"));
            this.upload.dispatchEvent(createEvent("loadstart"));

            if (this.timeout > 0) {
                this.timeoutTimer = global.setTimeout(() => {
                    if (this.readyState !== CockpitXMLHttpRequest.DONE && !this.aborted) {
                        this.aborted = true;
                        this.readyState = CockpitXMLHttpRequest.DONE;
                        this.dispatchEvent(createEvent("readystatechange"));
                        this.dispatchEvent(createEvent("timeout"));
                        this.dispatchEvent(createEvent("loadend"));
                    }
                }, this.timeout);
            }

            const requestHeaders = normalizeHeaders(this.requestHeaders);
            let requestBody;
            try {
                requestBody = convertFetchBody(body, requestHeaders);
            } catch (error) {
                this.errorMessage = error.message;
                this.readyState = CockpitXMLHttpRequest.DONE;
                this.dispatchEvent(createEvent("readystatechange"));
                this.dispatchEvent(createEvent("error"));
                this.dispatchEvent(createEvent("loadend"));
                return;
            }

            requestFollowingRedirects(
                this.method,
                this.path,
                { headers: requestHeaders, body: requestBody }
            ).then(response => {
                if (this.aborted) {
                    return;
                }
                if (this.timeoutTimer) {
                    global.clearTimeout(this.timeoutTimer);
                }
                this.status = response.status;
                this.responseHeaders = response.headers || {};
                if (response.redirected) {
                    setHeader(this.responseHeaders, "X-WebZFS-Full-Page", "true");
                }
                const htmxRedirect = global.WebZFSSession.getHeader(
                    this.responseHeaders,
                    "HX-Redirect"
                );
                const htmxRefresh = global.WebZFSSession.getHeader(
                    this.responseHeaders,
                    "HX-Refresh"
                );
                if (htmxRedirect) {
                    this.responseURL = `${WEBZFS_PARSE_ORIGIN}${normalizePath(htmxRedirect)}`;
                    setHeader(this.responseHeaders, "X-WebZFS-Full-Page", "true");
                    deleteHeader(this.responseHeaders, "HX-Redirect");
                }
                if (htmxRefresh === "true") {
                    setHeader(this.responseHeaders, "X-WebZFS-Full-Page", "true");
                    deleteHeader(this.responseHeaders, "HX-Refresh");
                }
                if (!htmxRedirect) {
                    this.responseURL = `${WEBZFS_PARSE_ORIGIN}${response.path}`;
                }
                this.responseText = global.WebZFSAssets
                    ? global.WebZFSAssets.rewriteResponseHtml(response.body)
                    : response.body;
                this.response = this.responseText;
                this.readyState = CockpitXMLHttpRequest.DONE;
                this.dispatchEvent(createEvent("readystatechange"));
                this.dispatchEvent(createEvent("progress", {
                    lengthComputable: true,
                    loaded: this.responseText.length,
                    total: this.responseText.length,
                }));
                this.dispatchEvent(createEvent("load"));
                this.dispatchEvent(createEvent("loadend"));
            }).catch(failure => {
                if (this.aborted) {
                    return;
                }
                if (this.timeoutTimer) {
                    global.clearTimeout(this.timeoutTimer);
                }
                this.status = failure && failure.status ? failure.status : 0;
                this.responseHeaders = failure && failure.headers ? failure.headers : {};
                this.responseText = failure && failure.body ? failure.body : "";
                this.response = this.responseText;
                this.readyState = CockpitXMLHttpRequest.DONE;
                this.dispatchEvent(createEvent("readystatechange"));
                if (this.status > 0) {
                    this.dispatchEvent(createEvent("load"));
                } else {
                    this.dispatchEvent(createEvent("error"));
                }
                this.dispatchEvent(createEvent("loadend"));
            });
        }
    }

    CockpitXMLHttpRequest.UNSENT = 0;
    CockpitXMLHttpRequest.OPENED = 1;
    CockpitXMLHttpRequest.HEADERS_RECEIVED = 2;
    CockpitXMLHttpRequest.LOADING = 3;
    CockpitXMLHttpRequest.DONE = 4;
    Object.assign(CockpitXMLHttpRequest.prototype, {
        UNSENT: 0,
        OPENED: 1,
        HEADERS_RECEIVED: 2,
        LOADING: 3,
        DONE: 4,
    });

    function convertFetchBody(body, headers) {
        if (body instanceof global.URLSearchParams) {
            if (!global.WebZFSSession.getHeader(headers, "content-type")) {
                headers["Content-Type"] = "application/x-www-form-urlencoded;charset=UTF-8";
            }
            return body.toString();
        }
        if (typeof global.FormData === "function" && body instanceof global.FormData) {
            const parameters = new global.URLSearchParams();
            for (const [name, value] of body.entries()) {
                if (typeof value !== "string") {
                    throw new Error("Multipart file uploads are not supported through Cockpit yet.");
                }
                parameters.append(name, value);
            }
            headers["Content-Type"] = "application/x-www-form-urlencoded;charset=UTF-8";
            return parameters.toString();
        }
        return body === undefined || body === null ? "" : body;
    }

    function createFetchResponse(response) {
        const headers = typeof global.Headers === "function"
            ? new global.Headers(response.headers || {})
            : response.headers || {};
        return {
            bodyUsed: false,
            headers,
            ok: response.status >= 200 && response.status < 300,
            redirected: Boolean(response.redirected),
            status: response.status,
            statusText: "",
            type: "basic",
            url: `${WEBZFS_PARSE_ORIGIN}${response.path}`,
            clone() {
                return createFetchResponse(response);
            },
            async json() {
                this.bodyUsed = true;
                return JSON.parse(response.body || "null");
            },
            async text() {
                this.bodyUsed = true;
                return response.body || "";
            },
        };
    }

    function cockpitFetch(url, options) {
        const requestOptions = options || {};
        const headers = normalizeHeaders(requestOptions.headers);
        const body = convertFetchBody(requestOptions.body, headers);
        return requestFollowingRedirects(
            requestOptions.method || "GET",
            url,
            { headers, body },
            requestOptions.redirect
        ).then(createFetchResponse).catch(failure => {
            if (failure && failure.status) {
                return createFetchResponse(failure);
            }
            throw failure;
        });
    }

    class CockpitEventSource extends SimpleEventTarget {
        constructor(url) {
            super();
            this.url = normalizePath(url);
            this.readyState = CockpitEventSource.CONNECTING;
            this.withCredentials = false;
            this.buffer = "";
            this.connect();
        }

        connect() {
            const requestOptions = {
                method: "GET",
                path: this.url,
                body: "",
                headers: global.WebZFSSession.addCookieHeader({ Accept: "text/event-stream" }),
            };
            this.httpRequest = httpClient.request(requestOptions);
            this.httpRequest.response(status => {
                if (status >= 200 && status < 300) {
                    this.readyState = CockpitEventSource.OPEN;
                    this.dispatchEvent(createEvent("open"));
                }
            });
            this.httpRequest.stream(chunk => {
                this.buffer += chunk;
                this.flushEvents();
                return chunk.length;
            });
            this.httpRequest.fail(() => {
                if (this.readyState !== CockpitEventSource.CLOSED) {
                    this.readyState = CockpitEventSource.CLOSED;
                    this.dispatchEvent(createEvent("error"));
                }
            });
        }

        flushEvents() {
            let boundary = this.buffer.search(/\r?\n\r?\n/);
            while (boundary >= 0) {
                const block = this.buffer.slice(0, boundary);
                const separator = this.buffer.match(/\r?\n\r?\n/)[0];
                this.buffer = this.buffer.slice(boundary + separator.length);
                const data = block
                    .split(/\r?\n/)
                    .filter(line => line.startsWith("data:"))
                    .map(line => line.slice(5).trimStart())
                    .join("\n");
                if (data) {
                    const event = createEvent("message");
                    event.data = data;
                    this.dispatchEvent(event);
                }
                boundary = this.buffer.search(/\r?\n\r?\n/);
            }
        }

        close() {
            this.readyState = CockpitEventSource.CLOSED;
            if (this.httpRequest && typeof this.httpRequest.close === "function") {
                this.httpRequest.close();
            }
        }
    }

    CockpitEventSource.CONNECTING = 0;
    CockpitEventSource.OPEN = 1;
    CockpitEventSource.CLOSED = 2;

    function initialize() {
        httpClient = global.cockpit.http(WEBZFS_PORT);
        binaryHttpClient = global.cockpit.http(WEBZFS_PORT, { binary: true });
        global.XMLHttpRequest = CockpitXMLHttpRequest;
        global.fetch = cockpitFetch;
        global.EventSource = CockpitEventSource;
    }

    const api = {
        WEBZFS_PORT,
        WEBZFS_PARSE_ORIGIN,
        buildExternalChannelUrl,
        CockpitEventSource,
        CockpitXMLHttpRequest,
        cockpitFetch,
        followRedirect,
        initialize,
        normalizeHeaders,
        normalizePath,
        request,
        requestBinary,
        requestFollowingRedirects,
    };

    global.WebZFSTransport = api;
    if (typeof module !== "undefined" && module.exports) {
        module.exports = api;
    }
})(typeof window !== "undefined" ? window : globalThis);