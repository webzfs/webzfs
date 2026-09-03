(function (global) {
    "use strict";

    const root = document.getElementById("webzfs-root");
    const status = document.getElementById("webzfs-adapter-status");
    const themeStylesheet = document.getElementById("webzfs-theme-stylesheet");
    let currentPath = "/";
    let pageCleanupCallbacks = [];
    let pageReadyCallbacks = [];
    let pageAlpineCallbacks = [];
    const fragmentScripts = new WeakMap();
    const loadedExternalScripts = new Set();

    function setStatus(message, isError) {
        status.textContent = message;
        status.classList.toggle("webzfs-adapter-error", Boolean(isError));
        status.hidden = !message;
    }

    function showError(message) {
        setStatus(message, true);
        if (!root.hasChildNodes()) {
            root.innerHTML = `
                <main class="webzfs-adapter-error-page">
                    <h1>WebZFS is unavailable</h1>
                    <p>${escapeHtml(message)}</p>
                    <button class="btn-primary" type="button" id="webzfs-retry">Retry</button>
                </main>
            `;
            document.getElementById("webzfs-retry").addEventListener("click", () => loadPage(currentPath));
        }
    }

    function escapeHtml(value) {
        const element = document.createElement("div");
        element.textContent = value;
        return element.innerHTML;
    }

    function applyPresentation(presentation) {
        document.title = presentation.title;
        if (presentation.bodyClass) {
            const adapterClasses = Array.from(document.body.classList).filter(
                className => className.startsWith("webzfs-adapter-")
            );
            document.body.className = presentation.bodyClass;
            document.body.classList.add(...adapterClasses);
        }
        if (presentation.themeUrl) {
            themeStylesheet.setAttribute("href", presentation.themeUrl);
        }
    }

    function cleanupPageRuntime() {
        if (global.Alpine && typeof global.Alpine.destroyTree === "function") {
            global.Alpine.destroyTree(root);
        }
        pageCleanupCallbacks.forEach(callback => callback());
        pageCleanupCallbacks = [];
        pageReadyCallbacks = [];
        pageAlpineCallbacks = [];
    }

    function addTrackedListener(target, type, callback, options) {
        target.addEventListener(type, callback, options);
        pageCleanupCallbacks.push(() => target.removeEventListener(type, callback, options));
    }

    function createPersistentElementProxy(element) {
        return new Proxy(element, {
            get(target, property) {
                if (property === "addEventListener") {
                    return (type, callback, options) => addTrackedListener(
                        target,
                        type,
                        callback,
                        options
                    );
                }
                if (property === "removeEventListener") {
                    return target.removeEventListener.bind(target);
                }
                const value = target[property];
                return typeof value === "function" ? value.bind(target) : value;
            },
            set(target, property, value) {
                target[property] = value;
                return true;
            },
        });
    }

    function createDocumentProxy() {
        const bodyProxy = createPersistentElementProxy(document.body);
        const documentElementProxy = createPersistentElementProxy(document.documentElement);
        return new Proxy({}, {
            get(target, property) {
                if (property === "body") {
                    return bodyProxy;
                }
                if (property === "documentElement") {
                    return documentElementProxy;
                }
                if (property === "addEventListener") {
                    return (type, callback, options) => {
                        if (type === "DOMContentLoaded") {
                            pageReadyCallbacks.push(callback);
                        } else if (type === "alpine:init") {
                            pageAlpineCallbacks.push(callback);
                        } else {
                            addTrackedListener(document, type, callback, options);
                        }
                    };
                }
                if (property === "removeEventListener") {
                    return document.removeEventListener.bind(document);
                }
                const value = document[property];
                return typeof value === "function" ? value.bind(document) : value;
            },
            set(target, property, value) {
                document[property] = value;
                return true;
            },
        });
    }

    function createWindowProxy() {
        const trackedTimeout = (callback, delay, ...args) => {
            const timer = global.setTimeout(callback, delay, ...args);
            pageCleanupCallbacks.push(() => global.clearTimeout(timer));
            return timer;
        };
        const trackedInterval = (callback, delay, ...args) => {
            const timer = global.setInterval(callback, delay, ...args);
            pageCleanupCallbacks.push(() => global.clearInterval(timer));
            return timer;
        };
        const TrackedMutationObserver = class extends global.MutationObserver {
            constructor(callback) {
                super(callback);
                pageCleanupCallbacks.push(() => this.disconnect());
            }
        };
        const locationProxy = {
            get href() {
                return `${global.WebZFSTransport.WEBZFS_PARSE_ORIGIN}${currentPath}`;
            },
            set href(path) {
                loadPage(path);
            },
            assign(path) {
                loadPage(path);
            },
            reload() {
                loadPage(currentPath);
            },
            replace(path) {
                loadPage(path);
            },
        };
        return new Proxy({}, {
            get(target, property) {
                if (property === "window" || property === "self" || property === "top" || property === "parent") {
                    return global.WebZFSPageWindow;
                }
                if (property === "document") {
                    return global.WebZFSPageDocument;
                }
                if (property === "location") {
                    return locationProxy;
                }
                if (property === "addEventListener") {
                    return (type, callback, options) => addTrackedListener(global, type, callback, options);
                }
                if (property === "setTimeout") {
                    return trackedTimeout;
                }
                if (property === "setInterval") {
                    return trackedInterval;
                }
                if (property === "MutationObserver") {
                    return TrackedMutationObserver;
                }
                if (property === "removeEventListener") {
                    return global.removeEventListener.bind(global);
                }
                const value = global[property];
                return typeof value === "function" ? value.bind(global) : value;
            },
            set(target, property, value) {
                global[property] = value;
                return true;
            },
        });
    }

    function extractFunctionNames(source) {
        const names = new Set();
        const expression = /(?:^|[;{}\n]\s*)(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(/gm;
        let match = expression.exec(source);
        while (match) {
            names.add(match[1]);
            match = expression.exec(source);
        }
        return Array.from(names);
    }

    function collectScripts(container) {
        const scripts = [];
        container.querySelectorAll("script").forEach(script => {
            const source = script.getAttribute("src");
            const text = script.textContent || "";
            const isBaseHandler = text.includes("htmx:responseError") && text.includes("htmx:timeout");
            if (!isBaseHandler) {
                scripts.push({ source, text });
            }
            script.remove();
        });
        return scripts;
    }

    function loadExternalScript(source) {
        const url = new URL(source, document.baseURI).href;
        if (loadedExternalScripts.has(url)) {
            return Promise.resolve();
        }
        loadedExternalScripts.add(url);
        return new Promise((resolve, reject) => {
            const script = document.createElement("script");
            script.src = url;
            script.onload = resolve;
            script.onerror = () => {
                loadedExternalScripts.delete(url);
                reject(new Error(`Failed to load WebZFS script: ${source}`));
            };
            document.head.appendChild(script);
        });
    }

    function runInlineScript(source) {
        const rewrittenSource = source
            .replace(/(?:window\.)?location\.reload\(\)/g, "window.WebZFSApp.reload()")
            .replace(
                /window\.location\.href\s*=\s*([^;]+);?/g,
                "window.WebZFSApp.navigate($1)"
            );
        const functionNames = extractFunctionNames(rewrittenSource);
        const exports = functionNames
            .map(name => `if (typeof ${name} === "function") window[${JSON.stringify(name)}] = ${name};`)
            .join("\n");
        const runScript = global.Function(
            "window",
            "document",
            "setTimeout",
            "setInterval",
            "clearTimeout",
            "clearInterval",
            "MutationObserver",
            "EventSource",
            `${rewrittenSource}\n${exports}`
        );
        runScript(
            global.WebZFSPageWindow,
            global.WebZFSPageDocument,
            global.WebZFSPageWindow.setTimeout,
            global.WebZFSPageWindow.setInterval,
            global.clearTimeout.bind(global),
            global.clearInterval.bind(global),
            global.WebZFSPageWindow.MutationObserver,
            class extends global.EventSource {
                constructor(url) {
                    super(url);
                    pageCleanupCallbacks.push(() => this.close());
                }
            }
        );
    }

    async function activateScripts(scripts) {
        for (const script of scripts) {
            try {
                if (script.source) {
                    await loadExternalScript(script.source);
                } else if (script.text.trim()) {
                    runInlineScript(script.text);
                }
            } catch (error) {
                console.error("WebZFS page script failed:", error);
                showError(error.message);
            }
        }
    }

    function finishPageRuntime(container) {
        pageAlpineCallbacks.splice(0).forEach(callback => callback.call(document, new Event("alpine:init")));
        if (global.Alpine && typeof global.Alpine.initTree === "function") {
            global.Alpine.initTree(container);
        }
        pageReadyCallbacks.splice(0).forEach(
            callback => callback.call(document, new Event("DOMContentLoaded"))
        );
        bindInlineHandlers(container);
        installModalHelpers();
    }

    function bindInlineHandlers(container) {
        container.querySelectorAll("*").forEach(element => {
            Array.from(element.attributes || []).forEach(attribute => {
                if (!attribute.name.startsWith("data-webzfs-on")) {
                    return;
                }
                const eventType = attribute.name.slice("data-webzfs-on".length);
                if (!eventType || eventType === "submit") {
                    return;
                }
                const runHandler = global.Function("event", attribute.value);
                element.addEventListener(eventType, function (event) {
                    const result = runHandler.call(this, event);
                    if (result === false) {
                        event.preventDefault();
                        event.stopPropagation();
                    }
                });
                element.removeAttribute(attribute.name);
            });
        });
    }

    function setModalOpen(dialogId, open) {
        const dialog = document.getElementById(dialogId);
        if (!dialog) {
            return;
        }
        if (global.Alpine && typeof global.Alpine.$data === "function") {
            const data = global.Alpine.$data(dialog);
            if (data && Object.prototype.hasOwnProperty.call(data, "open")) {
                data.open = open;
                return;
            }
        }
        if (dialog.__x && dialog.__x.$data) {
            dialog.__x.$data.open = open;
            return;
        }
        dialog.style.display = open ? "block" : "none";
    }

    function installModalHelpers() {
        global.showConfirm = dialogId => setModalOpen(dialogId, true);
        global.openModal = dialogId => setModalOpen(dialogId, true);
        global.closeModal = dialogId => setModalOpen(dialogId, false);
        global.showStatus = dialogId => setModalOpen(dialogId, true);
        global.hideStatus = dialogId => setModalOpen(dialogId, false);
    }

    async function renderHtml(html) {
        const parsedDocument = new DOMParser().parseFromString(html, "text/html");
        global.WebZFSAssets.rewriteDocumentAssets(parsedDocument);
        applyPresentation(global.WebZFSAssets.getPagePresentation(parsedDocument));
        const scripts = collectScripts(parsedDocument.body);
        cleanupPageRuntime();
        root.replaceChildren(...Array.from(parsedDocument.body.childNodes));

        global.WebZFSPageDocument = createDocumentProxy();
        global.WebZFSPageWindow = createWindowProxy();
        await activateScripts(scripts);
        finishPageRuntime(root);
        if (global.htmx) {
            global.htmx.process(root);
        }
    }

    async function loadPage(path, options) {
        const requestOptions = options || {};
        currentPath = global.WebZFSTransport.normalizePath(path);
        setStatus("Loading WebZFS...", false);

        try {
            const response = await global.WebZFSTransport.request(
                requestOptions.method || "GET",
                currentPath,
                requestOptions
            );
            const redirectPath = global.WebZFSTransport.followRedirect(response);
            if (redirectPath) {
                return loadPage(redirectPath);
            }
            await renderHtml(response.body);
            setStatus("", false);
        } catch (failure) {
            const redirectPath = failure && global.WebZFSTransport.followRedirect(failure);
            if (redirectPath) {
                return loadPage(redirectPath);
            }
            if (failure && failure.body && failure.status) {
                await renderHtml(failure.body);
                setStatus(`WebZFS returned HTTP ${failure.status}.`, true);
                return;
            }
            showError("WebZFS service could not be reached on localhost:26619.");
        }
    }

    function getDownloadFilename(headers, path) {
        const disposition = global.WebZFSSession.getHeader(headers, "content-disposition") || "";
        const encodedMatch = disposition.match(/filename\*=UTF-8''([^;]+)/i);
        if (encodedMatch) {
            return decodeURIComponent(encodedMatch[1].replace(/["']/g, ""));
        }
        const filenameMatch = disposition.match(/filename="?([^";]+)"?/i);
        if (filenameMatch) {
            return filenameMatch[1];
        }
        const pathname = global.WebZFSTransport.normalizePath(path).split("?")[0];
        return pathname.split("/").filter(Boolean).pop() || "webzfs-download";
    }

    async function download(path, options) {
        const requestOptions = options || {};
        setStatus("Preparing download...", false);
        try {
            const response = await global.WebZFSTransport.requestBinary(
                requestOptions.method || "GET",
                path,
                requestOptions
            );
            const contentType = global.WebZFSSession.getHeader(
                response.headers,
                "content-type"
            ) || "application/octet-stream";
            const blob = new Blob([response.body], { type: contentType });
            const objectUrl = URL.createObjectURL(blob);
            const anchor = document.createElement("a");
            anchor.href = objectUrl;
            anchor.download = getDownloadFilename(response.headers, path);
            document.body.appendChild(anchor);
            anchor.click();
            anchor.remove();
            global.setTimeout(() => URL.revokeObjectURL(objectUrl), 0);
            setStatus("", false);
        } catch (failure) {
            showError(
                failure && failure.status
                    ? `WebZFS download failed with HTTP ${failure.status}.`
                    : "WebZFS download failed."
            );
        }
    }

    function installHtmxErrorHandling() {
        document.body.addEventListener("htmx:responseError", event => {
            const responseStatus = event.detail.xhr ? event.detail.xhr.status : "unknown";
            showError(`WebZFS request failed with status ${responseStatus}.`);
        });
        document.body.addEventListener("htmx:timeout", () => {
            showError("The WebZFS request timed out.");
        });
        document.body.addEventListener("htmx:sendError", event => {
            showError(
                event.detail.xhr && event.detail.xhr.errorMessage
                    ? event.detail.xhr.errorMessage
                    : "The WebZFS request could not be sent through Cockpit."
            );
        });
    }

    function installAssetErrorHandling() {
        (global.WebZFSAssetFailures || []).forEach(asset => {
            showError(`Cockpit could not load the WebZFS package asset: ${asset}`);
        });
        document.querySelectorAll('link[rel="stylesheet"], script[src]').forEach(element => {
            element.addEventListener("error", () => {
                const asset = element.getAttribute("href") || element.getAttribute("src");
                showError(`Cockpit could not load the WebZFS package asset: ${asset}`);
            });
        });
    }

    function prepareFragmentHtml(html, xhr) {
        const parsedDocument = new DOMParser().parseFromString(
            `<body>${global.WebZFSAssets.rewriteResponseHtml(html)}</body>`,
            "text/html"
        );
        global.WebZFSAssets.rewriteDocumentAssets(parsedDocument);
        fragmentScripts.set(xhr, collectScripts(parsedDocument.body));
        return parsedDocument.body.innerHTML;
    }

    function installHtmxCompatibility() {
        document.body.addEventListener("htmx:beforeSwap", event => {
            const xhr = event.detail.xhr;
            if (xhr.getResponseHeader("X-WebZFS-Full-Page") === "true") {
                event.detail.shouldSwap = false;
                const responseUrl = xhr.responseURL || `${global.WebZFSTransport.WEBZFS_PARSE_ORIGIN}${currentPath}`;
                currentPath = global.WebZFSTransport.normalizePath(responseUrl);
                renderHtml(xhr.responseText).then(() => setStatus("", false));
                return;
            }
            event.detail.serverResponse = prepareFragmentHtml(event.detail.serverResponse, xhr);
        });

        document.body.addEventListener("htmx:afterSwap", async event => {
            const scripts = fragmentScripts.get(event.detail.xhr) || [];
            fragmentScripts.delete(event.detail.xhr);
            await activateScripts(scripts);
            finishPageRuntime(event.detail.target || root);
        });

    }

    global.cockpit.transport.wait(() => {
        global.WebZFSTransport.initialize();
        global.WebZFSApp = {
            navigate: loadPage,
            reload: () => loadPage(currentPath),
        };
        global.WebZFSNavigation.install(root, loadPage, download, showError);
        installHtmxCompatibility();
        installHtmxErrorHandling();
        installAssetErrorHandling();
        loadPage("/");
    });
})(window);