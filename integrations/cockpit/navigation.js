(function (global) {
    "use strict";

    const CANONICAL_DIRECTORY_PATHS = new Set([
        "/utils",
        "/utils/files",
        "/utils/health",
        "/utils/logs",
        "/utils/services",
        "/utils/settings",
        "/utils/shell",
        "/utils/smart",
        "/utils/ssh",
        "/utils/support-bundle",
        "/utils/text",
    ]);

    function canonicalizePagePath(path) {
        const normalizedPath = global.WebZFSTransport.normalizePath(path);
        const parsed = new URL(normalizedPath, global.WebZFSTransport.WEBZFS_PARSE_ORIGIN);
        if (CANONICAL_DIRECTORY_PATHS.has(parsed.pathname)) {
            parsed.pathname += "/";
        }
        return parsed.pathname + parsed.search + parsed.hash;
    }

    function isInternalLink(anchor) {
        if (!anchor) {
            return false;
        }
        const target = anchor.getAttribute("target");
        if (target && target !== "_self") {
            return false;
        }
        const href = anchor.getAttribute("href");
        if (!href || href.startsWith("#") || href.startsWith("mailto:") || href.startsWith("tel:")) {
            return false;
        }

        try {
            global.WebZFSTransport.normalizePath(href);
            return true;
        } catch (error) {
            return false;
        }
    }

    function isDownloadLink(anchor) {
        if (!anchor) {
            return false;
        }
        const href = anchor.getAttribute("href") || "";
        return anchor.hasAttribute("download")
            || /(?:^|\/)download(?:[-/]|\?|$)|\/download-all(?:\?|$)|\/(?:error-log|bug-report)(?:\?|$)/.test(href);
    }

    function isDownloadForm(form) {
        const action = form.getAttribute("action") || "";
        return action === "/utils/support-bundle/generate"
            || action === "/utils/settings/backup/export";
    }

    function serializeForm(form, submitter) {
        const formData = new FormData(form);
        if (submitter && submitter.name) {
            formData.append(submitter.name, submitter.value);
        }

        const contentType = (form.enctype || "application/x-www-form-urlencoded").toLowerCase();
        if (contentType === "multipart/form-data") {
            throw new Error("Multipart form submission is not supported by the Cockpit POC.");
        }

        if (contentType === "text/plain") {
            const lines = [];
            formData.forEach((value, name) => lines.push(`${name}=${value}`));
            return { body: lines.join("\r\n"), contentType: "text/plain" };
        }

        return {
            body: new URLSearchParams(formData).toString(),
            contentType: "application/x-www-form-urlencoded;charset=UTF-8",
        };
    }

    function submitForm(form, submitter, loadPage, download, showError) {
        try {
            const method = (form.method || "GET").toUpperCase();
            const action = form.getAttribute("action") || "/";
            if (isDownloadForm(form)) {
                const serialized = serializeForm(form, submitter);
                download(action, {
                    method,
                    body: serialized.body,
                    headers: { "Content-Type": serialized.contentType },
                });
                return;
            }
            if (method === "GET") {
                const serialized = serializeForm(form, submitter);
                const separator = action.includes("?") ? "&" : "?";
                loadPage(canonicalizePagePath(`${action}${separator}${serialized.body}`));
                return;
            }

            const serialized = serializeForm(form, submitter);
            loadPage(action, {
                method,
                body: serialized.body,
                headers: { "Content-Type": serialized.contentType },
            });
        } catch (error) {
            showError(error.message);
        }
    }

    function install(root, loadPage, download, showError) {
        const originalSubmit = global.HTMLFormElement.prototype.submit;
        if (!global.HTMLFormElement.prototype.__webzfsSubmitPatched) {
            global.HTMLFormElement.prototype.submit = function () {
                if (root.contains(this)) {
                    submitForm(this, null, loadPage, download, showError);
                    return;
                }
                return originalSubmit.call(this);
            };
            global.HTMLFormElement.prototype.__webzfsSubmitPatched = true;
        }

        root.addEventListener("click", event => {
            const anchor = event.target.closest("a");
            if (isDownloadLink(anchor) && isInternalLink(anchor)) {
                event.preventDefault();
                event.stopImmediatePropagation();
                download(anchor.getAttribute("href"));
                return;
            }
            if (!isInternalLink(anchor) || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
                return;
            }

            event.preventDefault();
            event.stopImmediatePropagation();
            loadPage(canonicalizePagePath(anchor.getAttribute("href")));
        }, true);

        root.addEventListener("submit", event => {
            const form = event.target;
            if (!(form instanceof HTMLFormElement)) {
                return;
            }
            const isLoginForm = form.id === "login-form";
            const submitter = event.submitter;
            const isHtmxForm = form.hasAttribute("hx-post")
                || form.hasAttribute("hx-get")
                || Boolean(submitter && (
                    submitter.hasAttribute("hx-post") || submitter.hasAttribute("hx-get")
                ))
                || Boolean(form.querySelector('[type="submit"][hx-post], [type="submit"][hx-get]'));
            if (isHtmxForm && !isLoginForm) {
                return;
            }

            const submitHandler = form.getAttribute("data-webzfs-onsubmit")
                || form.getAttribute("onsubmit");
            if (submitHandler) {
                const result = global.Function(`return (function(event) { ${submitHandler} }).call(this, event);`)
                    .call(form, event);
                if (result === false) {
                    event.preventDefault();
                    event.stopImmediatePropagation();
                    return;
                }
            }

            event.preventDefault();
            event.stopImmediatePropagation();
            submitForm(form, event.submitter, loadPage, download, showError);
        }, true);
    }

    const api = {
        canonicalizePagePath,
        install,
        isDownloadForm,
        isDownloadLink,
        isInternalLink,
        serializeForm,
        submitForm,
    };
    global.WebZFSNavigation = api;
    if (typeof module !== "undefined" && module.exports) {
        module.exports = api;
    }
})(typeof window !== "undefined" ? window : globalThis);