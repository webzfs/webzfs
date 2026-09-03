(function (global) {
    "use strict";

    const PACKAGE_STATIC_PREFIX = "static/";
    const WEBZFS_ORIGINS = new Set([
        "http://127.0.0.1:26619",
        "http://localhost:26619",
    ]);

    function rewriteStaticUrl(value) {
        if (!value) {
            return value;
        }

        const stringValue = String(value);
        const rootRelativeMatch = stringValue.match(/^\/static\/(.*)$/);
        if (rootRelativeMatch) {
            return `${PACKAGE_STATIC_PREFIX}${rootRelativeMatch[1]}`;
        }

        try {
            const parsed = new URL(stringValue);
            if (WEBZFS_ORIGINS.has(parsed.origin) && parsed.pathname.startsWith("/static/")) {
                return `${PACKAGE_STATIC_PREFIX}${parsed.pathname.slice("/static/".length)}${parsed.search}${parsed.hash}`;
            }
        } catch (error) {
            return value;
        }

        return value;
    }

    function rewriteSrcset(value) {
        return String(value)
            .split(",")
            .map(candidate => {
                const parts = candidate.trim().split(/\s+/, 2);
                parts[0] = rewriteStaticUrl(parts[0]);
                return parts.join(" ");
            })
            .join(", ");
    }

    function rewriteDocumentAssets(documentNode) {
        documentNode.querySelectorAll("[src]").forEach(element => {
            element.setAttribute("src", rewriteStaticUrl(element.getAttribute("src")));
        });
        documentNode.querySelectorAll("[href]").forEach(element => {
            element.setAttribute("href", rewriteStaticUrl(element.getAttribute("href")));
        });
        documentNode.querySelectorAll("[srcset]").forEach(element => {
            element.setAttribute("srcset", rewriteSrcset(element.getAttribute("srcset")));
        });
        documentNode.querySelectorAll('[hx-target="body"], [data-hx-target="body"]').forEach(
            element => {
                if (element.getAttribute("hx-target") === "body") {
                    element.setAttribute("hx-target", "#webzfs-root");
                }
                if (element.getAttribute("data-hx-target") === "body") {
                    element.setAttribute("data-hx-target", "#webzfs-root");
                }
                if (element.getAttribute("hx-swap") === "outerHTML") {
                    element.setAttribute("hx-swap", "innerHTML");
                }
                if (element.getAttribute("data-hx-swap") === "outerHTML") {
                    element.setAttribute("data-hx-swap", "innerHTML");
                }
            }
        );
        documentNode.querySelectorAll("[hx-push-url], [data-hx-push-url]").forEach(element => {
            if (element.hasAttribute("hx-push-url")) {
                element.setAttribute("hx-push-url", "false");
            }
            if (element.hasAttribute("data-hx-push-url")) {
                element.setAttribute("data-hx-push-url", "false");
            }
        });
        documentNode.querySelectorAll("*").forEach(element => {
            Array.from(element.attributes || []).forEach(attribute => {
                const isBehavior = attribute.name.startsWith("on")
                    || attribute.name.startsWith("x-on:")
                    || attribute.name.startsWith("@")
                    || attribute.name.startsWith("hx-on:");
                if (!isBehavior) {
                    return;
                }
                let value = attribute.value;
                value = value.replace(/(?:window\.)?location\.reload\(\)/g, "WebZFSApp.reload()" );
                value = value.replace(
                    /window\.location\.href\s*=\s*([^;]+);?/g,
                    "WebZFSApp.navigate($1)"
                );
                if (/^on[a-z]+$/i.test(attribute.name)) {
                    element.setAttribute(`data-webzfs-${attribute.name.toLowerCase()}`, value);
                    element.removeAttribute(attribute.name);
                } else {
                    element.setAttribute(attribute.name, value);
                }
            });
        });
        return documentNode;
    }

    function rewriteResponseHtml(html) {
        return String(html || "")
            .replace(
                /((?:src|href|srcset)=["'])http:\/\/(?:127\.0\.0\.1|localhost):26619\/static\//gi,
                "$1static/"
            )
            .replace(/((?:src|href|srcset)=["'])\/static\//gi, "$1static/")
            .replace(/((?:hx-target|data-hx-target)=["'])body(["'])/gi, "$1#webzfs-root$2")
            .replace(
                /((?:hx-swap|data-hx-swap)=["'])outerHTML(["'])/gi,
                "$1innerHTML$2"
            )
            .replace(
                /((?:hx-push-url|data-hx-push-url)=["'])[^"']*(["'])/gi,
                "$1false$2"
            )
            .replace(/(?:window\.)?location\.reload\(\)/g, "WebZFSApp.reload()")
            .replace(
                /window\.location\.href\s*=\s*([^;&quot;]+);?/g,
                "WebZFSApp.navigate($1)"
            );
    }

    function getPagePresentation(documentNode) {
        const themeLink = Array.from(documentNode.querySelectorAll("link[href]")).find(
            link => /(?:\/static\/|^static\/)css\/themes\/[^?]+\.css/.test(
                link.getAttribute("href")
            )
        );
        return {
            bodyClass: documentNode.body ? documentNode.body.className : "",
            themeUrl: themeLink ? rewriteStaticUrl(themeLink.getAttribute("href")) : null,
            title: documentNode.title || "WebZFS",
        };
    }

    const api = {
        getPagePresentation,
        rewriteDocumentAssets,
        rewriteResponseHtml,
        rewriteSrcset,
        rewriteStaticUrl,
    };

    global.WebZFSAssets = api;
    if (typeof module !== "undefined" && module.exports) {
        module.exports = api;
    }
})(typeof window !== "undefined" ? window : globalThis);