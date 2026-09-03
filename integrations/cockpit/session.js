(function (global) {
    "use strict";

    const TOKEN_COOKIE = "token";
    let webzfsToken = null;

    function getHeader(headers, name) {
        if (!headers) {
            return null;
        }

        const expectedName = name.toLowerCase();
        const headerName = Object.keys(headers).find(
            candidate => candidate.toLowerCase() === expectedName
        );
        return headerName ? headers[headerName] : null;
    }

    function getHeaderValues(headers, name) {
        const value = getHeader(headers, name);
        if (value === null || value === undefined) {
            return [];
        }
        return Array.isArray(value) ? value : [value];
    }

    function extractToken(headers) {
        const cookieHeaders = getHeaderValues(headers, "set-cookie");
        for (const cookieHeader of cookieHeaders) {
            const cookies = String(cookieHeader).split(/,(?=\s*[^;,=\s]+=[^;,]*)/);
            for (const cookie of cookies) {
                const match = cookie.match(/(?:^|;\s*)token=([^;]*)/i);
                if (match) {
                    return match[1] || null;
                }
            }
        }
        return undefined;
    }

    function updateFromHeaders(headers) {
        const token = extractToken(headers);
        if (token !== undefined) {
            webzfsToken = token;
        }
    }

    function addCookieHeader(headers) {
        const requestHeaders = Object.assign({}, headers || {});
        if (webzfsToken) {
            requestHeaders.Cookie = `${TOKEN_COOKIE}=${webzfsToken}`;
        }
        return requestHeaders;
    }

    function clear() {
        webzfsToken = null;
    }

    const api = {
        addCookieHeader,
        clear,
        extractToken,
        getHeader,
        getHeaderValues,
        hasToken: () => Boolean(webzfsToken),
        updateFromHeaders,
    };

    global.WebZFSSession = api;
    if (typeof module !== "undefined" && module.exports) {
        module.exports = api;
    }
})(typeof window !== "undefined" ? window : globalThis);