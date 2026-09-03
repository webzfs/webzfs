"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

global.btoa = value => Buffer.from(value, "utf8").toString("base64");
global.cockpit = { transport: { csrf_token: "csrf-token" } };

const session = require("../session.js");
global.WebZFSSession = session;
const assets = require("../asset-rewrite.js");
const transport = require("../transport.js");

function makeHttpRequest(status, headers, body) {
    const successful = status >= 200 && status < 300;
    return {
        response(callback) {
            callback(status, headers || {});
            return this;
        },
        done(callback) {
            if (successful) {
                queueMicrotask(() => callback(body || ""));
            }
            return this;
        },
        fail(callback) {
            if (!successful) {
                queueMicrotask(() => callback({ status }, body || ""));
            }
            return this;
        },
        stream(callback) {
            this.streamCallback = callback;
            return this;
        },
        close() {},
    };
}

test("normalizes root-relative WebZFS paths", () => {
    assert.equal(transport.normalizePath("/utils/?page=2#item"), "/utils/?page=2#item");
    assert.equal(transport.normalizePath("system-info-data"), "/system-info-data");
    assert.equal(
        transport.normalizePath("http://localhost:26619/utils/ssh/"),
        "/utils/ssh/"
    );
});

test("rejects external URLs", () => {
    assert.throws(() => transport.normalizePath("https://example.com/"), /Not a WebZFS URL/);
});

test("extracts and forwards the WebZFS token cookie", () => {
    session.updateFromHeaders({ "Set-Cookie": "token=abc.def; Path=/; HttpOnly" });
    assert.deepEqual(session.addCookieHeader({ "HX-Request": "true" }), {
        "HX-Request": "true",
        Cookie: "token=abc.def",
    });
});

test("clears the token when WebZFS deletes the cookie", () => {
    session.updateFromHeaders({ "set-cookie": "token=; Max-Age=0; Path=/" });
    assert.equal(session.hasToken(), false);
});

test("rewrites WebZFS static URLs to package-local assets", () => {
    assert.equal(
        assets.rewriteStaticUrl("/static/css/themes/webzfs-theme-carbon-blue.css?v=1"),
        "static/css/themes/webzfs-theme-carbon-blue.css?v=1"
    );
    assert.equal(
        assets.rewriteStaticUrl("http://localhost:26619/static/css/themes/webzfs-theme-carbon-blue.css"),
        "static/css/themes/webzfs-theme-carbon-blue.css"
    );
    assert.equal(
        assets.rewriteStaticUrl("http://127.0.0.1:26619/static/img/webzfs-icon.svg?v=2"),
        "static/img/webzfs-icon.svg?v=2"
    );
    assert.equal(assets.rewriteStaticUrl("/zfs/pools/"), "/zfs/pools/");
    assert.equal(
        assets.rewriteResponseHtml(
            '<form hx-target="body" hx-swap="outerHTML" hx-push-url="true"><img src="/static/img/webzfs-icon.svg"></form>'
        ),
        '<form hx-target="#webzfs-root" hx-swap="innerHTML" hx-push-url="false"><img src="static/img/webzfs-icon.svg"></form>'
    );
    assert.equal(
        assets.rewriteResponseHtml(
            '<link href="http://localhost:26619/static/css/themes/webzfs-theme-carbon-blue.css">'
        ),
        '<link href="static/css/themes/webzfs-theme-carbon-blue.css">'
    );
});

test("canonicalizes utility router roots before Cockpit navigation", () => {
    const navigation = require("../navigation.js");

    assert.equal(navigation.canonicalizePagePath("/utils"), "/utils/");
    assert.equal(navigation.canonicalizePagePath("/utils/smart"), "/utils/smart/");
    assert.equal(navigation.canonicalizePagePath("/utils/logs"), "/utils/logs/");
    assert.equal(
        navigation.canonicalizePagePath("/utils/settings?message=Saved#theme"),
        "/utils/settings/?message=Saved#theme"
    );
    assert.equal(
        navigation.canonicalizePagePath("/utils/scrub-scheduling"),
        "/utils/scrub-scheduling"
    );
    assert.equal(
        navigation.canonicalizePagePath("/utils/smart/content-partial"),
        "/utils/smart/content-partial"
    );
});

test("recognizes a rewritten package-local theme stylesheet", () => {
    const link = {
        getAttribute: () => "static/css/themes/webzfs-theme-carbon-blue.css",
    };
    const documentNode = {
        body: { className: "corner-squared" },
        title: "WebZFS",
        querySelectorAll: () => [link],
    };
    assert.deepEqual(assets.getPagePresentation(documentNode), {
        bodyClass: "corner-squared",
        themeUrl: "static/css/themes/webzfs-theme-carbon-blue.css",
        title: "WebZFS",
    });
});

test("finds redirect locations without browser navigation", () => {
    assert.equal(
        transport.followRedirect({ status: 302, headers: { Location: "/" } }),
        "/"
    );
    assert.equal(transport.followRedirect({ status: 200, headers: {} }), null);
    assert.equal(
        transport.followRedirect({
            status: 303,
            headers: { Location: "http://localhost:26619/utils/ssh/" },
        }),
        "/utils/ssh/"
    );
});

test("builds an authenticated http-stream2 external channel URL", () => {
    session.updateFromHeaders({ "Set-Cookie": "token=secret; Path=/" });
    const channelUrl = transport.buildExternalChannelUrl(
        "GET",
        "/system-info-data",
        { "HX-Request": "true" }
    );
    const encodedChannel = channelUrl.split("?")[1];
    const channel = JSON.parse(Buffer.from(encodedChannel, "base64").toString("utf8"));

    assert.equal(channelUrl.startsWith("/cockpit/channel/csrf-token?"), true);
    assert.deepEqual(channel, {
        payload: "http-stream2",
        method: "GET",
        port: 26619,
        path: "/system-info-data",
        headers: {
            "HX-Request": "true",
            Cookie: "token=secret",
        },
    });
});

test("uses Cockpit's single request object API", async () => {
    let receivedRequest = null;
    const promise = makeHttpRequest(200, { "Content-Type": "text/html" }, "<html></html>");
    global.cockpit.http = () => ({
        request(request) {
            receivedRequest = request;
            return promise;
        },
    });
    transport.initialize();
    const response = await transport.request("GET", "/", { headers: { Accept: "text/html" } });

    assert.equal(response.status, 200);
    assert.deepEqual(receivedRequest, {
        method: "GET",
        headers: {
            Accept: "text/html",
            Cookie: "token=secret",
        },
        body: "",
        path: "/",
    });
});

test("forwards HTMX POST headers and body through cockpit.http", async () => {
    session.updateFromHeaders({ "Set-Cookie": "token=post-secret; Path=/" });
    let receivedRequest = null;
    global.cockpit.http = () => ({
        request(request) {
            receivedRequest = request;
            return makeHttpRequest(
                200,
                { "Content-Type": "text/html", "HX-Trigger": "saved" },
                '<div hx-target="body">Saved</div>'
            );
        },
    });
    transport.initialize();

    const xhr = new global.XMLHttpRequest();
    const loaded = new Promise(resolve => {
        xhr.onload = resolve;
    });
    xhr.open("POST", "/utils/text/save", true);
    xhr.setRequestHeader("HX-Request", "true");
    xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
    xhr.send("file_path=%2Ftmp%2Ftest&content=saved");
    await loaded;

    assert.deepEqual(receivedRequest, {
        method: "POST",
        headers: {
            "HX-Request": "true",
            "Content-Type": "application/x-www-form-urlencoded",
            Cookie: "token=post-secret",
        },
        body: "file_path=%2Ftmp%2Ftest&content=saved",
        path: "/utils/text/save",
    });
    assert.equal(xhr.status, 200);
    assert.equal(xhr.getResponseHeader("HX-Trigger"), "saved");
    assert.match(xhr.getAllResponseHeaders(), /HX-Trigger: saved/i);
    assert.equal(xhr.responseText, '<div hx-target="#webzfs-root">Saved</div>');
});

test("follows WebZFS redirects and marks the response as a full page", async () => {
    const requests = [];
    global.cockpit.http = () => ({
        request(request) {
            requests.push(request);
            if (requests.length === 1) {
                return makeHttpRequest(303, { Location: "/zfs/snapshots/?message=Created" }, "");
            }
            return makeHttpRequest(200, { "Content-Type": "text/html" }, "<main>Snapshots</main>");
        },
    });
    transport.initialize();

    const xhr = new global.XMLHttpRequest();
    const loaded = new Promise(resolve => {
        xhr.onload = resolve;
    });
    xhr.open("POST", "/zfs/snapshots/create", true);
    xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
    xhr.send("dataset_name=tank&snapshot_name=test");
    await loaded;

    assert.equal(requests.length, 2);
    assert.equal(requests[0].method, "POST");
    assert.equal(requests[1].method, "GET");
    assert.equal(requests[1].path, "/zfs/snapshots/?message=Created");
    assert.equal(xhr.getResponseHeader("X-WebZFS-Full-Page"), "true");
    assert.equal(xhr.responseURL, "http://127.0.0.1:26619/zfs/snapshots/?message=Created");
});

test("follows absolute localhost redirects returned by native form endpoints", async () => {
    const requests = [];
    global.cockpit.http = () => ({
        request(request) {
            requests.push(request);
            if (requests.length === 1) {
                return makeHttpRequest(
                    303,
                    { Location: "http://localhost:26619/utils/ssh/" },
                    ""
                );
            }
            return makeHttpRequest(200, { "Content-Type": "text/html" }, "<main>SSH</main>");
        },
    });
    transport.initialize();

    const response = await transport.requestFollowingRedirects(
        "POST",
        "/utils/ssh/add",
        {
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: "name=server",
        }
    );

    assert.equal(requests.length, 2);
    assert.equal(requests[0].method, "POST");
    assert.equal(requests[0].path, "/utils/ssh/add");
    assert.equal(requests[1].method, "GET");
    assert.equal(requests[1].path, "/utils/ssh/");
    assert.equal(response.redirected, true);
});

test("routes native forms through the shared submit adapter", () => {
    const navigation = require("../navigation.js");
    const originalFormData = global.FormData;
    const fields = [["name", "server"], ["host", "192.0.2.10"]];
    global.FormData = class {
        constructor(form) {
            this.fields = form.fields.slice();
        }

        append(name, value) {
            this.fields.push([name, value]);
        }

        forEach(callback) {
            this.fields.forEach(([name, value]) => callback(value, name));
        }

        entries() {
            return this.fields[Symbol.iterator]();
        }

        [Symbol.iterator]() {
            return this.fields[Symbol.iterator]();
        }
    };

    try {
        const requests = [];
        const downloads = [];
        const errors = [];
        const makeForm = (method, action, enctype) => ({
            method,
            enctype: enctype || "application/x-www-form-urlencoded",
            fields,
            getAttribute(name) {
                return name === "action" ? action : null;
            },
        });

        navigation.submitForm(
            makeForm("post", "/utils/ssh/add"),
            { name: "save", value: "1" },
            (requestPath, options) => requests.push({ path: requestPath, options }),
            (requestPath, options) => downloads.push({ path: requestPath, options }),
            message => errors.push(message)
        );
        navigation.submitForm(
            makeForm("get", "/zfs/snapshots"),
            null,
            (requestPath, options) => requests.push({ path: requestPath, options }),
            (requestPath, options) => downloads.push({ path: requestPath, options }),
            message => errors.push(message)
        );
        navigation.submitForm(
            makeForm("post", "/utils/support-bundle/generate"),
            null,
            (requestPath, options) => requests.push({ path: requestPath, options }),
            (requestPath, options) => downloads.push({ path: requestPath, options }),
            message => errors.push(message)
        );
        navigation.submitForm(
            makeForm("post", "/utils/settings/backup/inspect", "multipart/form-data"),
            null,
            (requestPath, options) => requests.push({ path: requestPath, options }),
            (requestPath, options) => downloads.push({ path: requestPath, options }),
            message => errors.push(message)
        );

        assert.deepEqual(requests[0], {
            path: "/utils/ssh/add",
            options: {
                method: "POST",
                body: "name=server&host=192.0.2.10&save=1",
                headers: {
                    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                },
            },
        });
        assert.equal(requests[1].path, "/zfs/snapshots?name=server&host=192.0.2.10");
        assert.equal(downloads[0].path, "/utils/support-bundle/generate");
        assert.match(errors[0], /Multipart form submission is not supported/);
    } finally {
        global.FormData = originalFormData;
    }
});

test("audits every template form for Cockpit submit compatibility", () => {
    const templatesRoot = path.resolve(__dirname, "../../../templates");
    const templateFiles = [];
    const visit = directory => {
        fs.readdirSync(directory, { withFileTypes: true }).forEach(entry => {
            const entryPath = path.join(directory, entry.name);
            if (entry.isDirectory()) {
                visit(entryPath);
            } else if (entry.isFile() && entry.name.endsWith(".jinja")) {
                templateFiles.push(entryPath);
            }
        });
    };
    visit(templatesRoot);

    const forms = [];
    const submitButtonsOutsideForms = [];
    const implicitSubmitButtons = [];
    templateFiles.forEach(templatePath => {
        const source = fs.readFileSync(templatePath, "utf8");
        for (const match of source.matchAll(/<form\b[^>]*>/gis)) {
            forms.push({ templatePath, tag: match[0] });
        }
        let formDepth = 0;
        for (const match of source.matchAll(/<\/?(?:form|button)\b[^>]*>/gis)) {
            const tag = match[0];
            if (/^<form\b/i.test(tag)) {
                formDepth += 1;
            } else if (/^<\/form\b/i.test(tag)) {
                formDepth = Math.max(0, formDepth - 1);
            } else if (/^<button\b/i.test(tag)) {
                if (/\btype=["']submit["']/i.test(tag) && formDepth === 0) {
                    submitButtonsOutsideForms.push({ templatePath, tag });
                }
                if (formDepth > 0 && !/\btype\s*=/i.test(tag)) {
                    implicitSubmitButtons.push({ templatePath, tag });
                }
            }
        }
    });

    assert.ok(forms.length > 0);
    forms.forEach(({ templatePath, tag }) => {
        assert.match(tag, /\baction\s*=/i, `Missing form action in ${templatePath}: ${tag}`);
        assert.match(tag, /\bmethod\s*=/i, `Missing form method in ${templatePath}: ${tag}`);
        assert.doesNotMatch(tag, /\baction\s*=\s*["']\s*["']/i, `Empty form action in ${templatePath}: ${tag}`);
    });
    assert.deepEqual(submitButtonsOutsideForms, []);
    assert.deepEqual(implicitSubmitButtons, []);

    const encodedForms = forms.filter(({ tag }) => /\benctype\s*=/i.test(tag));
    assert.equal(encodedForms.length, 1);
    assert.match(encodedForms[0].tag, /multipart\/form-data/i);
    assert.match(encodedForms[0].tag, /\/utils\/settings\/backup\/inspect/);
});

test("audits Fleet routes and disk-check modal compatibility", () => {
    const projectRoot = path.resolve(__dirname, "../../..");
    const fleetTemplate = fs.readFileSync(
        path.join(projectRoot, "templates/fleet/index.jinja"),
        "utf8"
    );
    const fleetViews = fs.readFileSync(path.join(projectRoot, "views/fleet.py"), "utf8");
    const createTemplate = fs.readFileSync(
        path.join(projectRoot, "templates/zfs/pools/create.jinja"),
        "utf8"
    );
    const vdevTemplate = fs.readFileSync(
        path.join(projectRoot, "templates/zfs/pools/vdevs.jinja"),
        "utf8"
    );

    assert.match(fleetTemplate, /action="\/fleet\/refresh"/);
    assert.match(fleetViews, /@router\.post\("\/refresh"\)/);
    assert.match(fleetViews, /@router\.get\("\/servers\/\{server_id\}\/card"/);

    [createTemplate, vdevTemplate].forEach(template => {
        assert.doesNotMatch(template, /id="confirm-check-disks"[^>]*x-data=/s);
        assert.doesNotMatch(template, /<template x-if="state ===/);
        assert.match(template, /id="disk-check-confirm-state"/);
        assert.match(template, /id="disk-check-checking-state" style="display: none;"/);
        assert.match(template, /id="disk-check-complete-state" style="display: none;"/);
        assert.match(template, /id="disk-check-error-state" style="display: none;"/);
        assert.match(template, /dialog\.style\.display = 'none'/);
    });
});

test("routes fetch requests through the bridge and parses JSON", async () => {
    let receivedRequest = null;
    global.cockpit.http = () => ({
        request(request) {
            receivedRequest = request;
            return makeHttpRequest(200, { "Content-Type": "application/json" }, '{"success":true}');
        },
    });
    transport.initialize();

    const response = await global.fetch("/utils/shell/autocomplete", {
        method: "POST",
        body: new URLSearchParams({ partial: "zp" }),
    });

    assert.equal(response.ok, true);
    assert.deepEqual(await response.json(), { success: true });
    assert.equal(receivedRequest.method, "POST");
    assert.equal(receivedRequest.body, "partial=zp");
    assert.equal(
        receivedRequest.headers["Content-Type"],
        "application/x-www-form-urlencoded;charset=UTF-8"
    );
});

test("parses server-sent event messages from a Cockpit HTTP stream", async () => {
    let streamRequest = null;
    global.cockpit.http = () => ({
        request() {
            streamRequest = makeHttpRequest(200, { "Content-Type": "text/event-stream" }, "");
            return streamRequest;
        },
    });
    transport.initialize();

    const eventSource = new global.EventSource("/zfs/replication/api/progress-stream");
    const message = new Promise(resolve => {
        eventSource.onmessage = event => resolve(event.data);
    });
    streamRequest.streamCallback('data: {"execution_id":1}\n\n');

    assert.equal(await message, '{"execution_id":1}');
    eventSource.close();
});

test("builds binary download requests with UTF-8 request bodies", async () => {
    const requests = [];
    const binaryBody = new Uint8Array([1, 2, 3]);
    global.cockpit.http = (port, options) => ({
        request(request) {
            requests.push({ options, request });
            return makeHttpRequest(
                200,
                {
                    "Content-Type": "application/zip",
                    "Content-Disposition": 'attachment; filename="bundle.zip"',
                },
                binaryBody
            );
        },
    });
    transport.initialize();

    const response = await transport.requestBinary("POST", "/utils/support-bundle/generate", {
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: "item_logs=1",
    });

    assert.equal(requests.length, 1);
    assert.deepEqual(requests[0].options, { binary: true });
    assert.equal(requests[0].request.method, "POST");
    assert.equal(
        Buffer.from(requests[0].request.body).toString("utf8"),
        "item_logs=1"
    );
    assert.deepEqual(response.body, binaryBody);
});

test("recognizes WebZFS download links and forms", () => {
    const navigation = require("../navigation.js");
    const makeElement = (href, attributes) => ({
        getAttribute(name) {
            return name === "href" || name === "action" ? href : null;
        },
        hasAttribute(name) {
            return Boolean(attributes && attributes.includes(name));
        },
    });

    assert.equal(navigation.isDownloadLink(makeElement("/utils/shell/download-history")), true);
    assert.equal(navigation.isDownloadLink(makeElement("/zfs/replication/history/1/error-log")), true);
    assert.equal(navigation.isDownloadLink(makeElement("/zfs/snapshots/sanoid/validate")), false);
    assert.equal(
        navigation.isDownloadForm(makeElement("/utils/support-bundle/generate")),
        true
    );
    assert.equal(
        navigation.isDownloadForm(makeElement("/utils/settings/apply-theme")),
        false
    );
});