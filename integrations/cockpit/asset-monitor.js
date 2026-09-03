(function (global) {
    "use strict";

    const failures = [];

    function showFailure(asset) {
        const status = document.getElementById("webzfs-adapter-status");
        if (!status) {
            return;
        }
        status.textContent = `Cockpit could not load the WebZFS package asset: ${asset}`;
        status.classList.add("webzfs-adapter-error");
        status.hidden = false;
    }

    global.addEventListener("error", event => {
        const element = event.target;
        if (!element || !element.getAttribute) {
            return;
        }
        const asset = element.getAttribute("href") || element.getAttribute("src");
        if (!asset) {
            return;
        }
        failures.push(asset);
        showFailure(asset);
    }, true);

    global.WebZFSAssetFailures = failures;
    global.WebZFSShowAssetFailure = showFailure;
    global.addEventListener("DOMContentLoaded", () => {
        failures.forEach(showFailure);
    }, { once: true });
})(window);