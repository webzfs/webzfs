/*
 * Alpine controller for observability pages that offer a cumulative view,
 * an on-demand sample window, and an optional auto refresh.
 *
 * Used by the Latency Histograms and ZIL Statistics pages. The root element
 * carries its configuration as data attributes:
 *
 *   x-data="observabilitySampler()"
 *   data-url="/zfs/observability/<partial>"   partial route (required)
 *   data-target="#panel-id"                   element the partial replaces (required)
 *   data-pool="<pool>"                        optional, sent as ?pool=
 *
 * The partial is requested with ?sample=0 for the cumulative view and
 * ?sample=N for an N-second sample; the server blocks for the window.
 * A partial that contains [data-sample-error] turns auto refresh off.
 *
 * Leaving the page: the layout uses hx-boost, so navigation swaps the body
 * without a page load and timers survive. Auto refresh stops and any running
 * sample is aborted as soon as a boosted navigation starts or the user uses
 * back/forward. Requests are never issued once the panel has left the
 * document, because htmx.ajax falls back to document.body when its target
 * cannot be found and the panel would replace the whole page.
 */
function observabilitySampler() {
    // Kept outside the Alpine data object so they are not made reactive.
    let panel = null;
    let currentRequest = null;
    let onRequest = null;
    let onPopState = null;

    return {
        url: '',
        target: '',
        extraParams: {},
        seconds: 10,
        autoRefresh: false,
        busy: false,
        status: '',
        nextTimer: null,

        init() {
            this.url = this.$el.dataset.url;
            this.target = this.$el.dataset.target;
            if (this.$el.dataset.pool !== undefined) {
                this.extraParams.pool = this.$el.dataset.pool;
            }
            panel = document.querySelector(this.target);

            onRequest = (event) => {
                if (event.detail.elt === panel) {
                    // Our own sample request; remember it so it can be aborted.
                    currentRequest = event.detail.xhr;
                } else if (event.detail.boosted) {
                    // User clicked a link or submitted a form to leave the page.
                    this.leavePage();
                }
            };
            onPopState = () => this.leavePage();
            document.addEventListener('htmx:beforeRequest', onRequest);
            window.addEventListener('popstate', onPopState);

            this.load(0);
        },

        // Called by Alpine when the component is removed from the page.
        destroy() {
            this.leavePage();
            document.removeEventListener('htmx:beforeRequest', onRequest);
            window.removeEventListener('popstate', onPopState);
        },

        leavePage() {
            this.stopAutoRefresh();
            if (currentRequest) {
                currentRequest.abort();
                currentRequest = null;
            }
        },

        panelPresent() {
            return panel !== null && panel.isConnected;
        },

        showCumulative() {
            this.stopAutoRefresh();
            this.load(0);
        },

        toggleAutoRefresh() {
            if (this.autoRefresh) {
                this.stopAutoRefresh();
                return;
            }
            this.autoRefresh = true;
            if (!this.busy) {
                this.load(this.seconds);
            }
        },

        stopAutoRefresh() {
            this.autoRefresh = false;
            if (this.nextTimer) {
                clearTimeout(this.nextTimer);
                this.nextTimer = null;
            }
        },

        load(sample) {
            if (this.busy) {
                return;
            }
            if (!this.panelPresent()) {
                this.stopAutoRefresh();
                return;
            }
            this.busy = true;
            this.status = sample ? `Sampling for ${sample} seconds...` : 'Loading...';
            const params = new URLSearchParams(this.extraParams);
            params.set('sample', sample);
            // Pass the element, not the selector, as both source and target so
            // htmx never substitutes document.body for a missing target.
            htmx.ajax('GET', this.url + '?' + params.toString(), { source: panel, target: panel, swap: 'innerHTML' })
                .then(() => this.finish(true), () => this.finish(false));
        },

        finish(ok) {
            currentRequest = null;
            this.busy = false;
            this.status = '';
            const failed = !ok || !this.panelPresent() || panel.querySelector('[data-sample-error]');
            if (failed) {
                this.stopAutoRefresh();
                return;
            }
            if (this.autoRefresh) {
                // Short pause so a fast failure cannot loop without delay.
                this.nextTimer = setTimeout(() => {
                    this.nextTimer = null;
                    if (this.autoRefresh) {
                        this.load(this.seconds);
                    }
                }, 1000);
            }
        },
    };
}
