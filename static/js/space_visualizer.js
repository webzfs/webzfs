/*
 * WebZFS Dataset Space Visualizer
 *
 * Renders an icicle / flamegraph layout of a ZFS pool's dataset
 * hierarchy into an existing <svg> element. Used both on the pool
 * detail page and inside the fleet view's modal so the rendering is
 * defined exactly once.
 *
 * Tree shape expected (returned by the Python backend):
 *   {
 *     name: "pool",
 *     used: 12345,
 *     referenced: 12345,
 *     available: 12345,
 *     used_by_dataset: 12345,
 *     used_by_snapshots: 12345,
 *     used_by_children: 12345,
 *     compressratio: "1.10x",
 *     snapshot_count: 0,
 *     children: [ ... same shape ... ]
 *   }
 *
 * Usage:
 *   var viz = WebzfsSpaceVisualizer.create({
 *       container: HTMLElement,        // wrapping div with the SVG inside
 *       svg: SVGElement,               // the <svg> to draw into
 *       loadingEl: HTMLElement,        // shown while fetching/error
 *       tooltip: HTMLElement,          // floating tooltip
 *       breadcrumb: HTMLElement,       // updated with current path
 *       backButton: HTMLElement,       // disabled at root
 *       snapshotToggle: HTMLInputElement,
 *       maxDepth: 4,                   // optional, default 4
 *       fetchTree: function(){ return Promise<treeJson>; }
 *   });
 *   viz.load();
 */
(function() {
    "use strict";

    var SVG_NS = "http://www.w3.org/2000/svg";
    var DEFAULT_MAX_DEPTH = 4;
    var ROW_HEIGHT = 56;
    var ROW_GAP = 4;
    // Width reserved for the free-space block when "Collapse free space"
    // is enabled. Just enough room for the label so dataset segments can
    // claim the rest of the row.
    var COLLAPSED_FREE_WIDTH = 140;

    function hexToRgb(hex) {
        var h = (hex || "").replace("#", "").trim();
        if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
        if (h.length !== 6) return null;
        return {
            r: parseInt(h.substring(0, 2), 16),
            g: parseInt(h.substring(2, 4), 16),
            b: parseInt(h.substring(4, 6), 16)
        };
    }

    function rgbToHsl(r, g, b) {
        r /= 255; g /= 255; b /= 255;
        var max = Math.max(r, g, b), min = Math.min(r, g, b);
        var h, s, l = (max + min) / 2;
        if (max === min) { h = s = 0; }
        else {
            var d = max - min;
            s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
            switch (max) {
                case r: h = (g - b) / d + (g < b ? 6 : 0); break;
                case g: h = (b - r) / d + 2; break;
                default: h = (r - g) / d + 4;
            }
            h *= 60;
        }
        return { h: h, s: s * 100, l: l * 100 };
    }

    function readTheme() {
        var defaults = { hue: 217, sat: 65, base: 50 };
        try {
            var rootStyle = getComputedStyle(document.documentElement);
            var hex = (rootStyle.getPropertyValue("--primary-500") || "").trim();
            var rgb = hexToRgb(hex);
            if (!rgb) return defaults;
            var hsl = rgbToHsl(rgb.r, rgb.g, rgb.b);
            return {
                hue: Math.round(hsl.h),
                sat: Math.max(35, Math.min(75, Math.round(hsl.s))),
                base: Math.max(35, Math.min(60, Math.round(hsl.l)))
            };
        } catch (e) {
            return defaults;
        }
    }

    function hashString(s) {
        var h = 0;
        for (var i = 0; i < s.length; i++) {
            h = ((h << 5) - h) + s.charCodeAt(i);
            h |= 0;
        }
        return Math.abs(h);
    }

    function colorForName(theme, name, depth) {
        // Bounded hue window centered on the theme's primary hue so all
        // segments belong to the active palette.
        var spread = 160;
        var offset = (hashString(name) % spread) - (spread / 2);
        var hue = (theme.hue + offset + 360) % 360;
        var sat = theme.sat - 5 + (hashString(name + ":sat") % 12);
        if (sat < 30) sat = 30;
        if (sat > 70) sat = 70;
        var lightness = theme.base - 6 + (depth * 5);
        if (lightness < 32) lightness = 32;
        if (lightness > 62) lightness = 62;
        return "hsl(" + hue + ", " + sat + "%, " + lightness + "%)";
    }

    function formatBytes(bytes) {
        if (bytes === null || bytes === undefined || isNaN(bytes)) return "-";
        var b = Number(bytes);
        if (b < 1024) return b + " B";
        var units = ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
        var v = b / 1024;
        var i = 0;
        while (v >= 1024 && i < units.length - 1) {
            v = v / 1024;
            i++;
        }
        return (v >= 100 ? v.toFixed(0) : v.toFixed(2)) + " " + units[i];
    }

    function shortName(fullName) {
        var idx = fullName.lastIndexOf("/");
        return idx === -1 ? fullName : fullName.substring(idx + 1);
    }

    function findNode(tree, name) {
        if (!tree) return null;
        if (tree.name === name) return tree;
        if (!tree.children) return null;
        for (var i = 0; i < tree.children.length; i++) {
            var found = findNode(tree.children[i], name);
            if (found) return found;
        }
        return null;
    }

    function makeRect(x, y, w, h, fill, opts) {
        var rect = document.createElementNS(SVG_NS, "rect");
        rect.setAttribute("x", x);
        rect.setAttribute("y", y);
        rect.setAttribute("width", Math.max(0, w));
        rect.setAttribute("height", h);
        rect.setAttribute("fill", fill);
        if (opts && opts.stroke) rect.setAttribute("stroke", opts.stroke);
        if (opts && opts.strokeWidth) rect.setAttribute("stroke-width", opts.strokeWidth);
        if (opts && opts.opacity !== undefined) rect.setAttribute("opacity", opts.opacity);
        return rect;
    }

    function makeText(x, y, content, opts) {
        var t = document.createElementNS(SVG_NS, "text");
        t.setAttribute("x", x);
        t.setAttribute("y", y);
        t.setAttribute("fill", (opts && opts.fill) || "#fff");
        t.setAttribute("font-size", (opts && opts.fontSize) || 12);
        t.setAttribute("font-family", "ui-sans-serif, system-ui, sans-serif");
        t.setAttribute("dominant-baseline", "middle");
        t.setAttribute("pointer-events", "none");
        if (opts && opts.weight) t.setAttribute("font-weight", opts.weight);
        t.textContent = content;
        return t;
    }

    function computeMaxDepth(node, current) {
        var d = current || 0;
        if (!node.children || node.children.length === 0) return d;
        var deepest = d;
        for (var i = 0; i < node.children.length; i++) {
            var sub = computeMaxDepth(node.children[i], d + 1);
            if (sub > deepest) deepest = sub;
        }
        return deepest;
    }

    function create(opts) {
        var container = opts.container;
        var svg = opts.svg;
        var loadingEl = opts.loadingEl;
        var tooltip = opts.tooltip;
        var breadcrumb = opts.breadcrumb;
        var backButton = opts.backButton;
        var snapshotToggle = opts.snapshotToggle;
        var maxDepth = opts.maxDepth || DEFAULT_MAX_DEPTH;
        var fetchTree = opts.fetchTree;

        if (!container || !svg || !fetchTree) {
            throw new Error("space_visualizer.create: missing required options");
        }

        var collapseFreeToggle = opts.collapseFreeToggle;

        var theme = readTheme();
        var rootTree = null;
        var viewStack = [];
        var showSnapshotBands = snapshotToggle ? !!snapshotToggle.checked : true;
        var collapseFreeSpace = collapseFreeToggle ? !!collapseFreeToggle.checked : false;

        function setBackEnabled(enabled) {
            if (!backButton) return;
            if (enabled) backButton.removeAttribute("disabled");
            else backButton.setAttribute("disabled", "disabled");
        }

        function renderBreadcrumb() {
            if (!breadcrumb) return;
            breadcrumb.innerHTML = "";
            if (viewStack.length === 0) return;
            var parts = viewStack.map(function(name, idx) {
                var label = idx === 0 ? name : shortName(name);
                return '<button type="button" class="hover:text-text-primary underline-offset-2 hover:underline" data-stack-index="' + idx + '">' + label + '</button>';
            });
            breadcrumb.innerHTML = "Viewing: " + parts.join(' <span class="text-text-tertiary">/</span> ');
            var buttons = breadcrumb.querySelectorAll("button[data-stack-index]");
            buttons.forEach(function(btn) {
                btn.addEventListener("click", function() {
                    var idx = parseInt(btn.getAttribute("data-stack-index"), 10);
                    viewStack = viewStack.slice(0, idx + 1);
                    draw();
                });
            });
        }

        function showTooltip(node, event) {
            if (!tooltip) return;
            var lines = [];
            lines.push('<div class="font-semibold text-text-primary mb-1">' + node.name + "</div>");
            lines.push('<div class="grid grid-cols-2 gap-x-3 gap-y-0.5">');
            lines.push('<span class="text-text-tertiary">Used:</span><span>' + formatBytes(node.used) + "</span>");
            lines.push('<span class="text-text-tertiary">Referenced:</span><span>' + formatBytes(node.referenced) + "</span>");
            if (node.available > 0) {
                lines.push('<span class="text-text-tertiary">Available:</span><span>' + formatBytes(node.available) + "</span>");
            }
            lines.push('<span class="text-text-tertiary">By dataset:</span><span>' + formatBytes(node.used_by_dataset) + "</span>");
            lines.push('<span class="text-text-tertiary">By snapshots:</span><span>' + formatBytes(node.used_by_snapshots) + "</span>");
            lines.push('<span class="text-text-tertiary">By children:</span><span>' + formatBytes(node.used_by_children) + "</span>");
            lines.push('<span class="text-text-tertiary">Compress:</span><span>' + (node.compressratio || "-") + "</span>");
            lines.push('<span class="text-text-tertiary">Snapshots:</span><span>' + (node.snapshot_count || 0) + "</span>");
            lines.push("</div>");
            tooltip.innerHTML = lines.join("");
            tooltip.classList.remove("hidden");
            positionTooltip(event);
        }

        function positionTooltip(event) {
            if (!tooltip) return;
            var pad = 12;
            var x = event.clientX + pad;
            var y = event.clientY + pad;
            var rect = tooltip.getBoundingClientRect();
            if (x + rect.width > window.innerWidth - pad) {
                x = event.clientX - rect.width - pad;
            }
            if (y + rect.height > window.innerHeight - pad) {
                y = event.clientY - rect.height - pad;
            }
            tooltip.style.left = x + "px";
            tooltip.style.top = y + "px";
        }

        function hideTooltip() {
            if (tooltip) tooltip.classList.add("hidden");
        }

        function drawSegment(parent, node, x, y, width, depth, isFreeSpace) {
            if (width < 1) return;
            var fill = isFreeSpace ? "rgba(120,120,120,0.35)" : colorForName(theme, node.name, depth);
            var group = document.createElementNS(SVG_NS, "g");
            group.style.cursor = isFreeSpace ? "default" : "pointer";

            var rect = makeRect(x, y, width, ROW_HEIGHT, fill, {
                stroke: "rgba(0,0,0,0.45)",
                strokeWidth: 1
            });
            group.appendChild(rect);

            if (!isFreeSpace && showSnapshotBands && node.used_by_snapshots > 0) {
                var totalUsed = Math.max(1, node.used);
                var bandWidth = width * (node.used_by_snapshots / totalUsed);
                if (bandWidth >= 0.5) {
                    var bandRect = makeRect(
                        x + width - bandWidth, y, bandWidth, ROW_HEIGHT,
                        "rgba(255,255,255,0.18)", { opacity: 0.85 }
                    );
                    group.appendChild(bandRect);
                }
            }

            var label = isFreeSpace
                ? "free " + formatBytes(node.used)
                : shortName(node.name);
            if (width > 50) {
                var textColor = isFreeSpace ? "#cbd5e1" : "#ffffff";
                var t = makeText(x + 6, y + ROW_HEIGHT / 2, label, {
                    fontSize: 12,
                    fill: textColor,
                    weight: depth === 0 ? "600" : "500"
                });
                group.appendChild(t);
            }

            if (!isFreeSpace) {
                group.addEventListener("mouseenter", function(ev) { showTooltip(node, ev); });
                group.addEventListener("mousemove", function(ev) { positionTooltip(ev); });
                group.addEventListener("mouseleave", hideTooltip);
                group.addEventListener("click", function() {
                    if (depth === 0) return;
                    viewStack.push(node.name);
                    draw();
                });
            }

            parent.appendChild(group);
        }

        function layoutChildren(parentSvg, node, parentX, parentY, parentWidth, depth, depthCap) {
            if (!node.children || node.children.length === 0) return;
            var totalChildUsed = 0;
            for (var i = 0; i < node.children.length; i++) {
                totalChildUsed += Math.max(0, node.children[i].used);
            }
            if (totalChildUsed <= 0) return;

            var rowY = parentY - (ROW_HEIGHT + ROW_GAP);
            var consumedFraction = totalChildUsed / Math.max(1, node.used);
            if (consumedFraction > 1) consumedFraction = 1;
            var childAreaWidth = parentWidth * consumedFraction;
            var cursorX = parentX;
            for (var j = 0; j < node.children.length; j++) {
                var child = node.children[j];
                var childWidth = childAreaWidth * (Math.max(0, child.used) / totalChildUsed);
                drawSegment(parentSvg, child, cursorX, rowY, childWidth, depth, false);
                if (depth + 1 < depthCap) {
                    layoutChildren(parentSvg, child, cursorX, rowY, childWidth, depth + 1, depthCap);
                }
                cursorX += childWidth;
            }
        }

        function layoutLevel(parentSvg, node, x, y, width, depth, depthCap) {
            if (depth === 0 && node.available > 0) {
                var totalCapacity = node.used + node.available;
                if (totalCapacity > 0) {
                    var usedWidth;
                    var freeWidth;
                    if (collapseFreeSpace) {
                        // Collapse the free-space block to a fixed slice
                        // so dataset segments fill the rest of the row.
                        // Cap at half so a tiny pool with mostly-free
                        // space does not collapse to nothing.
                        freeWidth = Math.min(COLLAPSED_FREE_WIDTH, width * 0.5);
                        usedWidth = width - freeWidth;
                    } else {
                        usedWidth = width * (node.used / totalCapacity);
                        freeWidth = width - usedWidth;
                    }
                    drawSegment(parentSvg, node, x, y, usedWidth, depth, false);
                    drawSegment(
                        parentSvg,
                        { name: "free", used: node.available },
                        x + usedWidth, y, freeWidth, depth, true
                    );
                    if (depth + 1 < depthCap) {
                        layoutChildren(parentSvg, node, x, y, usedWidth, depth + 1, depthCap);
                    }
                    return;
                }
            }
            drawSegment(parentSvg, node, x, y, width, depth, false);
            if (depth + 1 < depthCap) {
                layoutChildren(parentSvg, node, x, y, width, depth + 1, depthCap);
            }
        }

        function draw() {
            if (!rootTree) return;
            var currentName = viewStack[viewStack.length - 1];
            var node = findNode(rootTree, currentName);
            if (!node) {
                viewStack = [rootTree.name];
                node = rootTree;
            }
            setBackEnabled(viewStack.length > 1);
            renderBreadcrumb();

            while (svg.firstChild) svg.removeChild(svg.firstChild);

            var width = container.clientWidth || 800;
            var depthCap = Math.min(maxDepth, computeMaxDepth(node, 1));
            if (depthCap < 1) depthCap = 1;
            var rows = depthCap;
            var totalHeight = rows * ROW_HEIGHT + (rows - 1) * ROW_GAP;
            if (totalHeight < 80) totalHeight = 80;

            svg.setAttribute("viewBox", "0 0 " + width + " " + totalHeight);
            svg.setAttribute("width", width);
            svg.setAttribute("height", totalHeight);
            svg.style.height = totalHeight + "px";

            var rootY = totalHeight - ROW_HEIGHT;
            layoutLevel(svg, node, 0, rootY, width, 0, depthCap);

            if (loadingEl) loadingEl.style.display = "none";
            svg.style.display = "block";
        }

        function load() {
            // Refresh theme each load so theme switches apply on next open.
            theme = readTheme();
            if (loadingEl) {
                loadingEl.textContent = "Loading dataset usage...";
                loadingEl.style.display = "";
            }
            svg.style.display = "none";
            return Promise.resolve(fetchTree())
                .then(function(tree) {
                    if (!tree) throw new Error("Empty response");
                    rootTree = tree;
                    viewStack = [rootTree.name];
                    draw();
                })
                .catch(function(err) {
                    if (loadingEl) {
                        loadingEl.textContent = "Failed to load dataset usage: " + (err && err.message ? err.message : err);
                    }
                });
        }

        if (backButton) {
            backButton.addEventListener("click", function() {
                if (viewStack.length > 1) {
                    viewStack.pop();
                    draw();
                }
            });
        }

        if (snapshotToggle) {
            snapshotToggle.addEventListener("change", function() {
                showSnapshotBands = !!snapshotToggle.checked;
                draw();
            });
        }

        if (collapseFreeToggle) {
            collapseFreeToggle.addEventListener("change", function() {
                collapseFreeSpace = !!collapseFreeToggle.checked;
                draw();
            });
        }

        var resizeTimer = null;
        function onResize() {
            if (resizeTimer) clearTimeout(resizeTimer);
            resizeTimer = setTimeout(function() {
                if (rootTree) draw();
            }, 150);
        }
        window.addEventListener("resize", onResize);

        return {
            load: load,
            redraw: draw,
            destroy: function() {
                window.removeEventListener("resize", onResize);
            }
        };
    }

    window.WebzfsSpaceVisualizer = { create: create };
})();
