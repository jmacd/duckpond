// SPDX-FileCopyrightText: 2026 Caspar Water Company
// SPDX-License-Identifier: Apache-2.0
//
// relative-time.js -- rewrite server-rendered <time data-utc-us="..."> markers
// to the visitor's local timezone and append a "X minutes ago" relative label
// that ticks every 30 s without a page reload.
//
// The server emits each timestamp as:
//   <time datetime="2026-05-04T22:00:00Z" data-utc-us="1746399600000000">
//     2026-05-04 22:00:00 UTC
//   </time>
//
// On DOMContentLoaded we walk every such element, replace the text with the
// browser's local-time formatting via Intl.DateTimeFormat (no formatting
// library required), and append a sibling <span class="rel-time"> with the
// relative label.  setInterval keeps the relative label fresh as the page
// sits open.
//
// Elements without a parseable data-utc-us value are skipped (the em-dash
// path -- the server emits no <time> element when the value is None).

const REL_INTERVAL_MS = 30 * 1000;

const localFmt = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
});

function relativeLabel(deltaMs) {
    const past = deltaMs >= 0;
    const abs = Math.abs(deltaMs);
    const sec = Math.round(abs / 1000);
    const min = Math.round(sec / 60);
    const hr = Math.round(min / 60);
    const day = Math.round(hr / 24);
    let label;
    if (sec < 5) {
        label = "just now";
        return label;
    } else if (sec < 90) {
        label = `${sec} sec`;
    } else if (min < 90) {
        label = `${min} min`;
    } else if (hr < 48) {
        label = `${hr} h`;
    } else {
        label = `${day} d`;
    }
    return past ? `${label} ago` : `in ${label}`;
}

function hydrate(el) {
    const usStr = el.getAttribute("data-utc-us");
    const us = parseInt(usStr, 10);
    if (!Number.isFinite(us) || us <= 0) {
        return null;
    }
    const ms = us / 1000;
    const date = new Date(ms);
    if (Number.isNaN(date.getTime())) {
        return null;
    }
    el.textContent = localFmt.format(date);
    el.setAttribute("title", el.getAttribute("datetime") || "");
    let rel = el.nextElementSibling;
    if (!rel || !rel.classList || !rel.classList.contains("rel-time")) {
        rel = document.createElement("span");
        rel.className = "rel-time";
        el.insertAdjacentElement("afterend", rel);
    }
    return { el, rel, ms };
}

function refresh(entries) {
    const now = Date.now();
    for (const entry of entries) {
        entry.rel.textContent = `(${relativeLabel(now - entry.ms)})`;
    }
}

function init() {
    const entries = [];
    for (const el of document.querySelectorAll("time[data-utc-us]")) {
        const entry = hydrate(el);
        if (entry) entries.push(entry);
    }
    if (entries.length === 0) return;
    refresh(entries);
    setInterval(() => refresh(entries), REL_INTERVAL_MS);
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
} else {
    init();
}
