---
title: "WebZFS Security Audit: Findings and Fixes"
date: 2026-11-11
tags: [security, audit, zfs, announcement]
---

# WebZFS Security Audit: Findings and Fixes

If it's not obvious to everyone, AI and specifically Claude, has gotten really good at finding security vulnerabilities in software.  Regardless of someone's view of AI, patching security issues is an important issue; and IMHO, refusing to use it to find security vulnerabilities just puts your users at risk. If the problems exist, they need to be found and fixed.

Since WebZFS runs with a lot of privileges, it needs to be put through it's paces. It manages pools, holds SSH keys that can log in to other servers, and it is (or effectively is) root on every platform it supports. On FreeBSD and NetBSD it runs as root, and on Linux the `webzfs` service account holds a sudo policy that is root-equivalent. So I had Fable 5.1 chew through the repo for about an hour and to find where issues exist.

This post covers what turned up through a review with Anthropic's Fable 5.1 (aka; the things I screwed up). I'll try to summarize what was wrong, how it got fixed, and any changes you will have to deal with after updating.

Every finding Fable came up with, was tested and reproduced by me on my system to make sure it wasn't hallucinating anything. Some of these were pretty wonky and rely on assumptions of access and how a user might behave... but in the end it doesn't matter if its an improbably series of steps, if they exist, they need to be addressed. 


A couple things turned out to be smaller issues than they looked at first, and some more serious upon review. All are covered below. 

The fixes will land in the 0.87 branch first.

**Read this first if you run WebZFS today.** Login restriction is off by default during the beta and release candidate phase. That means every account on the system that PAM accepts can log in, and every login has root-level control of the host. It's recommended that you set `RESTRICT_LOGIN=yes` in `.env` now; the details are in "Login restriction" below. The 1.0 release will ship with it on by default. I'm not enabling it now because I dont want to break testing systems.

---

## What you will notice after updating

Most of this work is invisible, but a few things change behavior:

- **Everyone is logged out once.** Session tokens now carry a type marker, and tokens issued before the update do not have it. Log in again and you are done.
- **Links to WebZFS from other sites land on the login page.** If you click a WebZFS link in an email, chat, wiki... ok, I'm going to stop right here and interject that you should never ever do this. I dont know why you would, but just in case this ever crosses your mind. Stop. Dont do this. 
- **Reverse proxy on another host? List it in `FORWARDED_ALLOW_IPS`.** Otherwise every user behind   the proxy shares the proxy's address for login rate limiting, and five failed logins from anyone   lock everyone out for a minute. A proxy on the same host is already trusted by default.
- **Adding an SSH connection requires the "Check host key" step.** You review the server's host key fingerprint before your password is sent.
- **Hosts are never trusted automatically.** A connection or fleet server without a pinned host key is refused until you confirm its key on its "Trust host key" page. Manually added fleet servers now need this one extra click.
- **Native remote replication requires a selected SSH connection.** The form already required one;  the server now enforces it.
- **"Remove key from remote" tells you when it fails**, and which line to remove by hand.
- **WebZFS refuses to start with a placeholder or short `SECRET_KEY`.** Installed systems have a generated key and are not affected. If you wrote your own `.env` by hand with the example placeholder or a key shorter than 32 characters, the service stops at startup with a message that tells you how to generate one.
- **New, and strongly recommended: `RESTRICT_LOGIN=yes` in `.env` limits who can log in.** It is off by default while WebZFS is in beta, so nothing changes unless you turn it on. With it off, every account PAM accepts is a WebZFS administrator, and every WebZFS administrator is root on the host. Turn it on. See "Login restriction" below.

(I say invisible, but if I screwed up a fix, you may step on some very visible bugs. If you do, please file a ticket.)

---

## The problems 

### Cross-site scripting (XSS)

WebZFS renders pages with Jinja2, which escapes output automatically. That protection was being
bypassed in three ways:

1. **The confirmation dialog rendered its message as raw HTML.** About 40 pages build that message, and several put values into it: a pool name taken straight from the URL, a pool's mountpoint property (which ZFS allows to contain any characters), and others. A crafted link to the pool export page could script in the victim's browser if they clicked a webzfs link on a random website, which again they never should do, but we should prevent this anyway.
2. **Values placed inside JavaScript.** Patterns like `onclick="doThing('{{ name }}')"` look escaped, but they are not safe. The browser decodes HTML entities in an attribute before the JavaScript runs, so an escaped quote turns back into a real quote. The dataset create form had this with a query parameter, and fleet server names, scheduled job names, ZED script names, disk names and vdev device names all used the pattern.
3. **HTML built outside the template engine.** Several HTMX partials built HTML in Python with f-strings, inserting exception messages (which often repeat a query parameter back), `zpool` output, process names (which any local user controls) and pool history. Pool history records every `zfs` command with its arguments, so a property value set by a delegated user could end up as script on the observability dashboard.

The session cookie was not marked `HttpOnly`, so any of these let a script read the session token and use it from somewhere else, for the full session lifetime (up to 7 days).
So how was this all addressed?

- The confirmation dialog no longer renders raw HTML. Messages that need bold text are built with Jinja block sets, which escape every value inside them.
- Values used by JavaScript now travel in `data-*` attributes (where normal escaping is correct), or are encoded with `|tojson`. Inline handlers built in JavaScript use JSON encoding before attribute escaping. I tested the old and new patterns in a real browser: the old one executed injected code from a hostile device name, the new one passed the exact name through and ran nothing.
- HTML built in JavaScript escapes server data or uses `textContent`. HTML built in Python wraps every inserted value in `html.escape`.
- Path and query parameters are validated with the existing ZFS name validators before they reach a template.

---

### Third-party JavaScript

WebZFS loaded htmx, Alpine.js and Chart.js from public CDNs. htmx had an integrity hash; the other two did not. A compromised CDN or package would have run code inside every WebZFS page, with your session.

All three are now served from WebZFS itself. They were already pinned in `package.json`, and `package-lock.json` records a sha512 hash for each, which `npm install` checks. A new build step copies them into `static/vendor/`, the same way xterm.js was already handled, and those copies are now committed to the repository, so an install does not download them at all. Every installer and updater already runs that build, so there is nothing new to do. As a side effect, WebZFS no longer needs internet access from the browser to download these.

(While checking this, it turned out the Google Fonts import in the stylesheet has never actually loaded. It ends up partway down the compiled CSS, where browsers ignore it. The interface has always used the fallback fonts, so nothing looks different.)

---

### Session cookie and security headers

The session cookie is now:

- **HttpOnly:** page JavaScript cannot read it, so an XSS bug can no longer steal the session. Nothing in WebZFS reads this cookie from JavaScript.
- **SameSite=Strict:** the browser does not send it on requests that start on another site. A
  crafted link to a WebZFS page therefore arrives logged out, which defuses any reflected XSS that might remain. This is why links from other sites now land on the login page.
- **Secure when served over HTTPS**, directly or through a trusted reverse proxy.

The Cockpit integration reads the token on the server side through Cockpit's HTTP bridge, so these flags do not affect it. I tested its token parsing against the new headers to be sure.

Every response now carries a Content-Security-Policy, plus `X-Frame-Options`, `X-Content-Type-Options` and `Referrer-Policy`. The policy allows scripts, styles, fonts, images and network connections only from WebZFS itself, and forbids other sites from framing WebZFS (clickjacking).

To be clear about its limits: the templates use inline scripts and Alpine.js needs `eval`, so the policy still allows those. It is a safety net. The escaping work I mentioned above is what needed to be done. In a browser test, the policy blocked an injected external script, an outbound `fetch`, and an image beacon, while ten real pages loaded with zero violations.

---

### Session tokens

Every token WebZFS signs uses the same key. The login check accepted any of them that had a username and an expiry, including the short-lived confirmation token used by the ZFS delegation page, which is embedded in the page HTML. Anyone who got hold of that token could use it as a 5-minute login session, including for the shell terminal.

Session tokens now carry a `typ: session` claim, and only those are accepted as sessions. The delegation confirmation already checked its own purpose, so neither token can stand in for the other. 

This is a change that logs everyone out once after the update.

---

### Login rate limiting

The limiter had several problems that added up to "not much of a limit":

- It took the client address from the `X-Forwarded-For` header, which any client can set. Changing the header on every attempt reset the limit. But hey, previously there was at least an effort made. lol
- It counted in process memory, so each Gunicorn worker had its own count (two workers, double the attempts).
- The check and the recording of an attempt were separate steps around a slow PAM call, so parallel requests could all pass the check.
- It limited per address only, so guesses against one account from many addresses were unlimited.

Now:

- The client address comes from the connection itself. Uvicorn already replaces it with the forwarded address when, and only when, the request comes from a proxy listed in `FORWARDED_ALLOW_IPS`. That is why a remote proxy must be listed.
- Counts live in a small state file under a file lock, shared by all workers. Checking and recording happen in one step before PAM runs. In a test, eight separate processes racing on one address let through exactly five attempts.
- A second limit applies per username (10 attempts in 5 minutes), whatever the address.
- Very long usernames and passwords are refused before they reach PAM. So are usernames and passwords containing a NUL or any other control character. Python-pam hands both values to the C library as C strings, and a NUL byte ends a C string, so PAM would have accepted `root` followed by a NUL and anything else as `root` while WebZFS woul kept the entire string as the logged-in 'user'. Nothing beyond control characters is refused, because directory accounts can legitimately contain uppercase letters, dots, `@`, backslashes and spaces.

One trade-off to know about: someone who knows a username can keep that account locked out of the web UI by failing logins on purpose. PAM's own `pam_faillock` has the same property. The limits are constants in `auth/rate_limiter.py` if you want to relax them. But if you actually have this problem; you should address the issue of the person misbehaving.

---

### Login restriction

Let's be very clear about this before deciding who may log in: **a WebZFS login is root on the host.** Not "can manage ZFS", root. This is why the README.md is very clear that this should ONLY be accessed over a secure SSH tunnel and not available on your wider network. If you set 0.0.0.0; the blame is on you.

WebZFS is an administration tool, and its features need root privileges: the file browser and text editor read and write any file on the system, ZED event scripts, the `smartd` and Sanoid configuration pages and Syncoid's additional flags all define things that run as root, and the scheduler writes systemd units. On FreeBSD and NetBSD the process is root; on Linux the service account's sudo policy is root-equivalent. I have taken the effort to try to limit what the webzfs user on linux can do with sudo, but even if they get no further permissions, they can still create havoc for you.  Thi

It follows that the shell terminal allowlist (`SHELL_ALLOWED_USERS`), the recording auditor list and the ZFS delegation allowlist decide which pages a user sees. They do not limit privilege. It's more of a 'filter' on the site, not an actual user level restriction. A user who is not on the shell allowlist can still read `.env` through the file browser, take the signing key, and mint a session for a name that is on it. Treat those lists as a convenience for tidy interfaces, and treat the login restriction below as the only real access boundary.

So who can log in? By default, any account PAM accepts can log in to WebZFS. On Linux, the service user can read `/etc/shadow`, so that is every local account with a password, and every directory account if PAM uses LDAP or SSSD.
This was the design from day 1, and it's fine for personal systems or home labs, since it required SSH access to get to the webUI. However, as I've learned that there are some businesses using this, I think it's high time for a bit more control over who can access it even if they have ssh to the server. 

**This default is off during the beta and release candidate phase** 

I'm keeping it off for now because the project is still shaking out how people deploy it, and a restriction that locks the only administrator out of a new install would be worse than a loud warning. 

The 1.0 release will ship with the restriction enabled by default and the installers will ask for the first administrator. Until then, please do not run WebZFS on a multi-user system, or on anything reachable beyond localhost, without turning it on. It's your system so you can do what you want... but that's on you.

Setting `RESTRICT_LOGIN=yes` in `.env` limits access to the accounts in `LOGIN_ALLOWED_USERS` (default `root`) and members of the groups in `LOGIN_ALLOWED_GROUPS` (default `wheel,webzfs-admin`). The installers now create an empty `webzfs-admin` group; add your administrators to it with your system's normal tools. `wheel` is included because it is the traditional admin group on FreeBSD and NetBSD, but it does not exist on every Linux distribution, which is why there is a WebZFS-specific group as well.

A few details:

- The check runs at login and on every request, so removing someone from the group ends their open sessions within about a minute. 
- A refused account sees the same "Invalid credentials" page as a wrong password, so the page never confirms a correct password. The audit log records the real reason.
- Group membership comes from the operating system, so LDAP and SSSD accounts work.
- The startup log says whether the restriction is on, and warns if a listed group does not exist or if nobody is allowed at all.

---

### SSH option injection in replication

This was the most serious finding, and I'm kinda embaressed that I missed it. But thankfully it's fixed now. 

The send/receive form had a hidden field holding the remote destination (`user@host`), and the server used it as the destination on the `ssh` command line. A value starting with `-` is read by `ssh` as an option, and one `ssh` option runs a local command. The host-key check only looked at the part after the last `@`.
What this means is that a logged in user could craft a value and pass it forward. The result: a logged in user could run an arbitrary command on the WebZFS server as the service account. I confirmed this with a harmless test command before fixing it. 
However, technically this user -already- had access to run commands on the server, so it's not like it was a catastrophic issue, if anything this would require more effort than just running commands directly with their access; but it's still something that the service shouldn't allow.
And although this bug did not grant anything a logged-in user lacked... it's still serious, because it is an unintended execution path with no audit trail. 

---

### SSH credential handling

When you add an SSH connection, WebZFS uses your password once to install a generated key on the remote server, via `sshpass -p <password> ssh-copy-id`. `sshpass` blanks the password in its own process listing right after it starts, so the window is tiny, but technically still exists for someone to snoop it. Hoever, if `ssh-copy-id` timed out, Python's timeout error message contains the full command, password included, and WebZFS wrote that message to its log.

WebZFS now uses `sshpass -e`, which reads the password from an environment variable. The password is no longer part of the command at all, so it cannot show up in the process list or in any error message.

Also, regarding installing and removing keys on remote servers...

- The key was installed with a shell command that contained the connection name you typed, so a quote in the name broke the command or worse. Keys are now sent to the remote side on standard input, and new keys are labelled `webzfs-<connection id>` instead of carrying your text.
- Appending to an `authorized_keys` file that did not end with a newline glued the new key onto the last existing line, breaking both. That is now handled.
- Removing a key used GNU `sed -i`, which fails on FreeBSD and NetBSD. Stupid mistake on my part. The exit status was ignored, so WebZFS reported the key as removed while it stayed authorized on the remote host. Removal now uses portable shell commands, succeeds only when the key is confirmed gone, and if it fails the page tells you exactly which line to delete by hand.
- Both commands run as `/bin/sh -c '...'`, so they behave the same whether the remote login shell is sh, bash, ksh or csh.

Regarding the new host key trust feature...
WebZFS pins each remote server's host key, so a server presenting a different key is refused. But when no key was pinned for a host, routine operations (scheduled Syncoid jobs, replication, fleet) silently pinned whatever key answered. The most likely way to get there: restore a backup onto a new machine. The backup brings the connections back but not the pinned keys, so the next scheduled job trusts whoever answers.

Now nothing trusts an unseen key. Each connection stores the fingerprints you confirmed when you created it. If its pin goes missing, it is restored automatically only if the server presents those same keys. Otherwise the operation is refused until you confirm the key on the new "Trust host key" page for SSH connections (fleet servers already had one). The SSH Connections page shows which connections have a pinned key. Creating a connection requires confirming the fingerprint, and WebZFS refuses to send your password to a host whose key has not been confirmed.

I actually found two smaller bugs while testing it so those are fixed too.
1. **"Check host key" crashed.** The fingerprint preview template looped over `preview.keys`. In Jinja, that finds the dictionary's built-in `.keys()` method instead of the `"keys"` entry, so the page returned a server error whenever the scan actually succeeded. The fleet "Trust host key" page had the same bug. In practice, the confirm-the-fingerprint step never worked, so every connection had been created by trusting the key on first use.
2. **The confirmation pinned a second scan.** After you confirmed a fingerprint, the code scanned the host again and pinned whatever that second scan returned, not the key you approved. If the second scan failed, it pinned nothing and reported success anyway. It now pins exactly the key material it compared.


I also found a performance issue while going through all of this, and that's now resolved.  freezing the server. The SSH and fleet pages ran their network work (key scans, connection tests, remote commands) inside async request handlers. While one of those waited on the network, every other request on that worker waited too. They are now ordinary functions, which now run in a thread pool.

---

Overall this review was a slow process that took place over 2 weeks. 
After the fixes were found and I sketched out a plan, I stepped back and let some time go by before implemnting them to make sure I was approaching it fresh. Then after the fixes were put in place, I took a few days, so I could come back to review with fresher eyes. I wanted to try to make sure that I didn't fix a dumb mistake with an even dumber mistake.




