---
title: "WebZFS 0.87: Hopefully no new features (I lied)"
date: 2026-11-27
tags: [release, beta, zfs, announcement]
---

# WebZFS 0.87: Hopefully no new features (I lied)

In the 0.85 post, under "What comes next", the very first bullet point was:

`Hopefully no new features`  
![](./images/maury.gif)

So anyway, here's a release with over 16,000 new lines in it.

In my defense, I didn't go looking for shiny new things to build. What I did was sit down and compare what `zpool` and `zfs` can actually do against what WebZFS lets you do, and write down every missing thing... and as it turns out... that list got long. Uncomfortably long. A lot of it was stuff where I honestly thought "wait, I don't have that already?"
After working through the list and deciding what I didn't want to be bothered with, I made a branch for each on my local Gitea, worked on them, and then spent a weekend merging them all into 0.87. If you want to know how that went, the commit log has you covered:

```text
sometimes i suck at merging
maybe not just sometimes, maybe most times
or almost all the time
oops I did it again, I forgot to add files
this is getting ridiculous, there's always one more file
```

However thankfully... it's all in main now. Here's the highlights.

---

### WebZFS finally knows what version of ZFS you're running

At the end of the 0.78 post I said I needed to go verify which features are 2.4.x only and disable them in the UI if you're on something older, and then told someone to go make that issue ticket.

Fun fact, no one made that ticket for me.  So I kinda forgot about it. Seriously, if you find something wrong, make a ticket so I dont forget.

ANywho, it's done now. WebZFS now checks your OpenZFS version and platform, and hides or disables the things your system can't do. dRAID needs 2.1, error scrubs need 2.2, scrub time ranges need 2.4, and NetBSD's ZFS is its own special little world. You shouldn't get offered a button that ZFS is just going to laugh at anymore. Key word being "shouldn't", it might still happen from time to time where I missed things.

A lot of the things below relied on this.

---

### Pools

- **Pool creation got the options I missed the first time.** Root dataset properties, pool properties, mountpoint and altroot, dRAID layouts, the choice of mirrored or striped log devices (we always mirrored them before, whether you wanted that or not), and an encrypted root dataset. There's also a real dry run using `zpool create -n`, which unlike a certain Syncoid checkbox from a few releases ago, is a flag that actually exists.
- **`zpool clear`.** For the whole pool or for a single device. Yes, really, there was no way to do this before. I dont know how I missed this... idk... I'm going blame it on indigestion one random day in the past. 
- **Read-only import.** For when you want to look at a pool without touching it.
- **More scrub control.** Pause and resume, error scrubs, time ranged scrubs, and scrub all pools.

And in the grand WebZFS tradition of "properties you could edit, except you couldn't"... the pool properties page let you change `readonly` which is an import time only property. So ZFS said no every single time. It's now displayed as what it is, and you set it when you import.

---

### Datasets and Snapshots

- **The property editor is no longer a hand typed list in a template.** There's now a proper catalog behind it that knows which properties apply to filesystems vs volumes, what values are valid, and which changes only affect newly written data. Values get validated before `zfs set` ever runs. You can also add, edit, and remove user properties.
- **Inherit.** You can now set a property back to inherited or revert it to its received value. Previously once you set something locally in WebZFS, it was local forever unless you went to the CLI.
- **Zvols.** Sparse volumes, `volblocksize` at creation, `volmode`, the device path, and resizing are all now available. Shrinking a zvol makes you confirm it, because that's a great way to ruin your evening or weekend.
- **Encryption keys.** Datasets used to always get created with a passphrase and a prompt. Now you get hex and raw keys, key files, https key locations if your ZFS was built with support for it, key file generation, proper `change-key` options, and a "Load All Keys" button.
For anyone using encryption heavily, please create some dummy datasets and give these options a good workout. I want more testing on these to ensure that there's no issues. So test changing the key with dummy data in test datasets not your already encrypted datasets.
- **Bulk snapshot destroy.** Check the boxes, see the dry run with how much space you'll get back, type `DESTROY`, done. My commit message for this one was "not sure how I missed this before" and I stand by it.

---

### Replication

This is the part I'm happiest with.

- **Resumable replication.** Previously, if a native send/receive got interrupted, everything transferred so far was thrown away and you had to start over. On a multi-terabyte send over a flaky link, that's the kind of thing that makes people take up farming. Now the receive side keeps its partial state, and you can resume it or explicitly throw it away.
- **Intermediate snapshots.** Incremental sends only ever used `-i`, which means the snapshots between your base and your newest one never made it to the target. Most people expect the target to have the same snapshot history as the source. There's now an option to use `-I` and send all of them.
- **Bookmarks as the incremental source.** Matching is now done by GUID and not by snapshot name, so renamed snapshots don't break things, and you can still do an incremental after the source snapshot was pruned as long as a bookmark of it exists.
- **Receive options.** Receive unmounted (on by default for new jobs), property overrides and excludes, and the `-d`/`-e` target naming modes. These work for both native replication and Syncoid jobs.
- **Raw send for Syncoid jobs.** A checkbox, so you can replicate encrypted datasets without loading keys and without hand typing `--sendoptions` into the additional flags box. I even included a warning so people dont screw this up. 

---

### Observability

New panels for slow I/O counts per vdev, TRIM and initialize state, ZIL statistics, per pool txg history, and latency histograms. These are Linux and FreeBSD only.

Fun story on the latency histograms. The first version of that page never showed a single number. It sampled a one second window, and on an idle or lightly loaded pool, the count of I/Os in any given second is a whole lot of zero. So what I actually had built was a very nice table of dots. The latency and ZIL pages now default to the cumulative counters, and you can take a 5, 10, or 30 second sample or turn on auto refresh if you want to see what's happening right now.

The observability page also loads asynchronously now. Turns out if you have a workstation with spun down hard drives, waiting for them all to wake up before rendering anything makes for a bad time. Is sync vs async is the P vs NP of our time? (that joke seemed clever-er in my head than when I actually type it out... but I'm leaving it)

---

### Security

0.87 is also where all the fixes from the security audit landed. That was a big enough chunk of work that it got its own post, and I'm not going to repeat it all here:

**[WebZFS Security Audit: Findings and Fixes](./2026-11-11-security-audit-findings-and-fixes.md)**

Go read it, especially the "What you will notice after updating" section. The short version is that everyone gets logged out once, SSH host keys have to be confirmed by a human before they're trusted, and you should really set `RESTRICT_LOGIN=yes` in your `.env`.

---

### Node.js and npm were removed, and there was much rejoicing

![](./images/much_rejoicing.gif)
Remember the Ubuntu Snap saga from 0.78, where the installer got handed Node 10? The sequel was Linux Mint shipping Node 18 when one of the build dependencies had moved on to requiring Node 22.

Here's the thing. Node was only ever used at install time to build one CSS file. That's it. So the built stylesheet is now committed to the repo, the vendored JavaScript libraries are already there from the security work, and the installers and updaters on Linux, FreeBSD, and NetBSD no longer need Node.js or npm at all. Nothing gets downloaded from npm on your server.

If you're hacking on the templates or styles, you still need Node to rebuild the CSS. But for every other user, this is a me problem and not a you problem.

---

### Other fixes

- **smartd.conf on the BSDs.** WebZFS assumed `/etc/smartd.conf` everywhere. FreeBSD uses `/usr/local/etc/smartd.conf` and NetBSD uses `/usr/pkg/etc/smartd.conf`. So on BSD the page told you the config didn't exist, and then Save helpfully wrote a file that smartd would never read. Thanks to a GhostBSD user who caught that one. (See, this is why you file bug tickets people!)
- The Linux installer now stops and asks if you've actually installed the prerequisite packages from the README before it starts changing things on your system.
- Wheels for NetBSD 10.2 and 11.1 have been added so you can install on those versions now.
- htmx and Alpine.js bumped, along with the usual pile of dependabot updates.

---

### Thanks again, again

Same as always, a good number of these came from people running WebZFS on real systems and filing tickets when it did something dumb. Keep doing that.

A lot of this release is new code touching a lot of pages, and my test machines only cover so many combinations of OS and ZFS version. If something is offered that your system can't do, or something is hidden that your system can do, that's a bug and I want to hear about it.

Next up is 0.90. And this time I really do mean it about the features.

Mostly...










































hopefully...
