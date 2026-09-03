---
title: "WebZFS 0.78: The bugs are coming from inside the house"
date: 2026-08-29
tags: [release, beta, zfs, announcement]
---

# WebZFS 0.78: The bugs are coming from inside the house
![inside the hoouse](https://raw.githubusercontent.com/webzfs/webzfs/refs/heads/main/blog/images/inside.jpg)

WebZFS 0.78 is ready, and it contains a pile of fixes. Some of these are legitimate edge cases. Some are nice improvements. And some are the sort of bugs where the correct technical explanation is:

![tarded](https://raw.githubusercontent.com/webzfs/webzfs/refs/heads/0.78/blog/images/tarded.gif)

So, in the interest of transparency, here are some of the more entertaining fixes in 0.78.

---

### Scheduled Jobs Now Survive Reboots

Previously, scheduled replication jobs were stored in a Python dictionary, as one does... in RAM. This worked perfectly, assuming WebZFS never restarted, the server never rebooted, and users did not expect scheduled jobs to persist through the passage of time. Cause rebooting is only for changing hardware right?

Now, there's a new scheduler which behaves much more like... well... a scheduler.

---

### Syncoid Was “Not Installed,” Except It Was

A permission error in the replication subsystem could sometimes become `Syncoid not installed`, but syncoid was installed, so something else had failed, and some broad exception handling decided that error was close enough to call it a day and clock out.

Error reporting is fixed so “Syncoid not installed” increasingly means the radical condition where Syncoid is actually not installed. Really breaking new ground here on concepts.

---

### We Added a Syncoid Option That Syncoid Doesn't Have

WebZFS had a helpful **Dry Run** checkbox.  I remember this PR when it came up, and I never followed up on it, I just remember reading it when it was proposed and thinking, 'Oh yea, that'd be great' And between then and now, I somehow just assumed it was accepted and merged because it just makes sense right?  ZFS has a -n flag for no-op checks.  Obviously Jim would have accpeted this into Syncoid...

Except Jim did not.  No blame on Jim, he knows his project better than anyone, and if he thinks it's not a good fit, that's cool. 

So, 0.78 removes our imaginary real but actually not real Syncoid feature.

---

### Meanwhile, We Forgot a Real ZFS Option

While supporting a nonexistent Syncoid option, WebZFS did not support the very real, `zfs send -L`.
How did this happen? Well... I forgot. 

![whatever](https://raw.githubusercontent.com/webzfs/webzfs/refs/heads/0.78/blog/images/whatever-shrug.gif)

Large-block replication now works correctly, including the proper Syncoid form: `--sendoptions=L`.

---

### Dataset Properties You Could Edit, Except You Couldn't

The Dataset Properties page let users modify:

```text
casesensitivity
normalization
utf8only
```

ZFS promptly rejected the operation because those properties are creation-only. Naturally, WebZFS did not expose them on the Dataset Creation page, which is the one place they actually work.

0.78 fixes both sides of that arrangement. Turns out that allowins creation-only properties to be set on creation is rather important for operations restricted to “creation-only" time.

![IDK, I just work here](https://raw.githubusercontent.com/webzfs/webzfs/refs/heads/0.78/blog/images/i-just-work-here-idk.gif)

---

### Dataset Creation Finally Shows the Command

Pool creation already had a command preview from an earlier fix, I meant to do the same for dataset creation, but yea... I forgot again. 

---

### Force Import Was More of a Suggestion

The UI offered both **Import** and **Force Import**.

Unfortunately, the `force=true` value was being sent differently from how the backend expected to receive it, so clicking Force Import could still arrive at the backend as `force=False`. 
0.78 makes the Force Import button considerably more forceful.

---

### The Audit Log Viewer Was Missing the Viewer

WebZFS already had an audit logging backend, routes, searching, filtering, and a link in the Utilities page. The three Jinja templates those routes tried to render did not exist.
Well that's not entirely true.  They did exist... on my machine... they did not exist in my local Gitea instance or on the github repo.  

This produced the technically accurate but somewhat disappointing, `TemplateNotFound`.
So I took a Linkedin Learning course on how to use git and added those three pages. I'm now a certified Git Master. Those templates existed very strongly in our hearts until now... now they live in our repos too.

---

### Sanoid Expected Debian to Create Files Debian Doesn't Create

So this is my fault for not checking, but I'm going to say that this is Debians fault for not being normal. 

![Why cant you just be normal](https://raw.githubusercontent.com/webzfs/webzfs/refs/heads/0.78/blog/images/debian.jpg)

I'm sorry my Debian friends, my brain just does not work the way yours does. Too many years of Slackware, FreeBSD, RHEL/Fedora, and Arch... the "Debian Way" just seems intentionally obtuse for no other reason than to be a distro for contrarians.

Anywho, the Sanoid integration expected `/etc/sanoid/sanoid.conf` to already exist. On Debian and Proxmox, that is not necessarily true.

After trying to manually creating it, users could then discover the hot action packed sequel: WebZFS did not have permission to write it. Now, 0.78 handles fresh Sanoid configuration properly on Debian and uses the correct privileged write path.

---

### Ubuntu Snaps Entered the Chat

Following on the "Why can't you be normal?" train... Snaps. 

One Ubuntu installer failure turned out to be Node.js and npm coming from, `/snap/bin/node` & `/snap/bin/npm`. The Snap refused to operate with the `webzfs` user's `/opt/webzfs` HOME. Even better, the Node Snap was version 10.24.1 while WebZFS requires Node 20+.

Our installer checked whether Node existed, it did not apparently feel the need to ask how old it was. In my defense, I was always told it's rude to ask a woman her age. 

0.78 improves Snap detection and actually validates the Node version. #professional_type_stuff

---

### Replication Progress Is Less Magical

Replication progress previously depended too much on process-local state and whichever Gunicorn worker happened to handle a request. 0.78 moves that state toward persistent per-execution tracking. This is less exciting than inventing nonexistent command-line flags, but it's considerably better software.

---

### And a Bunch of Normal Work Happened Too

There are plenty of less embarrassing improvements in 0.78 as well:

* better Syncoid and SSH integration,
* persistent replication history,
* SMART scheduling improvements,
* scheduler reconciliation at startup,
* dataset Peek functionality,
* pool export investigator updates,
* installer and updater hardening,
* FreeBSD updater improvements,
* and removal of a substantial amount of obsolete replication code.

Not every change began with us discovering something silly, but there were enough of them that it felt apt to make the release post entertaining.

---

### Thanks to Everyone Breaking It

Seriously, most of these bugs were found because people installed WebZFS on real systems and tried things I did not, or I hadn't tried in a while.  This is what makes Open Source as powerful as it is.  We write something cool, put it out there, and then everyone points out what an idiot we are. 

Never hesitate to create a bug in any of my projects when you find something wrong or odd.  It helps make the software better. If you filed one of these bugs: thank you.

If you discover that WebZFS offers a button for a feature ZFS does not have: tell me, I'll apologize, and then I'll tell you to maybe go tell OpenZFS that feature should exist. ;)

Seriously though, with my day work with Klarasystems and the OpenZFS Leadership calls, I hear so many discussions about cool features and new ideas... that sometimes I actually forget what's in the latest release.

Speaking of, that's one thing I need to do, go back and verify which features are 2.4.x only and make sure I flag and disable them in the UI if you're running 2.3. Someone go make that issue ticket on github.

I'm planning NetBSD 11 support with 0.79, but otherwise will only include small fixes as they are needed.
I've got three big new features planned for 0.80, which with any luck will introduce an entirely new and more sophisticated class of mistakes; so we all can sit back and laugh together.



And remember...  
![be_excellent_to_each_other](https://github.com/user-attachments/assets/cddda332-04e9-48a7-ab8c-b9a829896684)


