---
title: "Convenient Per-Dataset I/O Monitoring is finally a thing"
date: 2026-07-13
tags: [feature, zfs, observability, iostat, performance]
---

# Finally, a `zfs iostat` (Sort Of)

Quick one this week. I shipped a new **Per-Dataset I/O Stats** page under ZFS Observability, and I'm pretty happy with how it turned out.

## Why I Built It

Here's the itch I've been meaning to scratch for years. OpenZFS gives you `zpool iostat`, which is great for watching I/O at the pool and vdev level. What it does not give you is a `zfs iostat`. There is no built-in way to ask "which of my datasets is actually generating all this traffic right now?" You can see that the pool is busy, but not who is making it busy.

That has quietly annoyed me for a long time. When you have dozens of datasets on a pool and something is hammering the disks, "the pool is busy" is not a useful answer. I wanted to see it broken down per dataset, live.

## The Proof-of-Concept First

Before wiring anything into WebZFS, I wanted to prove the data was even reliably available across platforms. So I started with a standalone CLI utility to test the idea:

- https://github.com/webzfs/zfs-iostat

That tool is the proof-of-concept. It confirmed I could pull the per-dataset counters I needed and compute meaningful rates from them. It is Python only for now, and honestly it started as a scratchpad to make sure the whole thing was actually possible before I committed to it.

I do plan a C++ version of that utility soon. I would write a native C version and try to get it merged upstream into OpenZFS itself, because that is where it really belongs. But I will be the first to admit I suck at C, so realistically this is going to live as an extra utility people can add and build on their own rather than something I push into the core project.

## How It Works in WebZFS

Once the concept held up, I brought it into the Observability dashboard. There is a new "Per-Dataset I/O Stats" card that takes you to a live view. It sits right alongside the existing pool-level and per-vdev I/O pages, so now you can drill from pool, to vdev, to individual dataset.

The rates are computed the same way `zpool iostat` does it. WebZFS samples the ZFS dataset kstats, waits a short interval, samples again, and divides the counter deltas by the actual elapsed time. Nothing invented, nothing stored, just reading the counters the kernel already exposes. Counter resets from a remount clamp to zero so you don't see garbage spikes.

Platform-wise it works where you'd expect. On Linux it reads the objset kstats under `/proc/spl/kstat/zfs`. On FreeBSD and NetBSD it reads the same information out of the `kstat.zfs` sysctl tree. No elevated privileges needed just to watch I/O.

A few things I made sure to include:

- **Default view**: every dataset with its read/write operations and bandwidth rates, auto-refreshing every few seconds.
- **Top view**: the same data sorted by activity so the busiest datasets float to the top. Sort by total, read, write, or operations.
- **Files view**: lists the open files under each dataset and which processes are holding them. This can help you track down processes that are being a bit too greedy with file access.

There is also a dataset filter with an "Include children" toggle, so you can scope the view to a single dataset or a whole branch of the tree.

## Wrapping Up

This is one of those features that is more about closing a personal long-standing gap than reinventing anything. OpenZFS has always had the data; it just never had a friendly `zfs iostat` front door. Now WebZFS does, and the standalone CLI tool is out there too if you'd rather live on the command line.
