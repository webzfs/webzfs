---
title: "Thoughts: Why so many ZFS web UI's now?"
date: 2026-08-31
tags: [question, answer, thoughts]
---

# Why so many ZFS web UI's now?

While talking with a friend about some homelab stuff a question came up that I've seen other people make reference to as well. What is with all of the new ZFS management web UI that have come out recently? This is happening often enough that it has actually become a meme on the ZFS subreddit for "How many days since the last AI slop coded ZFS tool was made".  This clearly is happening since so many people have noticed it. 

To be crystal clear up front, I don't want this to come off as an attack but I think it's a relevant point that needs to be made. I can see how some people might want to intepret it as hot take, but I do not mean it that way.

When TrueNAS shifted from FreeBSD to Linux, they kind of made the mistake of promising the world. When the marquee feature of their "SCALE out storage" completely blew up in their faces (because they were unaware that Red Hat had already announced the EOL of Gluster); they were left trying to scramble to figure out how to make TrueNAS on Linux be a standout product. 

This is not a good place to find yourself, whoever you are.  Even worse if you're a business; even worse if you're a business that has already had a major public blunder with the whole TrueNAS Corral saga. Slight interjection and shameless self promotion; back when I worked at IX I did an interview with the now CEO, then Executive VP of IX systems, Brett Davis, about iX's legacy; where they came from and where they were going. We specifically talked on the TrueNAS Corral issue and why iX was able to weather that storm. So if you're interested in that go give that episode listen. [Open Source Voices](https://www.opensourcevoices.org/30)

Anywho, popping the stack back to the point. A second major public blender would not have gone well. Enterprise businesses are very very touchy about the stability of tools and systems they rely upon.  Two major blunders in a decade is going to be a hard thing for sales and marketing to hand wave away. So in a sense, the TrueNAS project was faced with the only good option being to double down, batten down the hatches, and really make their vision work, make it amazing, and hopefully pull a rabbit out of their hat and wow everybody. 
Unfortunately that hasn't happened yet.  The future still exists though, so here's to hoping they can pull it off. (Raises Glass)

Anyway, when any project has massive fundamental changes with almost every release like TrueNAS has, and you're designing an Enterprise product, but you're replacing major back end services with every release... well there's no real easy way to say it... It makes it look like you don't know what you're doing. 

Let's consider an analogy so we're not so directly tied to the actual issue. If I started a restaurant to sell hot dogs, and I told everybody that these were the best hot dogs in the world, and over years I perfected my hot dog receipe; people would be shocked when I come out years later and claim that I'm changing my hot dog restaurant to be a burger restaurant, because Burgers are better. 
If I started telling everyone that Burgers are the future, and I'm going to have the best burgers in the world, that's a lot to live up to. If a year or so later I then come out and tell everyone that actually "Burgers are just ok", and that pancakes are the best thing, people would scratch their heads. Now imaging that after changing selling pancakes and not selling burgers, a year later I come out and claim that actually... Waffles are the new hotness. I explain that pankcakes are basically waffles, but waffles are better. (Insert Mitch Hedberg joke here if you know it).
People would again scratch their head. If about a year after that... I then came back out and make the claim that Waffles were a distraction and that pancakes are where I need to focus; people are going to start to wonder if I've lost the plot. 
People would look at me and think you have no idea how to run a restaurant. 
And based on the fact that I have contradicted myself multiple times about what's the good food and what's the bad food, it kind of looks like I don't know what is good or bad. 

I think this is the perspective that a lot of people are unfortunately taking when it comes to TrueNAS. They see major changes, they see major back pedals, and every single time there's always the claim about "now" what they're doing next is going to be great... Right up until they change their mind and then the next thing is going to be great. Or they go back to the thing that used to be great... but then it wasn't great... but now it's great again.  That's going to cause people to question the people behind TrueNAS.  

Let me stop here and be very cleary about something. The people working on TrueNAS, are smart people. These are not dumb, they're great engineers that are working hard to create a good solid dependable product, they're just facing challenging work.

However, reality on the ground can't be denied. It's rough being a TrueNAS user right now, and in an environment like that people are going to look for stability. Now that AI development is a thing... they're going to be a lot of people that think that most sinister of thoughts... "How hard can it be?"
Anyone who's worked in software engineering for a while or even hardware engineering for a while, has has thought that thought about something. And if they've tried to make a go of it, they've usually discovered that it's actually a whole lot harder than they thought it was.  As they ride that Dunning-Kruger wave they realize that they didn't actually know what they were talking about before, but they settle in and start learning and making progress towards a quality project. 
 
One of the bigger downsides of AI that I dont think people talk about; is that it allows people to never hit the down slope. Because they're not doing any of the thinking or learning on their own (since they've outsourced that to the model); they continue to think that everything they're doing is amazing. 
My code is not perfect, nor will it ever be. In the three-ish years that I worked on WebZFS before releasing it, I did a lot of dumb stuff. And doing that dumb stuff and then realizing that it was dumb helped me learn more. 

AI development, is one of these reaosns I think we see the feature set of some of these new management tools growing at an astronomical pace. There simply is not enough time to write, test, and document all of this code by hand.  Even if someone was dedicating 8 hours a day to development and testing and documentation, features would not be shipping the fast. I would venture a guess that for a lot of these projects, their 'testing' is literally the meme of "It works on my box". 

Having worked in QA at IX before, and written tests specifically to try to target and discover edge cases within software... Im keenly aware that something working great on one system doesn't matter much. "Working properly" needs to be reproducible.
There's a big difference between a software application that has couple dozen to a hundred or so users... and a software application or project that has tens of thousands to potentially millions of users. In my opinion you should not trust your storage (enterprise or not) to someone who has not had some kind of experience at that kind of scale.

I apply that same standard to my work, my current building and test system is a FreeBSD 15 box, running bhyve with... I think 11 test VMS. FreeBSD 14.4, 15.0, and 15.1, NetBSD 10.1, and 11.0. RockyLinux 8, 9, and 10, Ubuntu 2604, and I recently added the latest proxmox version. (so that's 10, I promise I know how to count).
I will add additional VMS as new releases come out or specific bugs are found in the way the install or setup takes place on a certain distro.  

Popping the stack and getting back to the point, I think a lot of these people that are creating these new web based ZFS tools are trying to fill a niche because they know a lot of people are looking for alternatives. While I don't know these people personally, I can look at the trend of the way a lot of vibecode promote "their work", in a chase for accolades, clout, and magic internet points.  So I wouldn't be surprised if ego is not also in play.

Of course we can't avoid the elephant in the room of financial benefit when at least one of the projects I've seen promoted in the few months projects has attach payment to being able to use it. Not to imply that it's malicious, because as the job market has become a lot more brutal, people are leaning on being able to show projects "they made" as some way to distinguish their resume as they're trying to find other work.
Anybody that's worked in Enterprise and had to go through the process of software procurement at an Enterprise level knows there's a lot of critical eyes that go into reviewing things before dollars come out. (Yes yes I know there are exceptions)  While I could be wrong I don't expect any of these ZFS management utilities to be a windfall of money. And I would absolutely say the same about webzfs.

I also believe that a lot of these new web-based management tools that are coming out, are from people who have only casually used ZFS. Not a single one is from someone who I have ever been aware of in the ZFS ecosystem or community. While obviously I don't know everyone in the ZFS world I've been deeply involved in that world for a decade, and a user for longer.  I could be totally wrong on that, and hey, if I am... I'm happy to admit I'm wrong.  But someone who was involved in ZFS for a while would have some type of ZFS footprint, they'd have worked somewhere that helped build ZFS, etc.  I hope I'm wrong though, I do not want to think about the future if ZFS management is being done by software written by people that dont know what they're doing.
To take this point a step further, some of these people have virtually no open source involvement or development experience that's visible of any kind. It doesn't mean that they haven't worked in it software dev before, they very well may have, but there is no track record of their involvement with large or major Open Source projects that have a lot of users.

Open Source Software promotion is weird, its always kinda has been. I dont expect it to get any easier in the age of AI. Before AI there was quite a differnce in opinion on how to promote a project, I dont think that dillemna is going to go away.
IMHO, a good stable project speaks for itself. When people find something that works and works really well and solves the problem, they promote it for you. This is something that I learned very well from iX. TrueNAS's engagement with the homelab and soho community, built an image of quality and dedication to the goal of the software. IT people that had their own home lab, took those solid experiences into their place of work and into their Enterprise and when the discussion around a storage appliance came up; FreeNAS/TrueNAS was right at the top of the suggestions. 

Sure constantly making fanfare and Reddit posts may get you eyeballs, but if the software is crap you're not going to last the distance.

Ironically the people who are trying to create an alternative to TrueNAS's "Move fast and break things and we'll get to something stable eventually"... are doing the very exact same thing in "their project". They're moving as fast as possible.

When I finally did release WebZFS, I pushed it to GitHub and that was it. I didn't make a single social media post about it. I talked with a few people that I talked to during building WebZFS and asked them to give it a try and see what they thought. i wanted to know what what bugs they found and wanted to know if they thought I did something really stupid that I should change how that was done.  To this day I still have never made a social media post about it. I didn't even start making blog posts within this project until like a month or so ago. The one thread that ended up on Reddit was from someone else that had tried it and liked it and decided to post. 

To be frank, at this point I don't know that I would go try to make a post about it on Reddit because there's been so many AI slop Management systems that people have "made", but I don't want to end up having to go through that same debate with people online. I have no deasire to yet again go through the slog of trying to convince people that I am not a random person using Claude. I've been doing enterprise ZFS things for over a decade, I've been an open source developer for two decades, and I've been a Linux user for over three decades. 
We covered a recent blog post in BSD now where someone was lamenting that the kind of don't even want to share anymore because of the way the ecosystem in the space is right now. 
And I completely understand that sentiment. 

So for now I'm just going to keep working on the software. I loathe to actually do this but I'm going to quote Mark Zuckerberg. 

"Code wins arguments". 

I'm going to focus on the code and leave the fanfare PR and drama to everyone else.


PS. When I mentioned my build system... I'm currently refactoring all of it. it's going to take me another week or two, but once I do get that all set up I will be making another post about it and I will be pushing scripts and documentation for what I'm doing to an 'infra' repo under the webzfs organization.

