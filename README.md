# SMR

SMR, i.e, Submodular Maximization Rewriting Scheme, is a cost-efficient rewriting scheme to improve restore performance in deduplication systems.

# The SMR paper

Jie Wu , Yu Hua , Pengfei Zuo and Yuanyuan Sun, "A Cost-efficient Rewriting Scheme to Improve Restore Performance in Deduplication Systems".

# Implementation

We use the interfaces in [Destor](https://github.com/fomy/destor) and make some revisions to implement SMR and support C++. 

The detailed implementation of SMR scheme is in the code file `destor-smr/src/smr_rewrite.cpp`.

# Environment

Linux 64bit.

# Dependencies

1. libssl-dev is required to calculate sha-1 digest;
2. GLib 2.32 or later version

# Installation

If all dependencies are installed, compiling destor is straightforward:

> ./configure

> make

> make install

# Running

The `rebuild` script should be run before destor to prepare working directory and clear data.

The default working path is in destor.config. You can modify it together with the rebuild script. 

You can do the following steps to backup and restore the backup stream.

1. start a backup task,

   > destor /backup_stream_directory

   Example: `destor /home/data/`


2. start a restore task,

   > destor -r$backup_job_id$ /restore_direstory

   Example: `destor -r0 /home/restore/ `

# Configuration

A sample configuration is shown in destor.config

The default rewriting scheme is SMR. The SMR level in destor.config is adjustable according  the demand of restore performance.

# Contact

Email: wujie@hust.edu.cn

If you have any questions, please feel free to contact me.

