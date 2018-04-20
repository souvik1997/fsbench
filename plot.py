#!/usr/bin/env python3

import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
from array import array
from matplotlib import colors
import json
import sys

order = ["ext2", "ext4", "ext4-no-journal", "btrfs", "f2fs", "xfs"]
patterns = ('.','', 'x', '-','\\')

def parse_duration(duration):
    return float(duration["secs"]) + float(duration["nanos"]) / 1E9

def parse_reads(reads):
    return int(reads)

def parse_writes(writes):
    return int(writes)

def triple_plot(first, first_label, second, second_label, third, third_label, title, ylabel):
    width = 0.2
    ind = np.arange(len(order))
    tplot_fig, tplot_ax = plt.subplots()
    tplot_fig.set_size_inches(9, 5)
    createfiles_tplot_rects = tplot_ax.bar(ind - width, first, width, color="SkyBlue", label=first_label)
    createfiles_batchsync_tplot_rects = tplot_ax.bar(ind, second, width, color="Red", label=second_label)
    createfiles_eachsync_tplot_rects = tplot_ax.bar(ind + width, third, width, color="Green", label=third_label)
    for rect, pattern in zip(createfiles_tplot_rects, patterns):
        rect.set_hatch(pattern)
    for rect, pattern in zip(createfiles_batchsync_tplot_rects, patterns):
        rect.set_hatch(pattern)
    for rect, pattern in zip(createfiles_eachsync_tplot_rects, patterns):
        rect.set_hatch(pattern)
    tplot_ax.set_title(title)
    tplot_ax.set_ylabel(ylabel)
    tplot_ax.set_xticks(ind)
    tplot_ax.set_xticklabels(order)
    tplot_ax.legend()
    return tplot_fig

def double_plot(first, first_label, second, second_label, title, ylabel):
    width = 0.2
    ind = np.arange(len(order))
    dplot_fig, dplot_ax = plt.subplots()
    createfiles_dplot_rects = dplot_ax.bar(ind - width, first, width, color="SkyBlue", label=first_label)
    createfiles_batchsync_dplot_rects = dplot_ax.bar(ind, second, width, color="Red", label=second_label)
    for rect, pattern in zip(createfiles_dplot_rects, patterns):
        rect.set_hatch(pattern)
    for rect, pattern in zip(createfiles_batchsync_dplot_rects, patterns):
        rect.set_hatch(pattern)
    dplot_ax.set_title(title)
    dplot_ax.set_ylabel(ylabel)
    dplot_ax.set_xticks(ind)
    dplot_ax.set_xticklabels(order)
    dplot_ax.legend()
    return dplot_fig


def single_plot(data, title, ylabel):
    width = 0.2
    ind = np.arange(len(order))
    plot_fig, plot_ax = plt.subplots()
    plot_rects = plot_ax.bar(ind, data, width, color="SkyBlue")
    for rect, pattern in zip(plot_rects, patterns):
        rect.set_hatch(pattern)
    plot_ax.set_title(title)
    plot_ax.set_ylabel(ylabel)
    plot_ax.set_xticks(ind)
    plot_ax.set_xticklabels(order)
    return plot_fig



def plot_poster(data):
    createfiles_eachsync_duration = [100000.0 / parse_duration(data[fs][2]["duration"]) for fs in order]
    createfiles_eachsync_reads = [parse_reads(data[fs][2]["reads"]) / 1024.0 / 100000.0 for fs in order]
    createfiles_eachsync_writes = [parse_writes(data[fs][2]["writes"]) / 1024.0 / 100000.0 for fs in order]
    single_plot(createfiles_eachsync_duration, "Create Files: Throughput", "Files/Second").savefig("plots/poster-createfiles-duration.png")
    single_plot(createfiles_eachsync_reads, "Create Files: Reads", "KiB/File").savefig("plots/poster-createfiles-reads.png")
    single_plot(createfiles_eachsync_writes, "Create Files: Writes", "KiB/File").savefig("plots/poster-createfiles-writes.png")

def plot_createfiles(data):
    # Plot duration
    createfiles_duration = [parse_duration(data[fs][0]["duration"]) for fs in order]
    createfiles_batchsync_duration = [parse_duration(data[fs][1]["duration"]) for fs in order]
    createfiles_eachsync_duration = [parse_duration(data[fs][2]["duration"]) for fs in order]
    createfiles_reads = [parse_reads(data[fs][0]["reads"]) / 400000 / 1024 for fs in order]
    createfiles_batchsync_reads = [parse_reads(data[fs][1]["reads"]) / 400000 / 1024 for fs in order]
    createfiles_eachsync_reads = [parse_reads(data[fs][2]["reads"]) / 400000 / 1024 for fs in order]
    createfiles_writes = [parse_writes(data[fs][0]["writes"]) / 400000 / 1024 for fs in order]
    createfiles_batchsync_writes = [parse_writes(data[fs][1]["writes"]) / 400000 / 1024 for fs in order]
    createfiles_eachsync_writes = [parse_writes(data[fs][2]["writes"]) / 400000 / 1024 for fs in order]


    single_plot(createfiles_duration, "Create Files Duration: No fsync", "Seconds").savefig("plots/createfiles-duration.png")
    double_plot(createfiles_batchsync_duration, "Batch fsync", createfiles_eachsync_duration, "Frequent fsync", "Createfiles Duration", "Seconds").savefig("plots/createfiles-duration-sync.png")

    triple_plot(createfiles_reads, "Create Files Reads: No fsync", createfiles_batchsync_reads, "Batch fsync", createfiles_eachsync_reads, "Frequent fsync", "Createfiles Reads", "KiB/File").savefig("plots/createfiles-reads.png")

    single_plot(createfiles_writes, "Create Files Writes: No fsync", "KiB/File").savefig("plots/createfiles-writes.png")
    double_plot(createfiles_batchsync_writes, "Batch fsync", createfiles_eachsync_writes, "Frequent fsync", "Createfiles Writes", "KiB/File").savefig("plots/createfiles-writes-sync.png")

def plot_renamefiles(data):
    renamefiles_duration = [parse_duration(data[fs][3]["duration"]) for fs in order]
    renamefiles_reads = [parse_reads(data[fs][3]["reads"]) for fs in order]
    renamefiles_writes = [parse_writes(data[fs][3]["writes"]) for fs in order]
    single_plot(renamefiles_duration, "Renamefiles Duration", "Seconds").savefig("plots/renamefiles-duration.png")
    double_plot(renamefiles_reads, "Reads", renamefiles_writes, "Writes", "Rename IO", "Bytes").savefig("plots/renamefiles-io.png")

def plot_deletefiles(data):
    deletefiles_duration = [parse_duration(data[fs][4]["duration"]) for fs in order]
    deletefiles_reads = [parse_reads(data[fs][4]["reads"]) for fs in order]
    deletefiles_writes = [parse_writes(data[fs][4]["writes"]) for fs in order]
    single_plot(deletefiles_duration, "Deletefiles Duration", "Seconds").savefig("plots/deletefiles-duration.png")
    double_plot(deletefiles_reads, "Reads", deletefiles_writes, "Writes", "Delete IO", "Bytes").savefig("plots/deletefiles-io.png")

def plot_listdir(data):
    listdirfiles_duration = [parse_duration(data[fs][5]["duration"]) for fs in order]
    listdirfiles_reads = [parse_reads(data[fs][5]["reads"]) for fs in order]
    listdirfiles_writes = [parse_writes(data[fs][5]["writes"]) for fs in order]
    single_plot(listdirfiles_duration, "Listdir Duration", "Seconds").savefig("plots/listdir-duration.png")
    double_plot(listdirfiles_reads, "Reads", listdirfiles_writes, "Writes", "Listdir IO", "Bytes").savefig("plots/listdir-io.png")


def main():
    data = {}
    data["ext2"] = json.load(open("ext2/summary.json", "r"))
    data["ext4"] = json.load(open("ext4/summary.json", "r"))
    data["btrfs"] = json.load(open("btrfs/summary.json", "r"))
    data["f2fs"] = json.load(open("f2fs/summary.json", "r"))
    data["xfs"] = json.load(open("xfs/summary.json", "r"))
    data["ext4-no-journal"] = json.load(open("ext4-no-journal/summary.json", "r"))
    plot_createfiles(data)
    plot_renamefiles(data)
    plot_deletefiles(data)
    plot_listdir(data)
    plot_poster(data)

    plt.show()

if __name__ == "__main__":
    main()
