const std = @import("std");
const c = @cImport({
    @cInclude("sys/mman.h");
    @cInclude("fcntl.h");
    @cInclude("unistd.h");
    @cInclude("sys/stat.h");
});

const RecordType = enum(u8) {
    Header = 0x01,
    Footer = 0x02,
    Schema = 0x03,
    Channel = 0x04,
    Message = 0x05,
    Chunk = 0x06,
    DataEnd = 0x0F,
    _,
};

inline fn readU8(data: []const u8, pos: *usize) ?u8 {
    if (pos.* >= data.len) return null;
    const val = data[pos.*];
    pos.* += 1;
    return val;
}

inline fn readU64Le(data: []const u8, pos: *usize) ?u64 {
    if (pos.* + 8 > data.len) return null;
    const val = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return val;
}

fn getDataBounds(data: []const u8) !struct { start: usize, end: usize } {
    const len = data.len;
    if (len < 45) return error.FileTooSmall;
    if (!std.mem.eql(u8, data[0..5], "\x89MCAP")) return error.InvalidMagic;

    const footer_start = len - 37;
    var fpos: usize = footer_start;
    const footer_op = readU8(data, &fpos) orelse return error.InvalidFooter;
    if (footer_op != @intFromEnum(RecordType.Footer)) return error.InvalidFooter;
    _ = readU64Le(data, &fpos);
    const summary_start = readU64Le(data, &fpos) orelse return error.InvalidFooter;

    var hpos: usize = 8;
    const header_op = readU8(data, &hpos) orelse return error.InvalidHeader;
    if (header_op != @intFromEnum(RecordType.Header)) return error.InvalidHeader;
    const header_len = readU64Le(data, &hpos) orelse return error.InvalidHeader;
    hpos += @intCast(header_len);

    return .{
        .start = hpos,
        .end = if (summary_start > 0) @intCast(summary_start) else footer_start,
    };
}

fn countMessagesFromBounds(data: []const u8, data_start: usize, data_end: usize) usize {
    var count: usize = 0;
    var pos: usize = data_start;
    while (pos + 9 < data_end) {
        const opcode = data[pos];
        pos += 1;
        const record_len = std.mem.readInt(u64, data[pos..][0..8], .little);
        pos += 8;
        if (pos + record_len > data_end) break;

        if (opcode == @intFromEnum(RecordType.Message)) {
            count += 1;
        } else if (opcode == @intFromEnum(RecordType.DataEnd) or opcode == @intFromEnum(RecordType.Footer)) {
            break;
        }
        pos += @intCast(record_len);
    }
    return count;
}

pub fn main() !void {
    const args = std.process.args();
    var arg_iter = args;
    _ = arg_iter.skip();
    
    const path = arg_iter.next() orelse {
        std.debug.print("Usage: bench <mcap_file> [iterations]\n", .{});
        return;
    };
    
    const iterations: usize = if (arg_iter.next()) |iter_str|
        std.fmt.parseInt(usize, iter_str, 10) catch 100
    else
        100;

    const fd = c.open(path.ptr, c.O_RDONLY);
    if (fd < 0) return;
    defer _ = c.close(fd);

    var stat_buf: c.struct_stat = undefined;
    if (c.fstat(fd, &stat_buf) < 0) return;

    const size: usize = @intCast(stat_buf.st_size);
    const ptr = c.mmap(null, size, c.PROT_READ, c.MAP_PRIVATE, fd, 0);
    if (ptr == c.MAP_FAILED) return;
    defer _ = c.munmap(@constCast(ptr), size);

    const data: []const u8 = @as([*]const u8, @ptrCast(ptr))[0..size];
    const bounds = getDataBounds(data) catch return;
    const msg_count = countMessagesFromBounds(data, bounds.start, bounds.end);

    std.debug.print("File: {s}\n", .{path});
    std.debug.print("Messages: {d}\n", .{msg_count});
    std.debug.print("Iterations: {d}\n", .{iterations});

    // Benchmark - file already open, just iterate
    var total_time: i128 = 0;
    for (0..iterations) |_| {
        const start = std.time.nanoTimestamp();
        const count = countMessagesFromBounds(data, bounds.start, bounds.end);
        std.mem.doNotOptimizeAway(&count);
        const end = std.time.nanoTimestamp();
        total_time += end - start;
    }

    const avg_ns: f64 = @as(f64, @floatFromInt(total_time)) / @as(f64, @floatFromInt(iterations));
    const avg_us = avg_ns / 1000.0;
    const throughput = @as(f64, @floatFromInt(msg_count)) / (avg_ns / 1_000_000_000.0);

    std.debug.print("\n=== Zig MCAP Benchmark (pure iteration) ===\n", .{});
    std.debug.print("Average time: {d:.2} µs\n", .{avg_us});
    std.debug.print("Throughput: {d:.2} msg/s ({d:.2} M/s)\n", .{ throughput, throughput / 1_000_000.0 });
}
