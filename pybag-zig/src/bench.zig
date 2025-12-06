const std = @import("std");
const mcap = @import("mcap.zig");
const mmap = @import("mmap.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <mcap_file> [iterations]\n", .{args[0]});
        return;
    }

    const path = args[1];
    const iterations: usize = if (args.len > 2) try std.fmt.parseInt(usize, args[2], 10) else 100;

    // Convert to null-terminated string
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    std.debug.print("Benchmarking: {s}\n", .{path});
    std.debug.print("Iterations: {}\n", .{iterations});

    // Warm up
    {
        var reader = try mcap.FastMcapReader.open(allocator, path_z.ptr);
        defer reader.close();
        const count = try reader.countMessages();
        std.debug.print("Message count: {}\n", .{count});
    }

    // Benchmark iterator
    var total_time_iter: u64 = 0;
    var total_messages_iter: usize = 0;

    for (0..iterations) |_| {
        var reader = try mcap.FastMcapReader.open(allocator, path_z.ptr);
        defer reader.close();

        const start = std.time.nanoTimestamp();
        var iter = reader.iterMessages();
        var count: usize = 0;
        while (iter.next()) |msg| {
            std.mem.doNotOptimizeAway(&msg);
            count += 1;
        }
        const end = std.time.nanoTimestamp();

        total_time_iter += @intCast(end - start);
        total_messages_iter += count;
    }

    const avg_time_iter_ns = total_time_iter / iterations;
    const avg_time_iter_us = @as(f64, @floatFromInt(avg_time_iter_ns)) / 1000.0;
    const msgs_per_iter = total_messages_iter / iterations;
    const throughput_iter = @as(f64, @floatFromInt(msgs_per_iter)) / (@as(f64, @floatFromInt(avg_time_iter_ns)) / 1_000_000_000.0);

    std.debug.print("\n=== Zig MCAP Reader Benchmark ===\n", .{});
    std.debug.print("Iterator method:\n", .{});
    std.debug.print("  Average time: {d:.2} µs\n", .{avg_time_iter_us});
    std.debug.print("  Messages per iteration: {}\n", .{msgs_per_iter});
    std.debug.print("  Throughput: {d:.2} msg/s\n", .{throughput_iter});

    // Benchmark countMessages (callback-based internally)
    var total_time_count: u64 = 0;

    for (0..iterations) |_| {
        var reader = try mcap.FastMcapReader.open(allocator, path_z.ptr);
        defer reader.close();

        const start = std.time.nanoTimestamp();
        const count = try reader.countMessages();
        std.mem.doNotOptimizeAway(&count);
        const end = std.time.nanoTimestamp();

        total_time_count += @intCast(end - start);
    }

    const avg_time_count_ns = total_time_count / iterations;
    const avg_time_count_us = @as(f64, @floatFromInt(avg_time_count_ns)) / 1000.0;
    const throughput_count = @as(f64, @floatFromInt(msgs_per_iter)) / (@as(f64, @floatFromInt(avg_time_count_ns)) / 1_000_000_000.0);

    std.debug.print("\nCount method (callback-based):\n", .{});
    std.debug.print("  Average time: {d:.2} µs\n", .{avg_time_count_us});
    std.debug.print("  Throughput: {d:.2} msg/s\n", .{throughput_count});
}
