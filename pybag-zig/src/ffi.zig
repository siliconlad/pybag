const std = @import("std");
const mcap = @import("mcap.zig");
const mmap = @import("mmap.zig");

// C-compatible message structure
pub const CMessage = extern struct {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data_ptr: [*]const u8,
    data_len: usize,
};

// Opaque reader handle
pub const ReaderHandle = opaque {};

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

/// Open an MCAP file and return a handle
export fn pybag_zig_open(path: [*:0]const u8) ?*ReaderHandle {
    const reader = mcap.FastMcapReader.open(gpa.allocator(), path) catch return null;
    const ptr = gpa.allocator().create(mcap.FastMcapReader) catch return null;
    ptr.* = reader;
    return @ptrCast(ptr);
}

/// Close the reader and free resources
export fn pybag_zig_close(handle: ?*ReaderHandle) void {
    if (handle) |h| {
        const reader: *mcap.FastMcapReader = @ptrCast(@alignCast(h));
        reader.close();
        gpa.allocator().destroy(reader);
    }
}

/// Count messages in the file
export fn pybag_zig_count_messages(handle: ?*ReaderHandle) i64 {
    if (handle) |h| {
        const reader: *const mcap.FastMcapReader = @ptrCast(@alignCast(h));
        const count = reader.countMessages() catch return -1;
        return @intCast(count);
    }
    return -1;
}

/// Callback type for message iteration
pub const MessageCallback = *const fn (*const CMessage, ?*anyopaque) callconv(.C) void;

/// Iterate over messages with a callback
export fn pybag_zig_for_each_message(
    handle: ?*ReaderHandle,
    callback: MessageCallback,
    user_data: ?*anyopaque,
) i64 {
    if (handle) |h| {
        const reader: *const mcap.FastMcapReader = @ptrCast(@alignCast(h));
        var count: i64 = 0;

        var iter = reader.iterMessages();
        while (iter.next()) |msg| {
            const c_msg = CMessage{
                .channel_id = msg.channel_id,
                .sequence = msg.sequence,
                .log_time = msg.log_time,
                .publish_time = msg.publish_time,
                .data_ptr = msg.data.ptr,
                .data_len = msg.data.len,
            };
            callback(&c_msg, user_data);
            count += 1;
        }
        return count;
    }
    return -1;
}

// For benchmarking: direct message iteration returning count
export fn pybag_zig_iterate_messages(handle: ?*ReaderHandle) i64 {
    if (handle) |h| {
        const reader: *const mcap.FastMcapReader = @ptrCast(@alignCast(h));
        var count: i64 = 0;

        var iter = reader.iterMessages();
        while (iter.next()) |_| {
            count += 1;
        }
        return count;
    }
    return -1;
}
