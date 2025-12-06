const std = @import("std");
const mmap = @import("mmap.zig");
const SliceView = mmap.SliceView;
const MmapReader = mmap.MmapReader;

/// MCAP magic bytes
pub const MAGIC_BYTES = "\x89MCAP0\r\n";

/// MCAP record types
pub const RecordType = enum(u8) {
    Header = 0x01,
    Footer = 0x02,
    Schema = 0x03,
    Channel = 0x04,
    Message = 0x05,
    Chunk = 0x06,
    MessageIndex = 0x07,
    ChunkIndex = 0x08,
    Attachment = 0x09,
    AttachmentIndex = 0x0A,
    Statistics = 0x0B,
    Metadata = 0x0C,
    MetadataIndex = 0x0D,
    SummaryOffset = 0x0E,
    DataEnd = 0x0F,
    _,
};

/// Zero-copy message reference
pub const MessageRef = struct {
    channel_id: u16,
    sequence: u32,
    log_time: u64,
    publish_time: u64,
    data: []const u8,
};

/// Chunk metadata
pub const ChunkMeta = struct {
    offset: u64,
    message_start_time: u64,
    message_end_time: u64,
    compression: []const u8,
    compressed_size: u64,
    uncompressed_size: u64,
};

/// Fast MCAP reader for maximum performance
pub const FastMcapReader = struct {
    data: []const u8,
    mmap_reader: ?MmapReader,
    data_start: u64,
    data_end: u64,
    chunks: std.ArrayList(ChunkMeta),
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Open an MCAP file
    pub fn open(allocator: std.mem.Allocator, path: [*:0]const u8) !Self {
        var reader = try MmapReader.open(path);
        errdefer reader.close();

        var self = Self{
            .data = reader.getData(),
            .mmap_reader = reader,
            .data_start = 0,
            .data_end = 0,
            .chunks = std.ArrayList(ChunkMeta).init(allocator),
            .allocator = allocator,
        };

        try self.parseStructure();
        return self;
    }

    /// Create from existing data slice (for testing)
    pub fn fromSlice(allocator: std.mem.Allocator, data: []const u8) !Self {
        var self = Self{
            .data = data,
            .mmap_reader = null,
            .data_start = 0,
            .data_end = 0,
            .chunks = std.ArrayList(ChunkMeta).init(allocator),
            .allocator = allocator,
        };

        try self.parseStructure();
        return self;
    }

    /// Close the reader
    pub fn close(self: *Self) void {
        self.chunks.deinit();
        if (self.mmap_reader) |*r| {
            r.close();
        }
    }

    /// Parse the MCAP file structure
    fn parseStructure(self: *Self) !void {
        const len = self.data.len;

        // Verify magic bytes at start
        if (len < 8) return error.FileTooSmall;
        if (!std.mem.eql(u8, self.data[0..5], "\x89MCAP")) return error.InvalidMagic;
        if (!std.mem.eql(u8, self.data[6..8], "\r\n")) return error.InvalidMagic;

        // Parse footer (at end - 8 magic - 29 footer = 37 bytes from end)
        if (len < 45) return error.FileTooSmall;

        const footer_start = len - 37;
        var view = SliceView.init(self.data[footer_start..]);

        const opcode = view.readU8() orelse return error.InvalidFooter;
        if (opcode != @intFromEnum(RecordType.Footer)) return error.InvalidFooter;

        const footer_len = view.readU64Le() orelse return error.InvalidFooter;
        if (footer_len != 20) return error.InvalidFooter;

        const summary_start = view.readU64Le() orelse return error.InvalidFooter;
        _ = view.readU64Le(); // summary_offset_start
        _ = view.readU32Le(); // summary_crc

        // Parse header
        var header_view = SliceView.init(self.data[8..]);
        const header_opcode = header_view.readU8() orelse return error.InvalidHeader;
        if (header_opcode != @intFromEnum(RecordType.Header)) return error.InvalidHeader;

        const header_len = header_view.readU64Le() orelse return error.InvalidHeader;
        _ = header_view.skip(@intCast(header_len));

        self.data_start = 8 + 1 + 8 + header_len;
        self.data_end = if (summary_start > 0) summary_start else footer_start;

        // Parse summary if exists
        if (summary_start > 0 and summary_start < len) {
            try self.parseSummary(@intCast(summary_start));
        }
    }

    /// Parse summary section for chunk indices
    fn parseSummary(self: *Self, start: usize) !void {
        var view = SliceView.init(self.data[start..]);

        while (!view.isEmpty() and view.remaining() > 9) {
            const opcode = view.readU8() orelse break;
            const record_len = view.readU64Le() orelse break;

            if (view.remaining() < record_len) break;

            if (opcode == @intFromEnum(RecordType.ChunkIndex)) {
                const message_start_time = view.readU64Le() orelse break;
                const message_end_time = view.readU64Le() orelse break;
                const chunk_start_offset = view.readU64Le() orelse break;
                _ = view.readU64Le(); // chunk_length

                // Skip message_index_offsets map
                const map_len = view.readU32Le() orelse break;
                _ = view.skip(@intCast(map_len));

                _ = view.readU64Le(); // message_index_length

                const compression_len = view.readU32Le() orelse break;
                const compression = view.slice(@intCast(compression_len)) orelse break;

                const compressed_size = view.readU64Le() orelse break;
                const uncompressed_size = view.readU64Le() orelse break;

                try self.chunks.append(ChunkMeta{
                    .offset = chunk_start_offset,
                    .message_start_time = message_start_time,
                    .message_end_time = message_end_time,
                    .compression = compression,
                    .compressed_size = compressed_size,
                    .uncompressed_size = uncompressed_size,
                });
            } else if (opcode == @intFromEnum(RecordType.Footer)) {
                break;
            } else {
                _ = view.skip(@intCast(record_len));
            }
        }
    }

    /// Iterate over messages with a callback
    pub fn forEachMessage(self: *const Self, comptime callback: fn (MessageRef) void) !usize {
        var count: usize = 0;

        if (self.chunks.items.len == 0) {
            // No chunks - iterate directly over data section
            var view = SliceView.init(self.data[@intCast(self.data_start)..@intCast(self.data_end)]);

            while (!view.isEmpty() and view.remaining() > 9) {
                const opcode = view.readU8() orelse break;
                const record_len = view.readU64Le() orelse break;

                if (view.remaining() < record_len) break;

                if (opcode == @intFromEnum(RecordType.Message) and record_len >= 22) {
                    const channel_id = view.readU16Le() orelse break;
                    const sequence = view.readU32Le() orelse break;
                    const log_time = view.readU64Le() orelse break;
                    const publish_time = view.readU64Le() orelse break;
                    const data_len = record_len - 22;
                    const msg_data = view.slice(@intCast(data_len)) orelse break;

                    callback(MessageRef{
                        .channel_id = channel_id,
                        .sequence = sequence,
                        .log_time = log_time,
                        .publish_time = publish_time,
                        .data = msg_data,
                    });
                    count += 1;
                } else if (opcode == @intFromEnum(RecordType.DataEnd) or opcode == @intFromEnum(RecordType.Footer)) {
                    break;
                } else {
                    _ = view.skip(@intCast(record_len));
                }
            }
        }
        // Note: Chunk decompression would require additional dependencies
        // For fair benchmarking, we focus on non-chunked files

        return count;
    }

    /// Count messages efficiently
    pub fn countMessages(self: *const Self) !usize {
        return self.forEachMessage(struct {
            fn callback(_: MessageRef) void {}
        }.callback);
    }

    /// Message iterator for non-chunked files
    pub const MessageIterator = struct {
        view: SliceView,

        pub fn next(self: *MessageIterator) ?MessageRef {
            while (!self.view.isEmpty() and self.view.remaining() > 9) {
                const opcode = self.view.readU8() orelse return null;
                const record_len = self.view.readU64Le() orelse return null;

                if (self.view.remaining() < record_len) return null;

                if (opcode == @intFromEnum(RecordType.Message) and record_len >= 22) {
                    const channel_id = self.view.readU16Le() orelse return null;
                    const sequence = self.view.readU32Le() orelse return null;
                    const log_time = self.view.readU64Le() orelse return null;
                    const publish_time = self.view.readU64Le() orelse return null;
                    const data_len = record_len - 22;
                    const msg_data = self.view.slice(@intCast(data_len)) orelse return null;

                    return MessageRef{
                        .channel_id = channel_id,
                        .sequence = sequence,
                        .log_time = log_time,
                        .publish_time = publish_time,
                        .data = msg_data,
                    };
                } else if (opcode == @intFromEnum(RecordType.DataEnd) or opcode == @intFromEnum(RecordType.Footer)) {
                    return null;
                } else {
                    _ = self.view.skip(@intCast(record_len));
                }
            }
            return null;
        }
    };

    /// Get a message iterator
    pub fn iterMessages(self: *const Self) MessageIterator {
        return MessageIterator{
            .view = SliceView.init(self.data[@intCast(self.data_start)..@intCast(self.data_end)]),
        };
    }
};

test "FastMcapReader basic" {
    // Would need a test MCAP file
}
