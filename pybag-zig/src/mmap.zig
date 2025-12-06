const std = @import("std");
const c = @cImport({
    @cInclude("sys/mman.h");
    @cInclude("fcntl.h");
    @cInclude("unistd.h");
    @cInclude("sys/stat.h");
});

pub const MmapError = error{
    OpenFailed,
    StatFailed,
    MmapFailed,
    InvalidFile,
};

/// Memory-mapped file reader for zero-copy parsing.
pub const MmapReader = struct {
    data: []const u8,
    fd: c_int,

    const Self = @This();

    /// Open a file and memory-map it.
    pub fn open(path: [*:0]const u8) MmapError!Self {
        const fd = c.open(path, c.O_RDONLY);
        if (fd < 0) {
            return MmapError.OpenFailed;
        }
        errdefer _ = c.close(fd);

        var stat: c.struct_stat = undefined;
        if (c.fstat(fd, &stat) < 0) {
            return MmapError.StatFailed;
        }

        const size: usize = @intCast(stat.st_size);
        if (size == 0) {
            return MmapError.InvalidFile;
        }

        const ptr = c.mmap(null, size, c.PROT_READ, c.MAP_PRIVATE, fd, 0);
        if (ptr == c.MAP_FAILED) {
            return MmapError.MmapFailed;
        }

        const data_ptr: [*]const u8 = @ptrCast(ptr);
        return Self{
            .data = data_ptr[0..size],
            .fd = fd,
        };
    }

    /// Close the memory-mapped file.
    pub fn close(self: *Self) void {
        if (self.data.len > 0) {
            _ = c.munmap(@constCast(@ptrCast(self.data.ptr)), self.data.len);
        }
        if (self.fd >= 0) {
            _ = c.close(self.fd);
            self.fd = -1;
        }
    }

    /// Get the underlying data slice.
    pub inline fn getData(self: *const Self) []const u8 {
        return self.data;
    }

    /// Get the file size.
    pub inline fn len(self: *const Self) usize {
        return self.data.len;
    }
};

/// Slice view for zero-copy parsing.
pub const SliceView = struct {
    data: []const u8,
    pos: usize,

    const Self = @This();

    pub fn init(data: []const u8) Self {
        return Self{
            .data = data,
            .pos = 0,
        };
    }

    pub inline fn remaining(self: *const Self) usize {
        return self.data.len - self.pos;
    }

    pub inline fn isEmpty(self: *const Self) bool {
        return self.pos >= self.data.len;
    }

    pub inline fn slice(self: *Self, n: usize) ?[]const u8 {
        if (self.pos + n > self.data.len) return null;
        const start = self.pos;
        self.pos += n;
        return self.data[start..self.pos];
    }

    pub inline fn skip(self: *Self, n: usize) bool {
        if (self.pos + n > self.data.len) return false;
        self.pos += n;
        return true;
    }

    pub inline fn readU8(self: *Self) ?u8 {
        if (self.pos >= self.data.len) return null;
        const val = self.data[self.pos];
        self.pos += 1;
        return val;
    }

    pub inline fn readU16Le(self: *Self) ?u16 {
        const s = self.slice(2) orelse return null;
        return std.mem.readInt(u16, s[0..2], .little);
    }

    pub inline fn readU32Le(self: *Self) ?u32 {
        const s = self.slice(4) orelse return null;
        return std.mem.readInt(u32, s[0..4], .little);
    }

    pub inline fn readU64Le(self: *Self) ?u64 {
        const s = self.slice(8) orelse return null;
        return std.mem.readInt(u64, s[0..8], .little);
    }
};

test "MmapReader basic" {
    // Would need a test file
}

test "SliceView operations" {
    const data = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
    var view = SliceView.init(&data);

    try std.testing.expectEqual(@as(usize, 8), view.remaining());
    try std.testing.expectEqual(@as(u8, 0x01), view.readU8().?);
    try std.testing.expectEqual(@as(usize, 7), view.remaining());
}
