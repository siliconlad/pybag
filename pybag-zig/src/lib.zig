const std = @import("std");
const c = @cImport({
    @cInclude("sys/mman.h");
    @cInclude("fcntl.h");
    @cInclude("unistd.h");
    @cInclude("sys/stat.h");
});

pub const mcap = @import("mcap.zig");
pub const MmapReader = @import("mmap.zig").MmapReader;

test "basic mcap parsing" {
    // Tests will be added
}
