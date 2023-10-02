
-- sub-division tree; this gets easier if we start with shard_id=1 for the smallest possible:
--
--                      1
--           2                   3
--       4      5            6        7
-- ... so the first shard_id is simply the
-- NOTE: The actual sub-division isn't as pictured above -- that would requiring reversing the relevant bits
-- of the shard_id -- the sub-division tree looks different. But that is on TODO, here we were just counting.

create function [changefeed].start_offset_for_shard_count(@shard_count int) returns int
as begin
    return (case
        when @shard_count in (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384) then @shard_count
        else null
        end);
end
