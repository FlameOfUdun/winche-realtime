namespace Winche.Realtime;

/// <summary>An item to append: the client's key (a retry of it never duplicates) and its payload (bytes, opaque here).</summary>
public sealed record RealtimeItemIn(string Key, byte[] Payload);

/// <summary>An item in a list: its key, its position (the same order for every reader), when, by whom, and its payload.</summary>
public sealed record RealtimeItem(string Key, long Position, DateTimeOffset At, string? By, byte[] Payload);

/// <summary>A list: its state (open, closed), how many items and payload bytes it holds, when it opened and closed.</summary>
public sealed record RealtimeListInfo(string Path, string State, long Count, long Bytes)
{
    public DateTimeOffset OpenedAt { get; init; }
    public DateTimeOffset? ClosedAt { get; init; }
}

/// <summary>A page of a list: items after a position, and the list (null when there's none at the path).</summary>
public sealed record RealtimePage(IReadOnlyList<RealtimeItem> Items, RealtimeListInfo? List);

/// <summary>Why Realtime refused: a shared error code, and a sentence.</summary>
public sealed class RealtimeException(WincheErrorCode code, string message, string? reason = null) : Exception(message)
{
    public WincheErrorCode Code { get; } = code;
    public string? Reason { get; } = reason;

    public WincheError ToError() => new(Code, Message, Reason);
}
