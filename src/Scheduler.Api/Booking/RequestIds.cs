using System.Security.Cryptography;
using System.Text;

namespace Scheduler.Api.Booking;

/// <summary>
/// Request ids: UUIDv7 by default (time-ordered, so the API can tell a long-unknown id from a pending one),
/// or a UUIDv5 derived from the client's Idempotency-Key, so a retry maps to the same request.
/// </summary>
public static class RequestIds
{
    private static readonly Guid IdempotencyNamespace = new("5d0a6f0e-3c1b-4f53-9a43-6c2f8f6e2b17");

    public static Guid New() => Guid.CreateVersion7();

    public static Guid FromIdempotencyKey(string key)
    {
        // RFC 9562 §5.5: SHA-1 over namespace bytes + name, then set version 5 and the RFC variant.
        var name = Encoding.UTF8.GetBytes(key);
        Span<byte> input = stackalloc byte[16 + name.Length];
        IdempotencyNamespace.TryWriteBytes(input[..16], bigEndian: true, out _);
        name.CopyTo(input[16..]);
        Span<byte> hash = stackalloc byte[20];
        SHA1.HashData(input, hash);
        hash[6] = (byte)((hash[6] & 0x0F) | 0x50);
        hash[8] = (byte)((hash[8] & 0x3F) | 0x80);
        return new Guid(hash[..16], bigEndian: true);
    }

    /// <summary>The creation time embedded in a UUIDv7, or null for other versions.</summary>
    public static DateTimeOffset? CreatedAt(Guid id)
    {
        if (id.Version != 7) return null;
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        long ms = 0;
        for (var i = 0; i < 6; i++) ms = (ms << 8) | bytes[i];
        return DateTimeOffset.FromUnixTimeMilliseconds(ms);
    }
}
