namespace RaftLab.Sim;

/// <summary>
/// Собственный генератор (SplitMix64): результат зависит только от зерна, а не от версии .NET или системы.
/// Это условие воспроизводимости: один и тот же прогон на Windows и в Linux-CI даёт одну и ту же историю.
/// </summary>
public sealed class SimRandom : Random
{
    private ulong _state;

    public SimRandom(ulong seed) => _state = seed;

    public static SimRandom Derive(long seed, ulong salt) => new(unchecked((ulong)seed * 0x9E3779B97F4A7C15UL ^ (salt + 0xD1B54A32D192ED03UL) * 0xA24BAED4963EE407UL));

    public ulong NextU64()
    {
        unchecked
        {
            _state += 0x9E3779B97F4A7C15UL;
            var z = _state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            return z ^ (z >> 31);
        }
    }

    public override int Next() => (int)(NextU64() >> 33);

    public override int Next(int maxValue) => maxValue <= 0 ? 0 : (int)(NextU64() % (ulong)maxValue);

    public override int Next(int minValue, int maxValue) => maxValue <= minValue ? minValue : minValue + (int)(NextU64() % (ulong)(maxValue - minValue));

    public override double NextDouble() => (NextU64() >> 11) * (1.0 / (1UL << 53));

    public bool Chance(double p) => NextDouble() < p;
}
