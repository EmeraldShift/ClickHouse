#pragma once

#include <fmt/format.h>

#include <algorithm>

/// Multiple Boolean values. That is, two Boolean values: can it be true, can it be false.
struct BoolMask
{
    bool can_be_true = false;
    bool can_be_false = false;

    BoolMask() = default;
    BoolMask(bool can_be_true_, bool can_be_false_) : can_be_true(can_be_true_), can_be_false(can_be_false_) { }

    BoolMask operator&(const BoolMask & m) const { return {can_be_true && m.can_be_true, can_be_false || m.can_be_false}; }
    BoolMask operator|(const BoolMask & m) const { return {can_be_true || m.can_be_true, can_be_false && m.can_be_false}; }
    BoolMask operator!() const { return {can_be_false, can_be_true}; }

    bool operator==(const BoolMask & other) const { return can_be_true == other.can_be_true && can_be_false == other.can_be_false; }

    /// Check if mask is no longer changeable under BoolMask::combine.
    /// We use this condition to early-exit KeyConditions::checkInRange methods.
    bool isComplete() const
    {
        return can_be_true && can_be_false;
    }

    /// Combine check result in different hyperrectangles.
    static BoolMask combine(const BoolMask & left, const BoolMask & right)
    {
        return {left.can_be_true || right.can_be_true, left.can_be_false || right.can_be_false};
    }

    /// Combine two checks of the same condition in the same hyperrectangle. A check may report a component
    /// as possible although it is not, but never the other way around, so only what both allow is possible.
    static BoolMask intersect(const BoolMask & left, const BoolMask & right)
    {
        return {left.can_be_true && right.can_be_true, left.can_be_false && right.can_be_false};
    }

    /// The following two special constants are used to speed up
    /// KeyCondition::checkInRange. When used as an initial_mask argument, they
    /// effectively prevent calculation of discarded BoolMask component as it is
    /// no longer changeable under BoolMask::combine (isComplete).
    static const BoolMask consider_only_can_be_true;
    static const BoolMask consider_only_can_be_false;
};

/// Index analysis checks a condition on a range of rows, such as a range of marks, a part, or a partition, using the index
/// of the range, for example the key values at its ends. On a row, the condition is true, false, or NULL. `BoolMask`
/// holds whether the condition can be true on some row of the range and whether it can be not true. `Truth` holds a `Side`
/// for each of the three values.
///
/// Each `Side` is about one value. Its level is `Never` if the condition has that value on no row of the range, and
/// `Always` if the condition has that value on every row. Otherwise the level is `Sometimes`. A check may also return
/// `Sometimes` for a range at `Never` or `Always` if the index doesn't store enough about the rows. So `Never` and `Always`
/// from a check are exact, and `Sometimes` may not be.
///
/// A `Side` also bounds the levels of the same check on subranges. `lo` is the lowest level on a subrange and `hi` the
/// highest. If `lo == hi`, the range and every subrange are at that level. Otherwise the range is at `Sometimes`. A check
/// usually returns one of four values.
///
///     {Never, Never}             The value is on no row.
///     {Always, Always}           The value is on every row.
///     {Never, Always}            The value can be on some rows, and a subrange can be at any level.
///     {Sometimes, Sometimes}     The value can be on some rows, and every subrange is at `Sometimes` too.
///
/// The last two are both `BoolMask(true, true)` and differ only on subranges. Take the key `(a, b)` and the condition
/// `b = 5`. On a range of marks where `a` changes, the key doesn't bound `b`. On a subrange where `a` is constant it does,
/// so `is_true` is `{Never, Always}`. For a condition on a column that the key doesn't store, every subrange is at
/// `Sometimes`, so `is_true` is `{Sometimes, Sometimes}`.
struct Side
{
    enum Level : uint8_t { Never, Sometimes, Always };

    Level lo = Never;
    Level hi = Always;

    static Side point(Level level) { return {level, level}; }

    /// The side that `hull` leaves unchanged.
    static Side none() { return {Always, Never}; }

    /// `can_be` tells whether the value can be on some row, and `can_be_other` whether another value can be on some row.
    /// `refinable` tells whether a subrange can be at a different level than the range.
    static Side of(bool can_be, bool can_be_other, bool refinable)
    {
        if (!can_be)
            return point(Never);
        if (!can_be_other)
            return point(Always);
        return refinable ? Side{Never, Always} : point(Sometimes);
    }

    Level level() const { return lo == hi ? lo : Sometimes; }

    /// Whether some subrange can be at `Never`, or at `Always`.
    bool mayBeNever() const { return lo == Never; }
    bool mayBeAlways() const { return hi == Always; }

    /// The sides of the conjunction and the disjunction on each row. `(a AND b).is_true` is `a.is_true & b.is_true`.
    Side operator&(Side other) const { return {std::min(lo, other.lo), std::min(hi, other.hi)}; }
    Side operator|(Side other) const { return {std::max(lo, other.lo), std::max(hi, other.hi)}; }

    /// The side of a range that is the union of the two ranges.
    static Side hull(Side left, Side right) { return {std::min(left.lo, right.lo), std::max(left.hi, right.hi)}; }

    /// Whether more `hull`s can no longer change the part of the side that a caller reads. The caller reads `lo` down to
    /// `need.lo` and `hi` up to `need.hi`, so it reads nothing for `none()`.
    bool covers(Side need) const { return lo <= need.lo && hi >= need.hi; }

    bool operator==(const Side &) const = default;
};

/// The values of a condition on the rows of a range, see `Side`. `WHERE` keeps the rows where the condition is true. A range
/// can be skipped if `is_true` is `Never`, and every row of the range matches if `is_true` is `Always`. By default,
/// every value of the condition can be on the range, and a subrange can be at any level.
///
/// `NOT`, `AND`, and `OR` combine truths the way they combine rows. `NOT` swaps `is_true` and `is_false`, and a NULL row stays
/// NULL. A row of `a AND b` is true only if both are true, and false if either is false. So every row of `a AND b` is true
/// only where every row of both is, and some row is false wherever some row of either is. `a OR b` is `NOT (NOT a AND NOT b)`.
///
/// The truth of an atom starts from `check`, the check of its range or set against the key. The methods below adjust the
/// truth of an atom that the key can't check exactly.
struct Truth
{
    Side is_true;
    Side is_false;
    Side is_null;

    /// The truth from a check that returns which values the condition can have on the range, and that can return fewer on
    /// a subrange. A key range check is such a check.
    static Truth check(bool can_be_true, bool can_be_false, bool can_be_null)
    {
        return {
            Side::of(can_be_true, can_be_false || can_be_null, true),
            Side::of(can_be_false, can_be_true || can_be_null, true),
            Side::of(can_be_null, can_be_true || can_be_false, true)};
    }

    /// The same for a check on rows that are never NULL.
    static Truth check(BoolMask mask) { return check(mask.can_be_true, mask.can_be_false, false); }

    /// Every value can be on the range and on every subrange. This is the truth of a condition on a column that the index
    /// doesn't store.
    static Truth unknown() { return {Side::point(Side::Sometimes), Side::point(Side::Sometimes), Side::point(Side::Sometimes)}; }

    /// The same levels, with every subrange at the level of the range.
    Truth fixed() const { return {fixed(is_true), fixed(is_false), fixed(is_null)}; }

    /// For a check that returns a superset of the rows where the atom is true. `s LIKE 'ab%c'` checks that `s` is in
    /// `['ab', 'ac')`. A row outside that range is false, but a row inside can be either. The atom is its check `AND` an
    /// unknown condition that is never NULL. Its `is_true` keeps `Never` but loses `Always`. The
    /// negation `s NOT LIKE 'ab%c'` keeps `Always` but loses `Never`, and no range can be skipped.
    Truth relaxed() const { return *this & Truth{Side::point(Side::Sometimes), Side::point(Side::Sometimes), Side::point(Side::Never)}; }

    /// The condition can also be NULL on some rows of the range. A subrange may hold only such rows.
    Truth withNull() const
    {
        return {
            {Side::Never, is_true.hi},
            {Side::Never, is_false.hi},
            {is_null.lo, Side::Always}};
    }

    /// For a key that stores a NULL as an ordinary value. The key `assumeNotNull(x)` stores a NULL `x` as an arbitrary value
    /// of its type. So for `x < 5`, a row that the key holds as `0` may be NULL. The condition may be NULL on any row where
    /// the check returns true or false.
    Truth withHiddenNull() const
    {
        auto may_be_null = [](Side side) -> Side { return {std::min(side.lo, Side::Sometimes), std::min(side.hi, Side::Sometimes)}; };
        return {may_be_null(is_true), may_be_null(is_false), {std::max(is_null.lo, Side::Sometimes), std::max(is_null.hi, Side::Sometimes)}};
    }

    Truth operator&(const Truth & other) const { return {is_true & other.is_true, is_false | other.is_false, nullOfAnd(*this, other)}; }
    Truth operator|(const Truth & other) const { return !(!*this & !other); }
    Truth operator!() const { return {is_false, is_true, is_null}; }

    /// The truth on a range that is the union of the two ranges.
    static Truth hull(const Truth & left, const Truth & right)
    {
        return {Side::hull(left.is_true, right.is_true), Side::hull(left.is_false, right.is_false), Side::hull(left.is_null, right.is_null)};
    }

    /// The truth from two checks of the same condition on the same range, such as checks on two indexes. A value can be on
    /// some row only if neither truth has it at `Never`. Both truths must describe the whole range, not a `hull` that stopped
    /// early at `covers`.
    static Truth meet(const Truth & left, const Truth & right)
    {
        auto can_be = [](Side l, Side r) { return l.level() != Side::Never && r.level() != Side::Never; };
        auto may_lack = [](Side l, Side r) { return l.mayBeNever() || r.mayBeNever(); };
        const bool t = can_be(left.is_true, right.is_true);
        const bool f = can_be(left.is_false, right.is_false);
        const bool n = can_be(left.is_null, right.is_null);
        const bool no_t = may_lack(left.is_true, right.is_true);
        const bool no_f = may_lack(left.is_false, right.is_false);
        const bool no_n = may_lack(left.is_null, right.is_null);
        return {
            fromValues(t, f || n, no_t, no_f && no_n),
            fromValues(f, t || n, no_f, no_t && no_n),
            fromValues(n, t || f, no_n, no_t && no_f)};
    }

    /// Whether more `hull`s can no longer change any side where a caller reads it, see `Side::covers`. A caller that reads
    /// every side whole, such as `meet`, needs `Truth{}`.
    bool covers(const Truth & need) const
    {
        return is_true.covers(need.is_true) && is_false.covers(need.is_false) && is_null.covers(need.is_null);
    }

    /// The `need` of a caller that reads only `is_true`.
    static Truth need(Side is_true) { return {is_true, Side::none(), Side::none()}; }

    /// The `need` of a caller that reads only `mask()`, with the bits that `initial_mask` sets already known.
    static Truth need(BoolMask initial_mask)
    {
        return need(Side{initial_mask.can_be_false ? Side::Always : Side::Sometimes, initial_mask.can_be_true ? Side::Never : Side::Sometimes});
    }

    /// Whether some row can match, and whether some row can fail to match.
    BoolMask mask() const { return {is_true.level() != Side::Never, is_true.level() != Side::Always}; }

private:
    static Side fixed(Side side) { return side.lo == side.hi ? side : Side::point(Side::Sometimes); }

    static Side fromValues(bool can_be, bool others_can_be, bool may_lack, bool may_be_only)
    {
        if (!can_be)
            return Side::point(Side::Never);
        if (!others_can_be)
            return Side::point(Side::Always);
        return {may_lack ? Side::Never : Side::Sometimes, may_be_only ? Side::Always : Side::Sometimes};
    }

    /// A row of `a AND b` is NULL where one of them is NULL and the other is not false.
    static Side nullOfAnd(const Truth & a, const Truth & b)
    {
        auto can_be = [](Side side) { return side.level() != Side::Never; };
        auto only = [](Side side) { return side.level() == Side::Always; };
        const bool can_be_null = (can_be(a.is_null) && !only(b.is_false)) || (can_be(b.is_null) && !only(a.is_false));
        const bool others_can_be = can_be(a.is_false) || can_be(b.is_false) || !(only(a.is_null) || only(b.is_null));
        const bool may_lack = (a.is_null.mayBeNever() && b.is_null.mayBeNever()) || a.is_false.mayBeAlways() || b.is_false.mayBeAlways();
        const bool may_be_only = (a.is_null.mayBeAlways() && b.is_false.mayBeNever()) || (a.is_false.mayBeNever() && b.is_null.mayBeAlways());
        return fromValues(can_be_null, others_can_be, may_lack, may_be_only);
    }
};

namespace fmt
{
template <>
struct formatter<BoolMask>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const BoolMask & mask, FormatContext & ctx)
    {
        return fmt::format_to(ctx.out(), "({}, {})", mask.can_be_true, mask.can_be_false);
    }
};
}
