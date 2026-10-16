#pragma once

#include <base/defines.h>

#include <fmt/format.h>

#include <algorithm>
#include <vector>

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

    /// The following two special constants are used to speed up
    /// KeyCondition::checkInRange. When used as an initial_mask argument, they
    /// effectively prevent calculation of discarded BoolMask component as it is
    /// no longer changeable under BoolMask::combine (isComplete).
    static const BoolMask consider_only_can_be_true;
    static const BoolMask consider_only_can_be_false;
};

/// The answer to a yes-or-no question about the rows of a range, like "is the condition true on the row", on the range and
/// on the subranges a search can split it into. The level says whether no row, some rows or every row answers yes. `lo` is
/// the lowest level a subrange can have and `hi` the highest. The range itself is at `lo` if `lo == hi`, and at `Sometimes`
/// otherwise.
struct Side
{
    enum Level : uint8_t { Never, Sometimes, Always };

    Level lo = Never;
    Level hi = Always;

    static Side point(Level level) { return {level, level}; }

    /// The value that `hull` leaves unchanged.
    static Side none() { return {Always, Never}; }

    /// `refinable` tells whether a subrange can answer differently from the range.
    static Side of(bool some_yes, bool some_no, bool refinable)
    {
        if (!some_yes)
            return point(Never);
        if (!some_no)
            return point(Always);
        return refinable ? Side{Never, Always} : point(Sometimes);
    }

    Level level() const { return lo == hi ? lo : Sometimes; }

    /// Whether some subrange can be at `Never`, or at `Always`.
    bool mayBeNever() const { return lo == Never; }
    bool mayBeAlways() const { return hi == Always; }

    /// The question "are both yes" and the question "is either yes".
    Side operator&(Side other) const { return {std::min(lo, other.lo), std::min(hi, other.hi)}; }
    Side operator|(Side other) const { return {std::max(lo, other.lo), std::max(hi, other.hi)}; }

    /// The side of a range that is the union of the two ranges.
    static Side hull(Side left, Side right) { return {std::min(left.lo, right.lo), std::max(left.hi, right.hi)}; }

    /// Whether more `hull`s can no longer change what a caller reads. The caller reads `lo` down to `need.lo` and `hi` up to
    /// `need.hi`, so it reads nothing for `none()`.
    bool covers(Side need) const { return lo <= need.lo && hi >= need.hi; }

    bool operator==(const Side &) const = default;
};

/// What an index tells about a condition on a range of rows. A condition is true, false or NULL on a row, and `WHERE` keeps
/// the rows where it is true. Each outcome has a `Side`. `NOT` swaps `is_true` and `is_false` and keeps `is_null`, so a NULL
/// row stays neither true nor false.
///
/// A range can be dropped when `is_true` is `Never`, and every row of it matches when `is_true` is `Always`. Splitting the
/// range can drop a part of it only if `is_true.mayBeNever()`, and find a part where every row matches only if
/// `is_true.mayBeAlways()`. The default value says that every outcome is possible and splitting can tell more.
struct Truth
{
    Side is_true;
    Side is_false;
    Side is_null;

    /// A check that finds which outcomes the rows of the range can have, and that gives sharper answers on smaller ranges,
    /// like a key range check.
    static Truth check(bool can_be_true, bool can_be_false, bool can_be_null)
    {
        return {
            Side::of(can_be_true, can_be_false || can_be_null, true),
            Side::of(can_be_false, can_be_true || can_be_null, true),
            Side::of(can_be_null, can_be_true || can_be_false, true)};
    }

    /// The same for a check on rows that are never NULL.
    static Truth check(BoolMask mask) { return check(mask.can_be_true, mask.can_be_false, false); }

    /// Every outcome is possible, on the range and on every subrange, like for a condition on a column the index does
    /// not store.
    static Truth unknown() { return {Side::point(Side::Sometimes), Side::point(Side::Sometimes), Side::point(Side::Sometimes)}; }

    /// The value that `hull` leaves unchanged.
    static Truth none() { return {Side::none(), Side::none(), Side::none()}; }

    /// The same outcomes, on every subrange, like for an atom on a column that is constant over the part.
    Truth fixed() const { return {fixed(is_true), fixed(is_false), fixed(is_null)}; }

    /// For a check that finds a superset of the rows where the condition is true, like a prefix range for `LIKE 'ab%c'`.
    /// Any row may be false.
    Truth relaxed() const { return *this & Truth{Side::point(Side::Sometimes), Side::point(Side::Sometimes), Side::point(Side::Never)}; }

    /// For a range that may hold NULL rows that smaller ranges can tell apart, like the end of the range of a `Nullable`
    /// key column, where the key order puts NULLs.
    Truth withNull() const
    {
        return {
            {Side::Never, is_true.hi},
            {Side::Never, is_false.hi},
            {is_null.lo, Side::Always}};
    }

    /// For a check that cannot see some NULL rows, like a check of the key `assumeNotNull(x)`, which stores a NULL `x` as an
    /// ordinary value. Any row may be NULL instead of what the check says.
    Truth withHiddenNull() const
    {
        auto may_be_null = [](Side side) -> Side { return {std::min(side.lo, Side::Sometimes), std::min(side.hi, Side::Sometimes)}; };
        return {may_be_null(is_true), may_be_null(is_false), {std::max(is_null.lo, Side::Sometimes), std::max(is_null.hi, Side::Sometimes)}};
    }

    Truth operator&(const Truth & other) const { return {is_true & other.is_true, is_false | other.is_false, nullOfAnd(*this, other)}; }
    Truth operator|(const Truth & other) const { return !(!*this & !other); }
    Truth operator!() const { return {is_false, is_true, is_null}; }

    /// The value on a range that is the union of the two ranges.
    static Truth hull(const Truth & left, const Truth & right)
    {
        return {Side::hull(left.is_true, right.is_true), Side::hull(left.is_false, right.is_false), Side::hull(left.is_null, right.is_null)};
    }

    /// The value from two checks of the same condition on the same range, like checks on two indexes. A row can have an
    /// outcome only if both checks allow it. Both values must describe the whole range, not a `hull` that stopped early
    /// at `covers`.
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
            fromOutcomes(t, f || n, no_t, no_f && no_n),
            fromOutcomes(f, t || n, no_f, no_t && no_n),
            fromOutcomes(n, t || f, no_n, no_t && no_f)};
    }

    /// Whether more `hull`s can no longer change any side where a caller reads it, see `Side::covers`. A caller that reads
    /// every side whole, like `meet`, needs `Truth{}`.
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

    static Side fromOutcomes(bool can_be, bool others_can_be, bool may_lack, bool may_be_only)
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
        return fromOutcomes(can_be_null, others_can_be, may_lack, may_be_only);
    }
};

/// Evaluates a condition in reverse Polish notation with the operators of `Mask`. `atom_mask` gives the mask of each element
/// other than NOT, AND and OR.
template <typename Mask, typename RPNElement, typename AtomMask>
Mask evaluateRPN(const std::vector<RPNElement> & rpn, AtomMask && atom_mask)
{
    std::vector<Mask> stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_NOT)
        {
            chassert(!stack.empty());
            stack.back() = !stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND || element.function == RPNElement::FUNCTION_OR)
        {
            chassert(stack.size() >= 2);
            Mask arg = stack.back();
            stack.pop_back();
            stack.back() = element.function == RPNElement::FUNCTION_AND ? (stack.back() & arg) : (stack.back() | arg);
        }
        else
            stack.push_back(atom_mask(element));
    }
    chassert(stack.size() == 1);
    return stack.back();
}

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
