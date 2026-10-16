#pragma once

#include <fmt/format.h>

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

/// Bounds on the mask of a condition on a range, when splitting the range cannot tell more about some atoms. For example,
/// an index cannot evaluate an atom on a column it does not store. `upper` reads each such atom as the value that makes the
/// condition truest, and `lower` as the value that makes it falsest. The condition is never truer than `upper` and never
/// falser than `lower`.
///
/// `outer()` is the mask of the condition itself. The other two bits say whether more work on the range, like splitting it
/// or reading an index on it, can change a decision. A subrange can be excluded only if `upper.can_be_false`, and a subrange
/// can be exact only if `lower.can_be_true`.
struct BoolMaskBounds
{
    BoolMask upper;
    BoolMask lower;

    BoolMaskBounds() = default;
    BoolMaskBounds(BoolMask upper_, BoolMask lower_) : upper(upper_), lower(lower_) { }

    /// The rows the range check describes are exactly the rows that match.
    BoolMaskBounds(BoolMask mask) : upper(mask), lower(mask) { } /// NOLINT(google-explicit-constructor)

    /// The range check describes a superset of the rows that match, like a prefix range for `LIKE 'ab%c'`.
    static BoolMaskBounds relaxed(BoolMask mask) { return {mask, BoolMask(false, true)}; }

    /// `bounds` hold on the range, and splitting it cannot tell more. Unknown atoms are `fixed(BoolMask(true, true))`.
    static BoolMaskBounds fixed(BoolMaskBounds bounds)
    {
        bool can_be_true = bounds.upper.can_be_true;
        bool can_be_false = bounds.lower.can_be_false;
        return {BoolMask(can_be_true, !can_be_true), BoolMask(!can_be_false, can_be_false)};
    }

    static BoolMaskBounds unknown() { return fixed(BoolMask(true, true)); }

    BoolMaskBounds operator&(const BoolMaskBounds & other) const { return {upper & other.upper, lower & other.lower}; }
    BoolMaskBounds operator|(const BoolMaskBounds & other) const { return {upper | other.upper, lower | other.lower}; }
    BoolMaskBounds operator!() const { return {!lower, !upper}; }

    BoolMask outer() const { return {upper.can_be_true, lower.can_be_false}; }

    bool isComplete() const { return upper.isComplete() && lower.isComplete(); }

    /// Bounds from two checks of the same condition on the same range, like checks on two indexes. The range is dropped if
    /// either check drops it, and it is exact if either check says it is. `none()` changes nothing.
    static BoolMaskBounds intersect(const BoolMaskBounds & left, const BoolMaskBounds & right)
    {
        return {left.upper & right.upper, left.lower | right.lower};
    }

    /// Bounds that say nothing about the condition.
    static BoolMaskBounds none() { return {BoolMask(true, false), BoolMask(false, true)}; }

    /// An initial value for `combine` with every bit except those of `outer()` set. A search that starts from it stops once
    /// it knows `outer()`, for callers that use only `outer()`.
    static BoolMaskBounds outerOnly(BoolMask initial_mask)
    {
        return {BoolMask(initial_mask.can_be_true, true), BoolMask(true, initial_mask.can_be_false)};
    }

    /// Bounds on a range that is the union of the two ranges.
    static BoolMaskBounds combine(const BoolMaskBounds & left, const BoolMaskBounds & right)
    {
        return {BoolMask::combine(left.upper, right.upper), BoolMask::combine(left.lower, right.lower)};
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
