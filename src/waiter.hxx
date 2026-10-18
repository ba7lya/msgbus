// SPDX-License-Identifier: MIT

///
/// @file waiter.hxx
/// @author BA7LYA (1042140025@qq.com)
/// @brief How a queue waits for its deadline: the one place the library reads a clock, made replaceable.
/// @version 0.1
/// @date 2026-10-18
/// @copyright Copyright (c) 2026 BA7LYA
///

#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>

namespace ba7lya::msgbus {

///
/// @brief Waits on a condition variable until a predicate holds or a deadline passes.
/// @details The library reads a clock in exactly one place -- a queue waiting out `drain(timeout)` -- so
/// "inject a clock" means injecting this. The real implementation is a condition-variable wait; a test
/// substitutes one that decides when the deadline passes, which makes the timeout path deterministic
/// instead of a race against the scheduler.
///
class waiter {
public:
    waiter() = default;

    waiter(const waiter&) = delete;
    waiter& operator=(const waiter&) = delete;
    waiter(waiter&&) = delete;
    waiter& operator=(waiter&&) = delete;

    virtual ~waiter() = default;

    /// @brief Waits until `ready` is true or `timeout` elapses.
    /// @param signal the condition variable the queue's worker notifies
    /// @param lock the lock the caller already holds; released while waiting
    /// @param timeout how long to wait; the maximum value waits without a deadline
    /// @param ready the predicate the wait ends on
    /// @return `ready()`, i.e. whether it became true within `timeout`
    [[nodiscard]]
    virtual bool wait_for(
        std::condition_variable& signal, std::unique_lock<std::mutex>& lock, std::chrono::milliseconds timeout,
        const std::function<bool()>& ready
    ) = 0;
};

///
/// @brief The waiter a bus uses: the condition variable and nothing else.
/// @details `timeout == milliseconds::max()` waits without a deadline rather than calling `wait_for` with
/// it, because that would overflow adding the duration to the clock.
///
class real_waiter final : public waiter {
public:
    [[nodiscard]]
    bool wait_for(
        std::condition_variable& signal, std::unique_lock<std::mutex>& lock, std::chrono::milliseconds timeout,
        const std::function<bool()>& ready
    ) override {
        if (timeout == std::chrono::milliseconds::max()) {
            signal.wait(lock, ready);
            return true;
        }
        return signal.wait_for(lock, timeout, ready);
    }
};

} // namespace ba7lya::msgbus
