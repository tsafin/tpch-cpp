/**
 * Performance Counters - Lightweight profiling instrumentation
 *
 * Usage:
 *   // Timed operations
 *   {
 *     ScopedTimer timer("arrow_append");
 *     // ... code to measure ...
 *   }
 *
 *   // Counter increments
 *   PerformanceCounters::instance().increment("rows_processed", batch_size);
 *
 *   // Print report
 *   PerformanceCounters::instance().print_report();
 */

#pragma once

#include <chrono>
#include <string>
#include <unordered_map>
#include <mutex>
#include <iostream>
#include <iomanip>
#include <vector>
#include <algorithm>

namespace tpch {

class PerformanceCounters {
public:
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = std::chrono::time_point<Clock>;
    using Duration = std::chrono::microseconds;

    static PerformanceCounters& instance() {
        static PerformanceCounters inst;
        return inst;
    }

    // Timer operations
    void start_timer(const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        timers_[name] = Clock::now();
    }

    void stop_timer(const std::string& name) {
        auto end = Clock::now();

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = timers_.find(name);
        if (it != timers_.end()) {
            auto duration = std::chrono::duration_cast<Duration>(end - it->second).count();
            durations_[name] += duration;
            counts_[name]++;
        }
    }

    // Counter operations
    void increment(const std::string& name, uint64_t value = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        counters_[name] += value;
    }

    void set(const std::string& name, uint64_t value) {
        std::lock_guard<std::mutex> lock(mutex_);
        counters_[name] = value;
    }

    // Report generation
    void print_report() const {
        std::lock_guard<std::mutex> lock(mutex_);

        std::cout << "\n";
        std::cout << "=" << std::string(78, '=') << "\n";
        std::cout << "Performance Counters Report\n";
        std::cout << "=" << std::string(78, '=') << "\n\n";

        // Print timers
        if (!durations_.empty()) {
            print_timers();
        }

        // Print counters
        if (!counters_.empty()) {
            print_counters();
        }

        std::cout << "=" << std::string(78, '=') << "\n";
    }

    void reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        timers_.clear();
        durations_.clear();
        counts_.clear();
        counters_.clear();
    }

    // Get individual values (for testing/validation)
    uint64_t get_duration_us(const std::string& name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = durations_.find(name);
        return (it != durations_.end()) ? it->second : 0;
    }

    uint64_t get_count(const std::string& name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = counts_.find(name);
        return (it != counts_.end()) ? it->second : 0;
    }

    uint64_t get_counter(const std::string& name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = counters_.find(name);
        return (it != counters_.end()) ? it->second : 0;
    }

private:
    PerformanceCounters() = default;
    ~PerformanceCounters() = default;

    PerformanceCounters(const PerformanceCounters&) = delete;
    PerformanceCounters& operator=(const PerformanceCounters&) = delete;

    void print_timers() const {
        std::cout << "## Timers\n\n";
        std::cout << std::left << std::setw(40) << "Name"
                  << std::right << std::setw(12) << "Total (ms)"
                  << std::setw(10) << "Calls"
                  << std::setw(12) << "Avg (us)"
                  << "\n";
        std::cout << std::string(78, '-') << "\n";

        // Sort by total duration (descending)
        std::vector<std::pair<std::string, uint64_t>> sorted_durations(
            durations_.begin(), durations_.end()
        );
        std::sort(sorted_durations.begin(), sorted_durations.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        for (const auto& [name, total_us] : sorted_durations) {
            uint64_t call_count = counts_.at(name);
            uint64_t avg_us = (call_count > 0) ? (total_us / call_count) : 0;
            double total_ms = total_us / 1000.0;

            std::cout << std::left << std::setw(40) << name
                      << std::right << std::setw(12) << std::fixed << std::setprecision(3) << total_ms
                      << std::setw(10) << call_count
                      << std::setw(12) << avg_us
                      << "\n";
        }

        std::cout << "\n";
    }

    void print_counters() const {
        std::cout << "## Counters\n\n";
        std::cout << std::left << std::setw(50) << "Name"
                  << std::right << std::setw(20) << "Value"
                  << "\n";
        std::cout << std::string(78, '-') << "\n";

        // Sort by name (alphabetically)
        std::vector<std::pair<std::string, uint64_t>> sorted_counters(
            counters_.begin(), counters_.end()
        );
        std::sort(sorted_counters.begin(), sorted_counters.end());

        for (const auto& [name, value] : sorted_counters) {
            std::cout << std::left << std::setw(50) << name
                      << std::right << std::setw(20) << value
                      << "\n";
        }

        std::cout << "\n";
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, TimePoint> timers_;
    std::unordered_map<std::string, uint64_t> durations_;  // microseconds
    std::unordered_map<std::string, uint64_t> counts_;     // number of calls
    std::unordered_map<std::string, uint64_t> counters_;   // general counters
};

/**
 * RAII timer - automatically starts on construction and stops on destruction
 *
 * Usage:
 *   void process_batch() {
 *     ScopedTimer timer("process_batch");
 *     // ... work ...
 *   }  // timer automatically stopped here
 */
class ScopedTimer {
public:
    explicit ScopedTimer(const std::string& name)
        : name_(name), enabled_(true) {
        PerformanceCounters::instance().start_timer(name_);
    }

    // Allow conditional timing
    ScopedTimer(const std::string& name, bool enabled)
        : name_(name), enabled_(enabled) {
        if (enabled_) {
            PerformanceCounters::instance().start_timer(name_);
        }
    }

    ~ScopedTimer() {
        if (enabled_) {
            PerformanceCounters::instance().stop_timer(name_);
        }
    }

    // Non-copyable, non-movable
    ScopedTimer(const ScopedTimer&) = delete;
    ScopedTimer& operator=(const ScopedTimer&) = delete;
    ScopedTimer(ScopedTimer&&) = delete;
    ScopedTimer& operator=(ScopedTimer&&) = delete;

private:
    std::string name_;
    bool enabled_;
};

/**
 * Macro for conditional performance instrumentation
 * Define TPCH_ENABLE_PERF_COUNTERS to enable, otherwise no-op
 */
#ifdef TPCH_ENABLE_PERF_COUNTERS
    #define TPCH_SCOPED_TIMER(name) ::tpch::ScopedTimer __perf_timer_##__LINE__(name)
    #define TPCH_INCREMENT_COUNTER(name, value) ::tpch::PerformanceCounters::instance().increment(name, value)
    #define TPCH_SET_COUNTER(name, value) ::tpch::PerformanceCounters::instance().set(name, value)
#else
    #define TPCH_SCOPED_TIMER(name) do {} while(0)
    #define TPCH_INCREMENT_COUNTER(name, value) do {} while(0)
    #define TPCH_SET_COUNTER(name, value) do {} while(0)
#endif

} // namespace tpch
