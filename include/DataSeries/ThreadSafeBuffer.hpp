#ifndef THREADSAFEBUFFER_HPP_
#define THREADSAFEBUFFER_HPP_

#include <deque>

#include <Lintel/PThread.hpp>

template <typename T>
class ThreadSafeBuffer {
public:
    ThreadSafeBuffer(uint32_t size = 3) : size(size), done(false) {
    }

    void add(T element) {
        PThreadScopedLock lock(mutex);
        while (buffer.size() == size) {
            not_full_cond.wait(mutex); // Wait for the buffer to be "not full"
        }
        buffer.push_back(element);
        not_empty_cond.signal(); // The buffer is certainly not empty.
    }

    bool remove(T *element) {
        PThreadScopedLock lock(mutex);
        while (buffer.size() == 0 && !done) {
            not_empty_cond.wait(mutex); // Wait for the buffer to be "not empty"
        }

        if (buffer.size() == 0) {
            return false;
        }
        *element = buffer.front();
        buffer.pop_front();
        not_full_cond.signal(); // The buffer is certainly not full.
        return true;
    }

    void signalDone() {
        PThreadScopedLock lock(mutex);
        done = true;
        not_empty_cond.signal();
    }

private:
    uint32_t size;
    bool done;
    std::deque<T> buffer;

    PThreadMutex mutex;
    PThreadCond not_full_cond;
    PThreadCond not_empty_cond;
};

#endif /* THREADSAFEBUFFER_HPP_ */
