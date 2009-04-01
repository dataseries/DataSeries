#ifndef __BOYER_MOORE_HORSPOOL_H
#define __BOYER_MOORE_HORSPOOL_H

#include <inttypes.h>
#include <limits.h>
#include <string.h>

class BoyerMooreHorspool {
public:
    BoyerMooreHorspool(const char *needle, int32_t needle_length);
    ~BoyerMooreHorspool();

    bool matches(const char *haystack, int32_t haystack_length);

private:
    int32_t bad_char_shift[CHAR_MAX + 1];
    char *needle;
    int32_t needle_length;
    int32_t last;
};

#endif /* __BOYER_MOORE_HORSPOOL_H */
