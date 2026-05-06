import numpy as np
cimport numpy as cnp
cimport cython


cnp.import_array()

cdef packed struct WordType:
    char[2] type_
    char arg1
    char arg2
    int arg3


@cython.boundscheck(False)
@cython.wraparound(False)
def analyze_words(events, cnp.ndarray[WordType, ndim=1, mode='c'] words):
    cdef int n_events = 0, e_counter = 0, p_counter = 0
    cdef int n_words = words.size

    cdef int[50] cur_event_E
    cdef int[50] cur_event_P

    cdef int word_idx
    cdef WordType word

    event_types = [key for key in events]
    event_types.remove("other")

    for word_idx in range(n_words):
        word = words[word_idx]

        if word.type_[0] == b'R':
            n_events += 1
            # Start of a new bunch, so analyze the previous one

            electrons = []
            photons = []

            for i in range(e_counter):
                electrons.append(cur_event_E[i])

            for i in range(p_counter):
                photons.append(cur_event_P[i])

            event_type = "".join(("E" * e_counter, "P" * p_counter))

            if event_type in event_types:
                events[event_type].append(electrons + photons)

            elif e_counter > 0 or p_counter > 0:
                electrons = [str(val) for val in electrons]
                photons = [str(val) for val in photons]

                events['other'].append('{0}E{1}P|{2}|{3}'.format(
                    e_counter, p_counter,
                    ','.join(electrons), ','.join(photons)
                ))

            e_counter = 0
            p_counter = 0

        elif word.type_[0] == b'G':
            pass  # ignore

        elif word.type_[0] == b'F':
            if word.arg1 == 1:
                cur_event_E[e_counter] = word.arg3
                e_counter += 1
            elif word.arg1 == 2:
                cur_event_P[p_counter] = word.arg3
                p_counter += 1

    return n_events
