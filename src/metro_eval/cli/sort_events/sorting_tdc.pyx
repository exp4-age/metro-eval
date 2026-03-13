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
def analyze_words_native(
    events, cnp.ndarray[WordType, ndim=1, mode='c'] words):
    cdef int n_events = 0, e_counter = 0, p_counter = 0
    cdef int n_words = words.size

    cdef int[50] cur_event_E
    cdef int[50] cur_event_P

    cdef int word_idx    
    cdef WordType word

    # Optimize the most common key accesses
    events_E = events['E']
    events_EE = events['EE']
    events_P = events['P']

    for word_idx in range(n_words):
        word = words[word_idx]

        if word.type_[0] == b'R':
            n_events += 1
            # Start of a new bunch, so analyze the previous one

            # E
            if e_counter == 1 and p_counter == 0:
                events_E.append(cur_event_E[0])

            # P
            elif e_counter == 0 and p_counter == 1:
                events_P.append(cur_event_P[0])

            # EP
            elif e_counter == 1 and p_counter == 1:
                events['EP'].append([cur_event_E[0],
                                     cur_event_P[0]])

            # EE
            elif e_counter == 2 and p_counter == 0:
                events_EE.append([cur_event_E[0],
                                     cur_event_E[1]])

            # PP
            elif e_counter == 0 and p_counter == 2:
                events['PP'].append([cur_event_P[0],
                                     cur_event_P[1]])

            # EEP
            elif e_counter == 2 and p_counter == 1:
                events['EEP'].append([cur_event_E[0],
                                      cur_event_E[1],
                                      cur_event_P[0]])

            # EEE
            elif e_counter == 3 and p_counter == 0:
                events['EEE'].append([cur_event_E[0],
                                      cur_event_E[1],
                                      cur_event_E[2]])

            # EEEE
            elif e_counter == 4 and p_counter == 0:
                events['EEEE'].append([cur_event_E[0],
                                       cur_event_E[1],
                                       cur_event_E[2],
                                       cur_event_E[3]])

            elif e_counter > 0 or p_counter > 0:
                electrons = []
                photons = []

                for i in range(e_counter):
                    electrons.append(str(cur_event_E[i]))

                for i in range(p_counter):
                    photons.append(str(cur_event_P[i]))

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
