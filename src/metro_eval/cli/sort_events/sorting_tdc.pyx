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
def analyze_words(events, cnp.ndarray[WordType, ndim=1, mode="c"] words):
    cdef int n_events = 0, e_counter = 0, p_counter = 0
    cdef int n_words = words.size

    cdef int[50] cur_event_E
    cdef int[50] cur_event_P

    cdef int word_idx
    cdef WordType word

    # Fast access to common event types
    events_E = events["E"]
    events_P = events["P"]
    events_EP = events["EP"]
    events_EE = events["EE"]
    events_PP = events["PP"]
    events_EEP = events["EEP"]
    events_EEE = events["EEE"]
    events_EEEE = events["EEEE"]
    events_other = events["other"]

    for word_idx in range(n_words):
        word = words[word_idx]

        if word.type_[0] == b"R":
            n_events += 1
            # Start of a new bunch, so analyze the previous one

            if e_counter == 1 and p_counter == 0:
                events_E.append(cur_event_E[0])

            elif e_counter == 0 and p_counter == 1:
                events_P.append(cur_event_P[0])

            elif e_counter == 1 and p_counter == 1:
                events_EP.append([cur_event_E[0], cur_event_P[0]])

            elif e_counter == 2 and p_counter == 0:
                events_EE.append([cur_event_E[0], cur_event_E[1]])

            elif e_counter == 0 and p_counter == 2:
                events_PP.append([cur_event_P[0], cur_event_P[1]])

            elif e_counter == 2 and p_counter == 1:
                events_EEP.append([
                    cur_event_E[0], cur_event_E[1], cur_event_P[0]
                ])

            elif e_counter == 3 and p_counter == 0:
                events_EEE.append([
                    cur_event_E[0], cur_event_E[1], cur_event_E[2]
                ])

            elif e_counter == 4 and p_counter == 0:
                events_EEEE.append([
                    cur_event_E[0],
                    cur_event_E[1],
                    cur_event_E[2],
                    cur_event_E[3],
                ])

            else:
                event_type = "".join(("E" * e_counter, "P" * p_counter))

                if event_type in events:
                    ep = []

                    for i in range(e_counter):
                        ep.append(cur_event_E[i])

                    for i in range(p_counter):
                        ep.append(cur_event_P[i])

                    events[event_type].append(ep)

                elif e_counter > 0 or p_counter > 0:
                    electrons = []
                    photons = []

                    for i in range(e_counter):
                        electrons.append(str(cur_event_E[i]))

                    for i in range(p_counter):
                        photons.append(str(cur_event_P[i]))

                    events_other.append("{0}E{1}P|{2}|{3}".format(
                        e_counter, p_counter,
                        ",".join(electrons), ",".join(photons)
                    ))

            e_counter = 0
            p_counter = 0

        elif word.type_[0] == b"G":
            pass  # ignore

        elif word.type_[0] == b"F":
            if word.arg1 == 1:
                cur_event_E[e_counter] = word.arg3
                e_counter += 1

            elif word.arg1 == 2:
                cur_event_P[p_counter] = word.arg3
                p_counter += 1

    return n_events
